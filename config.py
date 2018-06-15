#!/usr/bin/env python
"""returns a config dict string from YAML config file
Usage: config.py file [param]
    file   a YAML config file
    param  optional param to get from file
"""
__author__ = "siznax 2010"

import sys, os, pprint, re
# svn co http://svn.pyyaml.org/pyyaml/trunk/ lib/pyamml
#sys.path[0:0] = (os.path.join(sys.path[0], 'lib'),)
if __name__ == '__main__':
    libpath = os.path.join(os.path.dirname(__file__), 'lib')
    if libpath not in sys.path: sys.path.append(libpath)
import yaml

MAX_ITEM_SIZE_GB = 10

def is_alnum(x): return x.isalnum()
def is_integer(x): return type(x) == int
def is_name(x): return re.match(r'[-_a-zA-Z0-9]+$', x)
def is_boolean(x): return isinstance(x, (bool, int, long))

class DrainConfig(object):
    def __init__(self, fname):
        self.fname = fname
        self.cfg = self.load(fname)

    def load(self, fname):
        """return config dict from YAML file"""
        try:
            with open(fname) as f:
                return yaml.load(f.read().decode('utf-8'))
        except OSError:
            print >>sys.stderr, "Failed to open %s" % fname
        except yaml.YAMLError, exc:
            print >>sys.stderr, "Error parsing config:", exc
            sys.exit(1)

    def __check(self, name, vf, msg):
        v = self.get_param(name)
        if not vf(v):
            raise ValueError, '%s %s: %s' % (name, msg, v)
 
    def check_integer(self, name):
        self.__check(name, is_integer, 'must be an integer')

    def validate(self):
        self.__check('crawljob', is_name, 'must be alpha-numeric')
        self.__check('job_dir', os.path.isdir, 'must be a directory')
        self.__check('xfer_dir', os.path.isdir, 'must be a directory')
        self.check_integer('sleep_time')
        # max_size < MAX_ITEM_SIZE_GB
        if self.cfg['max_size'] > MAX_ITEM_SIZE_GB:
            raise ValueError, "max_size=%d exceeds MAX_ITEM_SIZE_GB=%d" % (
                self.cfg['max_size'], MAX_ITEM_SIZE_GB)
        # WARC_naming = 1, 2 or a string
        self.__check('WARC_naming',
                     lambda x: x in (1, 2) or isinstance(x, basestring),
                     'must be 1 or 2')
        self.validate_naming()

        self.check_integer('block_delay')
        self.check_integer('retry_delay')

        # description descriptive with keywords
        if re.search("{describe_effort}", self.cfg['description']):
            raise ValueError, "desription must not contain "\
                + "'{describe_effort}'"
        #for key in ('CRAWLHOST','CRAWLJOB','START_DATE','END_DATE'):
        #    if not re.search(key, self.cfg['description']):
        #        raise ValueError, "description must contain placeholder " + key
        # operator not tbd
        self.__check('operator', lambda x: x != 'tbd@archive.org',
                     'must be proper operator identifier')
        # collections not TBD
        self.__check('collections', lambda x: x != 'TBD',
                     'must not contain "TBD"')
        # title_prefix not TBD
        self.__check('title_prefix', lambda x: x != 'TBD Crawldata',
                     'is invalid')
        # creator, sponsor, contributor, scanningcenter not null
        metadata = self['metadata']
        for key in ('creator','sponsor','contributor','scanningcenter'):
            #self.__check(key, lambda x: x is not None, 'is missing')
            # these metadata has been moved to "metadata" submap, which
            # automatically incorporates values from old parameters.
            if not metadata.get(key, None):
                raise ValueError, '%s is missing' % key

        # derive is int
        self.check_integer('derive')
        # compact_names is int
        self.check_integer('compact_names')

        return True

    def validate_naming(self):
        """validate naming WARC filename pattern and item name template.
        """
        wnpat = self.warc_name_pattern
        intmp = self.item_name_template

        # all the fields referenced in intmp must either be defined in
        # wnpat, or be derivable from them.
        fields_in_wnpat = re.findall(r'\{([A-Za-z][A-Za-z0-9]*)\}', wnpat)
        #print "fields_in_wnpat=%r" % fields_in_wnpat
        defined_symbols = fields_in_wnpat + ['last'+s for s in fields_in_wnpat]
        if 'host' in fields_in_wnpat:
            defined_symbols.append('shost')
            defined_symbols.append('lastshost')
        if 'timestamp' in fields_in_wnpat:
            defined_symbols.append('timestamp14')
            defined_symbols.append('lasttimestamp14')
        defined_symbols.append('suffix')
        undefined_symbols = []
        for ref in re.findall(r'\{([A-Za-z][A-Za-z0-9]*)\}', intmp):
            if ref not in defined_symbols:
                undefined_symbols.append(ref)
        #print "undefined_symbols=%r" % undefined_symbols
        if undefined_symbols:
            raise ValueError, 'item_naming has undefined field(s): %s' % \
                ', '.join(undefined_symbols)

    @property
    def warc_name_pattern(self):
        """format of WARC files in job (incoming) directory.
        """
        naming = self.get_param('WARC_naming')
        if naming == 1:
            return '{prefix}-{timestamp}-{serial}-{host}'
        elif naming == 2:
            return '{prefix}-{timestamp}-{serial}-{pid}~{host}~{port}'
        else:
            return str(naming)

    @property
    def warc_name_pattern_upload(self):
        """format of WARC files in item directory (which is the name
        WARC files are uploaded to the storage.)
        """
        if self.get_param('compact_names'):
            # WARCs in item dir have been renamed to this format
            return '{prefix}-{timestamp}-{serial}'
        else:
            return self.warc_name_pattern

    @property
    def item_name_template(self):
        naming = self.get_param('item_naming')
        if naming is None:
            if self.get_param('compact_names'):
                naming = '{prefix}-{timestamp14}{suffix}-{shost}'
            else:
                naming = '{prefix}-{timestamp}-{serial}-{lastserial}-{shost}'
        return naming

    @property
    def item_name_template_sh(self):
        tmpl = self.item_name_template
        return re.sub(r'\{[A-Za-z][_0-9A-Za-z]*\}',
                      lambda m: '$'+m.group(0), tmpl)

    def get_param(self, param):
        return self[param]

    def __getitem__(self, param):
        if param in ('xfer_dir', 'job_dir'):
            return os.path.abspath(os.path.join(
                    os.path.dirname(self.fname), self.cfg[param]))
        if param == 'warc_name_pattern':
            return self.warc_name_pattern
        if param == 'warc_name_pattern_upload':
            return self.warc_name_pattern_upload
        if param == 'item_name_template':
            return self.item_name_template
        if param == 'item_name_template_sh':
            return self.item_name_template_sh
        if param == 'drainme':
            return os.path.join(self.get_param('job_dir'), 'DRAINME')
        if param == 'config':
            return os.path.abspath(self.fname)
        if param == 'collections':
            # allow a few different formats
            v = self.cfg[param]
            if isinstance(v, list):
                # lower collection first (IA convention)
                return '/'.join(reversed(v))
            if isinstance(v, basestring) and v.find(';') >= 0:
                # IA-conventional one-string notation
                return '/'.join(reversed(v.split(';')))
            return v
        if param == 'crawlhost':
            # used in place of CRAWLHOST placeholder in description template.
            # let metadata.scanner override default hostname.
            md = self.cfg.get('metadata')
            if md and 'scanner' in md:
                return md['scanner']
            return os.uname()[1]
        if param == 'metadata':
            # for backward-compatibility, incorporate top-level
            # metadata config parameters into metadata dict.
            # templated/auto-generated metadata, such as title, description
            # and scandate, are not included, because they are handled
            # specially. TODO: they should be unified into a single framework.
            # mediatype and subject used to be hard-coded. now they are
            # configurable with sensible default values.
            # TODO: which metadata to include by default would depend on
            # item's mediatype.
            meta = dict(
                [(name, self.cfg[name])
                for name in (
                    'creator',
                    'sponsor',
                    'contributor',
                    'operator',
                    'scanningcenter'
                    )
                if name in self.cfg
                ], mediatype='web', subject='crawldata',
                scanner=os.uname()[1])
            # values in "metadata" param take precedence over top-level
            # metadata.
            v = self.cfg.get(param)
            if isinstance(v, dict):
                meta.update(v)
            # TODO: we should warn/abort if v is not a dict
            return meta
        if param == 's3cfg':
            s3cfg = os.environ.get('S3CFG')
            if s3cfg:
                try:
                    open(s3cfg, 'r')
                    return s3cfg
                except Exception as ex:
                    print >>sys.stderr, "Error: cannot read %s (by S3CFG)" % (
                        s3cfg)
                    return None

            def candidates():
                yield os.path.dirname(self.fname)
                yield os.environ["HOME"]
                # try homedir of effective user. if user runs draintasker
                # with sudo, HOME often has a value for the real user.
                try:
                    # pwd module is not available on all platforms
                    import pwd
                    pw = pwd.getpwuid(os.geteuid())
                    if pw:
                        yield pw.pw_dir
                except ImportError:
                    pass
            for path in candidates():
                try:
                    s3cfg = os.path.join(path, '.ias3cfg')
                    open(s3cfg, 'r')
                    return s3cfg
                except Exception:
                    pass
            return None
                
        return self.cfg.get(param)

    def iteritems(self):
        """returns iterator on parameters defined in YAML.
        (does not include synthetic config parameters)
        """
        return self.cfg.iteritems()

    def pprint(self, param=None, format=None, out=sys.stdout):
        if param is None:
            pprint.pprint(self.cfg, stream=out)
        else:
            v = self.get_param(param)
            if isinstance(v, dict):
                if format == 'header':
                    for key, value in v.iteritems():
                        if re.search(r'\s', key): continue
                        if value is None or value == '': continue
                        for s in format_header(key, value):
                            print >>out, s
                else:
                    for key, value in v.iteritems():
                        # space in key screws up, so drop it
                        if re.search(r'\s', key): continue
                        if value is None:
                            print >>out, "%s\t"
                        elif isinstance(value, list):
                            print >>out, "%s\t%s" % (key, ';'.join(value))
                        else:
                            print >>out, "%s\t%s" % (key, value)
            elif isinstance(v, list):
                for value in v:
                    print >>out, value
            elif isinstance(v, bool):
                print >>out, int(v)
            else:
                print >>out, v if v is not None else ''
        
def format_header(k, v):
    if isinstance(v, list):
        for i, v1 in enumerate(v):
            yield 'x-archive-meta%02d-%s:%s' % (i+1, k, v1)
    else:
        yield 'x-archive-meta-%s:%s' % (k, v if v is not None else '')
            
if __name__ == "__main__":
    from optparse import OptionParser
    opt = OptionParser()
    opt.add_option('-f', dest='format', default=None)
    opt.add_option('-m', action='store_const', dest='format', const='header',
                   help='equivalent of -f header')
    options, args = opt.parse_args()
    if len(args) < 1:
        print os.path.basename(__file__),  __doc__, __author__
        sys.exit(1)
    else:
        config = DrainConfig(args[0])
        if len(args) == 1:
            if config.validate():
                config.pprint()
        elif len(args) == 2:
            config.pprint(param=args[1], format=options.format)
