#!/usr/bin/python
"""returns a config dict string from YAML config file
Usage: config.py file [param]
    file   a YAML config file
    param  optional param to get from file
"""
from __future__ import unicode_literals, print_function
__author__ = "siznax 2010"

import sys
import os
import pprint
import re
try:
    import yaml
except ImportError as ex:
    # for running config.py as main script
    pass

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
            print("Failed to open %s" % (fname,), file=sys.stderr)
        except yaml.YAMLError as exc:
            print("Error parsing config: %s", (exc,), file=sys.stderr)
            sys.exit(1)

    def __check(self, name, vf, msg):
        v = self.get_param(name)
        if not vf(v):
            raise ValueError('%s %s: %s' % (name, msg, v))

    def check_integer(self, name):
        self.__check(name, is_integer, 'must be an integer')

    def validate(self):
        self.__check('crawljob', is_name, 'must be alpha-numeric')
        self.__check('job_dir', os.path.isdir, 'must be a directory')
        self.__check('xfer_dir', os.path.isdir, 'must be a directory')
        self.check_integer('sleep_time')
        # max_size < MAX_ITEM_SIZE_GB
        if self['max_size'] > MAX_ITEM_SIZE_GB*1024*1024*1024:
            raise ValueError("max_size=%d exceeds MAX_ITEM_SIZE_GB=%d" % (
                self.cfg['max_size'], MAX_ITEM_SIZE_GB))
        # WARC_naming = 1, 2 or a string
        self.__check('WARC_naming',
                     lambda x: x in (1, 2) or isinstance(x, basestring),
                     'must be 1 or 2')
        self.validate_naming()

        self.check_integer('block_delay')
        self.check_integer('retry_delay')

        # description descriptive with keywords
        if re.search("{describe_effort}", self.cfg['description']):
            raise ValueError("desription must not contain "\
                + "'{describe_effort}'")
        for key in ('CRAWLHOST','CRAWLJOB','START_DATE','END_DATE'):
            if not re.search(key, self.cfg['description']):
                raise ValueError("description must contain placeholder " + key)
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
                raise ValueError('%s is missing' % key)

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
        if param == 'max_size':
            value = self.cfg.get(param)
            if isinstance(value, int):
                # if no suffix, defaults to 'G'
                return value * 1024 * 1024 * 1024
            m = re.match(r'([0-9.]+)([a-zA-Z])$', format(value))
            if not m:
                raise ValeError(
                    'illegal value for max_size: {!r}'.format(value))
            value, unit = m.groups()
            try:
                factor = {
                    'k': 1000, 'K': 1024, 'm': 1000**2, 'M': 1024*1024,
                    'g': 1000**3, 'G': 1024**3, 't': 1000**4, 'T': 1024**4
                    }[unit]
                return int(float(value) * factor)
            except KeyError as ex:
                raise ValueError(
                    'Undefined suffix for max_size: {!r}'.format(value))
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
                            print(s, file=out)
                else:
                    for key, value in v.iteritems():
                        # space in key screws up, so drop it
                        if re.search(r'\s', key): continue
                        if value is None:
                            print("%s\t" % (key,), file=out)
                        elif isinstance(value, list):
                            print("%s\t%s" % (key, ';'.join(value)), file=out)
                        else:
                            print("%s\t%s" % (key, value), file=out)
            elif isinstance(v, list):
                for value in v:
                    print(value, file=out)
            elif isinstance(v, bool):
                print(int(v), file=out)
            else:
                print(v if v is not None else '', file=out)

# TODO: move to transfer
def format_header(k, v):
    if isinstance(v, list):
        for i, v1 in enumerate(v):
            yield 'x-archive-meta%02d-%s:%s' % (i+1, k, v1)
    else:
        yield 'x-archive-meta-%s:%s' % (k, v if v is not None else '')

if __name__ == "__main__":
    libpath = os.path.join(os.path.dirname(__file__), '../../lib')
    if libpath not in sys.path:
        sys.path.append(libpath)
    import yaml
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--format', default=None)
    parser.add_argument('-m', action='store_const', dest='format',
            const='header', help='synonym of -f header')
    parser.add_argument('config')
    parser.add_argument('param', nargs='?')

    args = parser.parse_args()

    config = DrainConfig(args.config)

    if args.param is None:
        if config.validate():
            config.pprint()
    else:
        config.pprint(param=args.param, format=args.format)
