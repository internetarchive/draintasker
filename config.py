#!/usr/bin/python
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
 
    def validate(self):
        self.__check('crawljob', is_name, 'must be alpha-numeric')
        self.__check('job_dir', os.path.isdir, 'must be a directory')
        self.__check('xfer_dir', os.path.isdir, 'must be a directory')
        self.__check('sleep_time', is_integer, 'must be an integer')
        # max_size < MAX_ITEM_SIZE_GB
        if self.cfg['max_size'] > MAX_ITEM_SIZE_GB:
            raise ValueError, "max_size=%d exceeds MAX_ITEM_SIZE_GB=%d" % (
                self.cfg['max_size'], MAX_ITEM_SIZE_GB)
        # WARC_naming = {1, 2}
        self.__check('WARC_naming', lambda x: x in (1, 2),
                     'must be 1 or 2')
        self.__check('block_delay', is_integer, 'must be an integer')
        self.__check('retry_delay', is_integer, 'must be an integer')

        # description descriptive with keywords
        if re.search("{describe_effort}", self.cfg['description']):
            raise ValueError, "desription must not contain "\
                + "'{describe_effort}'"
        for key in ('CRAWLHOST','CRAWLJOB','START_DATE','END_DATE'):
            if not re.search(key, self.cfg['description']):
                raise ValueError, "description must contain " + key
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
        for key in ('creator','sponsor','contributor','scanningcenter'):
            self.__check(key, lambda x: x is not None, 'is missing')
        # derive is int
        self.__check('derive', is_integer, 'must be an integer')
        # compact_names is int
        self.__check('compact_names', is_integer, 'must be an integer')

        return True

    @property
    def warc_name_pattern(self):
        naming = self.get_param('WARC_naming')
        if naming == 1:
            return '{prefix}-{timestamp}-{serial}-{host}'
        elif naming == 2:
            return '{prefix}-{timestamp}-{serial}-{pid}~{host}~{port}'
        else:
            return str(naming)

    def get_param(self, param):
        if param in ('xfer_dir', 'job_dir'):
            return os.path.abspath(os.path.join(
                    os.path.dirname(self.fname), self.cfg[param]))
        if param == 'warc_name_pattern':
            return self.warc_name_pattern
        return self.cfg.get(param)

    def pprint(self):
        pprint.pprint(self.cfg)
        
def get_param(fname,param):
    """ return value for param from YAML file """
    cfg = load_config(fname)
    return cfg[param]

def validate(cfg):
    """ ensure config dict has valid params """
    # crawljob is alphanumeric
    if not cfg['crawljob'].isalnum():
        raise ValueError, "must give crawljob"
    # job_dir is a dir
    if not os.path.isdir(cfg["job_dir"]):
        raise ValueError, "job_dir is not a dir: "\
            + cfg['job_dir']
    # xfer_dir is a dir
    if not os.path.isdir(cfg["xfer_dir"]):
        raise ValueError, "xfer_dir is not a dir: "\
            + cfg['xfer_dir']
    # sleep_time is int
    if not type(cfg["sleep_time"]) == int:
        raise ValueError, "sleep_time must be integer type: "\
            + cfg['sleep_time']
    # max_size < MAX
    if cfg['max_size'] > MAX_ITEM_SIZE_GB:
        raise ValueError, "max_size=" + str(cfg['max_size'])\
            + " exceeds MAX_ITEM_SIZE_GB=" + str(MAX_ITEM_SIZE_GB)
    # WARC_naming < 2
    if cfg['WARC_naming'] > 2:
        raise ValueError, "invalid WARC_naming specified: "+cfg['WARC_naming']
    # block_delay is int
    if not type(cfg['block_delay']) == int:
        raise ValueError, "invalid block_delay: "\
            + cfg['block_delay']
    # retry_delay is int
    if not type(cfg['retry_delay']) == int:
        raise ValueError, "invalid retry_delay: "\
            + cfg['retry_delay']
    # description descriptive with keywords
    if re.search("{describe_effort}",cfg['description']) != None:
        raise ValueError, "desription must not contain "\
            + "'{describe_effort}'"
    for key in ('CRAWLHOST','CRAWLJOB','START_DATE','END_DATE'):
        if re.search(key,cfg['description']) == None:
            raise ValueError, "description must contain " + key
    # operator not tbd
    if cfg['operator'] == "tbd@archive.org":
        raise ValueError, "invalid operator email: " + cfg['operator']
    # collections not TBD
    if re.search("TBD",cfg['collections']) != None:
        raise ValueError, "collection must not contain 'TBD'"
    # title_prefix not TBD
    if cfg['title_prefix'] == "TBD Crawldata":
        raise ValueError, "invalid title_prefix: " + cfg['title_prefix']
    # creator, sponsor, contributor, scanningcenter not null
    for key in ('creator','sponsor','contributor','scanningcenter'):
        if not cfg.has_key(key):
            raise ValueError, "missing key: " + key
    # derive is int
    if not type(cfg['derive']) == int:
        raise ValueError, "derive must be int: " + cfg['derive']
    # compact_names is int
    if not type(cfg['compact_names']) == int:
        raise ValueError, "compact_names must be int: " + cfg['compact_names']

    return True

def pprint_config(cfg):
    """ pretty-print config dict """
    pprint.pprint(cfg)

def load_config(fname):
    """ return config dict from YAML file """
    try:
        f = open(fname)
        cfg = yaml.load(f.read().decode('utf-8'))
        f.close()
        return cfg
    except yaml.YAMLError, exc:
        print "Error parsing config:", exc
        sys.exit(1)

def get_config(fname):
    """ verify YAML filepath, return config dict"""
    if os.path.exists(fname) == False:
        sys.exit("ERROR: config file not found: " + fname)
    # elif fname[0] != "/":
    #     sys.exit("ERROR: must give fullpath to config: " + fname)
    else:
        return load_config(fname)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        """ usage """
        print os.path.basename(__file__),  __doc__, __author__
        sys.exit(1)
    else:
        """ process args """
        config = DrainConfig(sys.argv[1])
        if len(sys.argv) == 2:
            if config.validate():
                config.pprint()
        elif len(sys.argv) == 3:
            print config.get_param(sys.argv[2])
