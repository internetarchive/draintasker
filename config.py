#!/usr/bin/python
"""returns a config dict string from YAML config file
  config.py file [param] [validate]
    file      a YAML file
    param     optional param to get from file
    validate  1 = return 0 if valid (ignores param)
"""
__author__ = "siznax 2010"

import sys, os, pprint, re
# svn co http://svn.pyyaml.org/pyyaml/trunk/ lib/pyamml
sys.path.append(os.getcwd()+"/lib/")
import yaml

MAX_ITEM_SIZE_GB = 10

def get_param(fname,param):
    """ return value for param from YAML file """
    cfg = load_config(fname)
    try:
        return cfg[param]
    except KeyError:
        return

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
    return True

def pprint_config(cfg):
    """ pretty-print config dict """
    pprint.pprint(cfg)

def load_config(fname):
    """ return config dict from YAML file """
    try:
        cfg = yaml.load(open(fname))
        return cfg
    except yaml.YAMLError, exc:
        print "Error parsing config:", exc
        sys.exit()

def get_config(fname):
    """ verify YAML filepath, return config dict"""
    if os.path.exists(fname) == False:
        sys.exit("ERROR: config file not found: " + fname)
    elif fname[0] != "/":
        sys.exit("ERROR: must give fullpath to config: " + fname)
    else:
        return load_config(fname)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        """ usage """
        print os.path.basename(__file__),  __doc__, __author__
        sys.exit(1)
    else:
        """ process args """
        if len(sys.argv) == 2:
            pprint_config(load_config(sys.argv[1]))
        if len(sys.argv) == 3:
            print get_param(sys.argv[1],sys.argv[2])
        if len(sys.argv) == 4:
            validate(get_config(sys.argv[1]))
else:
    """ on import """
    # print "imported", __name__
