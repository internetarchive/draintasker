#!/usr/bin/python
"""returns a config dict string from YAML config file
  config.py file [param]
    file   a YAML file
    param  optional param to get from file
"""
__author__ = "siznax 2010"

import sys, os, pprint
# svn co http://svn.pyyaml.org/pyyaml/trunk/ lib/pyamml
sys.path.append(os.getcwd()+"/lib/")
import yaml

MAX_ITEM_SIZE_GB = 10

def get_param(fname,param):
    """ return value for param from YAML file """
    cfg = load_config(fname)
    return cfg[param]

def validate(cfg):
    """ ensure config dict has valid params """
    if not cfg['crawljob'].isalnum():
        raise ValueError, "must give crawljob"
    if not os.path.isdir(cfg["job_dir"]):
        raise ValueError, "job_dir is not a dir: " + cfg['job_dir']
    if not os.path.isdir(cfg["xfer_dir"]):
        raise ValueError, "xfer_dir is not a dir: " + cfg['xfer_dir']
    if cfg['max_size'] > MAX_ITEM_SIZE_GB:
        raise ValueError, "max_size=" + str(cfg['max_size'])\
            + " exceeds MAX_ITEM_SIZE_GB=" + str(MAX_ITEM_SIZE_GB)

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
 
else:
    """ on import """
    print "imported", __name__
