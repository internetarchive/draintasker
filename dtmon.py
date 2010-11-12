#!/usr/bin/python
"""drain job in single mode
Usage: dtmon.py config [ copy ]
    config = YAML file or top-level dir
    copy   = copy files only - do not delete anything
"""
__author__ = "siznax 2010"

import sys, os

# eventually, contained within ia_upldr module
class upldr:

    def __init__(self,fname):
        self.name = os.path.basename(__file__)
        self.init_config(fname)

    def validate_config(self):
        try:
            config.validate(self.config)
            print "config OK:", self.config_fname
            # config.pprint_config(self.config)
        except Exception as detail:
            print "Error:", detail
            sys.exit("Aborted: invalid config: "+self.config_fname)

    def configure_instance(self):
        """ set this instance's config params """
        self.drainme = self.config['job_dir']+ "/DRAINME"
        self.sleep = self.config["sleep_time"]
        self.ias3cfg = os.environ["HOME"]+ "/.ias3cfg" 
        if os.path.isfile(self.ias3cfg) == False:
            sys.exit("Error: ias3cfg file not found: "+self.ias3cfg)
        
    def init_config(self,fname):
        self.config_fname = fname
        self.config = config.get_config(fname)
        self.validate_config()
        self.configure_instance()

    def update_config(self):
        self.config = config.get_config(self.config_fname)
        self.validate_config()
        self.configure_instance()

    def drain_job(self):
        import subprocess
        try:
            subprocess.check_call(["s3-drain-job.sh"])
        except Exception, e:
            print "process failed:", e
            sys.exit()

    def process(self):
        import time
        utils.echo_start(self.name)
        while True:
            self.update_config()
            if os.path.isfile(self.drainme):
                self.drain_job()
            else:
                print "DRAINME file not found: ", self.drainme
            print "sleep("+str(self.sleep)+")"
            time.sleep(self.sleep)
        utils.echo_finish(self.name)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        """ usage """
        print os.path.basename(__file__), __doc__, __author__
        sys.exit(1)
    else:
        """ process """
        import config, utils 
        if os.path.isdir(sys.argv[1]):
            print "config = dir TBD"
        else:
            dt = upldr(sys.argv[1])
            # utils.reflect(dt)
            dt.process()
            
else:
    """ on import """
    print "imported", __name__
