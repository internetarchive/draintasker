#!/usr/bin/python
"""drain job in single mode
Usage: dtmon.py config
  config = YAML file like dtmon.yml
"""
__author__ = "siznax 2010"
__version__ = "2.3"

# for man page, "pydoc dtmon"

import sys, os
libdir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'lib'))
if libdir not in sys.path:
    sys.path.append(libdir)
import config, utils
import re
import subprocess
import time
from tempfile import NamedTemporaryFile
from datetime import datetime

class Storage(object):
    def __init__(self, **kwds):
        for k, v in kwds.iteritems():
            setattr(self, k, v)

def readfile(fn):
    try:
        f = open(fn)
        c = f.read().rstrip()
        f.close()
        return c
    except:
        return '?'
    
def getpids(excludes=[]):
    dir='/proc'
    for fn in os.listdir(dir):
        if fn != 'self' and os.path.isfile(os.path.join(dir, fn, 'cmdline')):
            try:
                pid = int(fn)
            except:
                continue
            if pid in excludes: continue
            yield int(fn)

def getdtprocesses(dtconf, excludes=[]):
    result = []
    for pid in getpids(excludes):
        cmdline = readfile(os.path.join('/proc', str(pid), 'cmdline'))\
            .split('\0')
        if cmdline[0]== '/bin/bash' or cmdline[0] == '/usr/bin/python':
            cmdline.pop(0)
        cmd = os.path.basename(cmdline[0])
        if cmd == 's3-drain-job.sh':
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=None))
        elif cmd == 's3-launch-transfers.sh':
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=None))
        elif cmd == 'dtmon.py':
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=None))
        elif cmd == 'pack-warcs.sh':
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=None))
        elif cmd == 's3-drain-job.sh':
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=None))
        elif cmd == 'make-manifests.sh':
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=None))
        elif cmd == 'curl':
            # drop "--header ..." arguments from cmdline - specifically
            # the one containing auth token.
            for i in range(len(cmdline) - 1, 0, -1):
                if cmdline[i].startswith('--header '):
                    cmdline.pop(i)
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=None))
    return result

def getstdout(pid):
    path = os.path.join('/proc', str(pid), 'fd', '1')
    try:
        return Storage(name=os.readlink(path))
    except:
        return None


class Series(object):
    def __init__(self, xfer, name):
        self.xfer = xfer
        self.name = name
        self.path = os.path.join(self.xfer, self.name)
        for f in ('RETRY','PACKED','MANIFEST','LAUNCH','TASK',
                  'SUCCESS','ERROR','TOMBSTONE'):
            setattr(self, f, os.path.join(self.path, f))
        try:
            st = os.stat(self.path)
            self.mtime = st.st_mtime
        except OSError:
            self.mtime = 0

    def has_file(self, file):
        return os.path.isfile(os.path.join(self.xfer, self.name, file))
    def read_file(self, file):
        path = os.path.join(self.xfer, self.name, file)
        try:
            f = open(path)
            c = f.read().rstrip()
            f.close()
            return c
        except:
            return '?'

    def check_file(self, file):
        if self.has_file(file): return file
        file_open = file+'.open'
        if self.has_file(file_open): return file_open
        return None

    @property
    def warcs(self):
        d = os.path.join(self.xfer, self.name)
        count = 0
        for fn in os.listdir(d):
            if re.match(r'.*\.w?arc(\.gz)?$', fn):
                count += 1
        return count

    @property
    def warcs_done(self):
        d = os.path.join(self.xfer, self.name)
        count = 0
        for fn in os.listdir(d):
            if re.match(r'.*\.w?arc(\.gz)?\.tombstone$', fn):
                count += 1
        return count

    @property
    def packed(self):
        return self.check_file('PACKED')
    @property
    def manifest(self):
        return self.check_file('MANIFEST')
    @property
    def launch(self):
        return self.check_file('LAUNCH')
    @property
    def task(self):
        return self.check_file('TASK')
    @property
    def tombstone(self):
        return self.check_file('TOMBSTONE')
    @property
    def success(self):
        if self.has_file('SUCCESS'):
            return 'SUCCESS'
        else:
            return None
    @property
    def error(self):
        if self.has_file('ERROR'):
            return 'ERROR'
        else:
            return None

    @property
    def retry(self):
        if self.has_file('RETRY'):
            return 'RETRY (%s)' % (self.read_file('RETRY'),)
        else:
            return None

    @property
    def status(self):
        if self.has_file('SUCCESS'):
            return 'completed'
        if not os.path.isfile(self.RETRY):
            if self.has_file('LAUNCH.open'): return 'running'
            if self.has_file('MANIFEST.open'): return 'running'
        return ''

    def retryasap(self):
        if os.path.isfile(self.SUCCESS):
            return dict(ok=0, error='completed series')
        if os.path.isfile(self.RETRY):
            # RETRY file may get deleted by s3-launch-transfer.sh
            # while running this code
            try:
                f = open(self.RETRY)
                retrytime = f.read().rstrip()
                f.close()
                if len(retrytime) > 0:
                    try:
                        os.rename(self.RETRY, '%s.%s' % (self.RETRY, retrytime))
                    except:
                        pass
            except:
                pass
                
            retrytime = readfile(self.RETRY)
            if retrytime != '?':
                try:
                    os.rename(self.RETRY, self.RETRY+'.'+str(retrytime))
                except:
                    pass
        try:
            f = open(self.RETRY, 'w')
            f.write("0\n")
            f.close()
            return dict(ok=1)
        except Exception as ex:
            return dict(ok=0, error=str(ex))

class Project(object):
    '''a class representing one drain project (for a drain.cfg)'''
    def __init__(self, id, config, manager):
        self.id = id
        self.config_fname = os.path.abspath(config)
        self.manager = manager
        self.processes = []
        self.config_mtime = None

        # check if user has .ias3cfg - not used in Project,
        # but it is useful to give user an error in the early stage.
        self.ias3cfg = os.environ["HOME"]+ "/.ias3cfg" 
        if not os.path.isfile(self.ias3cfg):
            sys.exit("Error: ias3cfg file not found: "+self.ias3cfg)

    def is_config_updated(self):
        if self.config_mtime is None: return True
        st = os.stat(self.config_fname)
        if st.st_mtime > self.config_mtime:
            self.config_mtime = st.st_mtime
            return True
        else:
            return False
        
    def loadconfig(self, force=False):
        # f = open(self.config_fname)
        # self.confobj = yaml.load(f.read().decode('utf-8'))
        # f.close()
        if force or self.is_config_updated():
            self.confobj = config.load_config(self.config_fname)
            self.validate_config()
            self.configure_instance()

    def validate_config(self):
        """ validate given config file """
        try:
            config.validate(self.confobj)
            print "config OK:", self.config_fname
            # config.pprint_config(self.config)
        except Exception as detail:
            print "Error:", detail
            sys.exit("Aborted: invalid config: "+self.config_fname)

    def configure_instance(self):
        """ set this instance's config params """
        self.DRAINME = os.path.join(self.confobj['job_dir'], 'DRAINME')
        self.sleep = self.confobj["sleep_time"]
        
    def configitems(self):
        return self.confobj.iteritems()

    @property
    def xfer_dir(self):
        return self.confobj['xfer_dir']

    def count_warcs(self, dir):
        cnt = 0
        try:
            for f in os.listdir(dir):
                if re.match(r'.*\.w?arc(\.gz)?$', f): cnt += 1
        except:
            pass
        return cnt

    def warcs_size(self, dir):
        size = 0
        for fn in os.listdir(dir):
            if re.match(r'.*\.w?arc(\.gz)?$', fn):
                try:
                    size += os.stat(os.path.join(dir, fn)).st_size
                except:
                    pass
        return size

    @property
    def source(self):
        srcdir = self.confobj['job_dir']
        if os.path.isdir(srcdir):
            s = os.statvfs(srcdir)
            r = Storage(
                exists=True,
                drainme=self.is_draining(),
                finishdrain=os.path.isfile(os.path.join(srcdir, 'FINISH_DRAIN')),
                packing=os.path.isfile(os.path.join(srcdir, 'PACKED.open')),
                free=s.f_bfree * s.f_bsize,
                total=s.f_blocks * s.f_bsize,
                warcs=self.count_warcs(srcdir),
                warcs_size=self.warcs_size(srcdir)
                )
            return r
        else:
            return Storage(exists=False, drainme=False, finishdrain=False,
                           free=0, avail=0)
        
    def uploads(self):
        '''return upload serieses'''
        serieses = []
        xferdir = self.confobj['xfer_dir']
        if os.path.isdir(xferdir):
            for e in os.listdir(xferdir):
                p = os.path.join(xferdir, e)
                if os.path.isdir(p):
                    s = Series(xferdir, e)
                    serieses.append(s)
        return sorted(serieses, lambda a, b: cmp(a.mtime, b.mtime),
                      reverse=True)

    def get_series(self, name):
        '''return specific series by its name'''
        # TODO should cache Series objects
        xferdir = self.confobj['xfer_dir']
        serdir = os.path.join(xferdir, name)
        if os.path.isdir(serdir):
             s = Series(xferdir, name)
             return s
        return None

    def start_packwarcs(self):
        args = (self.manager.DT_PACK_WARCS, self.confobj['job_dir'],
                self.confobj['xfer_dir'],
                str(self.confobj['max_size']),
                str(self.confobj['WARC_naming']),
                '1', 'single',
                str(self.confobj['compact_names']))
        outfile = NamedTemporaryFile(prefix='packwarcs', delete=False)
        p = subprocess.Popen(args, stdin=None, stdout=outfile, stderr=outfile,
                             cwd=self.manager.home)
        rec = Storage(p=p, o=outfile, st=datetime.now(), cmdline=args)
        self.processes.append(rec)
        return rec

    def start_launchtransfers(self, mode='single'):
        args = (self.manager.DT_LAUNCH_TRANSFERS, self.config_fname,
                '1', mode)
        outfile = NamedTemporaryFile(prefix='launchtransfers', delete=False)
        p = subprocess.Popen(args, stdin=None, stdout=outfile, stderr=outfile,
                             cwd=self.manager.home)
        rec = Storage(p=p, o=outfile, st=datetime.now(), cmdline=args)
        self.processes.append(rec)
        return rec

    def start_drain_job(self):
        args = (self.manager.DT_DRAIN_JOB, self.config_fname)
        # output goes to the same as this script
        try:
            subprocess.check_call(args, cwd=self.manager.home)
        except Exception as ex:
            print "Warning: failed to start %s: %s" % (' '.join(args), ex)

    def get_dtprocesses(self):
        excludes = [pinfo.p.pid for pinfo in self.processes]
        result = getdtprocesses(self.config_fname, excludes)
        return result

    def is_draining(self):
        return os.path.isfile(self.DRAINME)

    def drain(self, on):
        srcdir = self.confobj['job_dir']
        file = os.path.join(srcdir, 'DRAINME')
        if on:
            f = open(file, 'w')
            f.close()
        else:
            os.remove(file)

    def finishdrain(self, on):
        srcdir = self.confobj['job_dir']
        file = os.path.join(srcdir, 'FINISH_DRAIN')
        if on:
            f = open(file, 'w')
            f.close()
        else:
            os.remove(file)

# to be contained within iaupldr module
class UpLoader:

    def __init__(self, configs, sleep=None, home=None):
        """ initialize configuration """
        self.name = os.path.basename(__file__)
        self.sleep = sleep
        self.home = home
        if self.home is None:
            self.home = os.path.dirname(__file__)
        self.DT_LAUNCH_TRANSFERS = os.path.join(self.home,
                                                's3-launch-transfers.sh')
        self.DT_PACK_WARCS = os.path.join(self.home, 'pack-warcs.sh')
        self.DT_DRAIN_JOB = os.path.join(self.home, 's3-drain-job.sh')
        self.projects = [Project(i, config, self) for i, config
                         in enumerate(configs)]
        #self.init_config(fname)
        for pj in self.projects:
            pj.loadconfig()

    # def init_config(self,fname):
    #     """ initial config pass """
    #     self.config_fname = fname
    #     self.update_config()

    # def update_config(self):
    #     """ update config before each drain job """
    #     self.config = config.get_config(self.config_fname)
    #     self.validate_config()
    #     self.configure_instance()

    # def drain(self):
    #     """ drain job (or whatever) """
    #     import subprocess
    #     try:
    #         subprocess.check_call(["./s3-drain-job.sh",
    #                                self.config_fname])
    #     except Exception, e:
    #         print "Warning: process failed:", e

    def run(self):
        """ if DRAINME file exists, update config, drain job, sleep """
        utils.echo_start(self.name)
        while True:
            for pj in self.projects:
                pj.loadconfig()
                if pj.is_draining():
                    pj.start_drain_job()
                else:
                    print "DRAINME file not found: ", pj.DRAINME
                # old code that does not fit new multi-project support.
                # sleep time would be removed from dtmon.cfg and smarter
                # scheduling will be implemented. for now, sleeping for
                # sleep_time parameter in dtmon.cfg for the project
                # (overridden by command line option), until we start
                # using draintasker with multiple projects.
                sleep_time = self.sleep or pj.sleep
                print "sleeping %ds" % sleep_time
                sys.stdout.flush()
                time.sleep(sleep_time)

if __name__ == "__main__":
    from optparse import OptionParser
    opt = OptionParser(usage='%prog [OPTIONS] DTMON.CFG', version='2.2')
    opt.add_option('-p', '--http-port', action='store', dest='port', type='int',
                   help='port number for built-in HTTP server',
                   default='8321')
    opt.add_option('--no-http', action='store_false', dest='run_http_server',
                   help='disable built-in HTTP server',
                   default=True)
    opt.add_option('-i', '--interval', action='store', dest='interval',
                   type='int', help='time in seconds to sleep between draining',
                   default=None)
    options, args = opt.parse_args()
    if len(args) < 1:
        opt.print_help(sys.stderr)
        exit(1)

    configs = [os.path.abspath(a) for a in args]
    if os.path.isdir(configs[0]):
        exit('%s: is a directory' % args[0])

    dt = UpLoader(configs, sleep=options.interval)
    # utils.reflect(dt)
    if options.run_http_server:
        import admin
        admin.Server(dt, options.port).start()
        #admin.Server([dict(dtconf=p) for p in configs],
        #             options.port, dt).start()
    try:
        dt.run()
    except KeyboardInterrupt:
        pass
    finally:
        print "exiting..."
