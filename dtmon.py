#!/usr/bin/env python
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
import threading
import signal
from tempfile import NamedTemporaryFile
from datetime import datetime
try:
    import pwd
except:
    pwd = None

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
        try:
            mtime = os.stat(os.path.join('/proc', str(pid))).st_mtime
            start_time = datetime.utcfromtimestamp(mtime)
        except:
            start_time = None
        cmdline = readfile(os.path.join('/proc', str(pid), 'cmdline'))\
            .split('\0')
        if cmdline[0]== '/bin/bash' or cmdline[0] == '/usr/bin/python':
            cmdline.pop(0)
        cmd = os.path.basename(cmdline[0])
        if cmd == 's3-drain-job.sh':
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=start_time))
        elif cmd == 's3-launch-transfers.sh':
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=start_time))
        elif cmd == 'dtmon.py':
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=start_time))
        elif cmd == 'pack-warcs.sh':
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=start_time))
        elif cmd == 'make-manifests.sh':
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=start_time))
        elif cmd == 'curl':
            # drop "--header ..." arguments from cmdline - specifically
            # the one containing auth token.
            for i in range(len(cmdline) - 2, 0, -1):
                if cmdline[i] == '--header':
                    cmdline[i:i+2] = ('*',)
            result.append(Storage(p=Storage(pid=pid), cmdline=cmdline,
                                  o=getstdout(pid), st=start_time))
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

    def is_config_updated(self):
        if self.config_mtime is None: return True
        st = os.stat(self.config_fname)
        if st.st_mtime > self.config_mtime:
            self.config_mtime = st.st_mtime
            return True
        else:
            return False
        
    def loadconfig(self, force=False):
        if force or self.is_config_updated():
            self.configobj = config.DrainConfig(self.config_fname)
            try:
                self.configobj.validate()
                print "config OK: %s" % self.config_fname
            except Exception as ex:
                print >>sys.stderr, 'Aborting: invalid config %s: %s' % (
                    self.config_fname, ex)
                sys.exit(1)
            self.DRAINME = self.configobj['drainme']
            self.sleep = self.configobj['sleep_time']

    def configitems(self):
        return self.configobj.iteritems()

    @property
    def xfer_dir(self):
        return self.configobj['xfer_dir']

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
        srcdir = self.configobj['job_dir']
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
        xferdir = self.configobj['xfer_dir']
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
        xferdir = self.configobj['xfer_dir']
        serdir = os.path.join(xferdir, name)
        if os.path.isdir(serdir):
             s = Series(xferdir, name)
             return s
        return None

    def start_packwarcs(self):
        outfile = NamedTemporaryFile(prefix='packwarcs', delete=False)
        p = self.run_step('pack', outf=outfile)
        rec = Storage(p=p, o=outfile, st=datetime.now(), cmdline=args)
        self.processes.append(rec)
        return rec

    def start_launchtransfers(self, mode='single'):
        outfile = NamedTemporaryFile(prefix='launchtransfers', delete=False)
        p = self.run_step('ingest', outf=outfile)
        rec = Storage(p=p, o=outfile, st=datetime.now(), cmdline=args)
        self.processes.append(rec)
        return rec

    # no space in, no quotes around executable name.
    STEP_COMMAND = {
        'pack': 'pack-warcs.sh %(config)s 1 single',
        'manifest': 'make-manifests.sh %(xfer_dir)s single',
        'ingest': 's3-launch-transfers.sh %(config)s 1 single',
        'clean': 'delete-verified-warcs.sh %(xfer_dir)s 1'
        }

    def start_drain_job(self):
        for step in ('pack', 'manifest', 'ingest', 'clean'):
            p = self.run_step(step)
            returncode = p.wait()
            if returncode != 0:
                print >>sys.stderr, ('ERROR step %s failed with returncode %d' %
                                     (step, returncode))
                continue

    def run_step(self, step, outf=None):
        cmd = self.STEP_COMMAND[step]
        if not cmd.startswith('/'):
            cmd = self.manager.home + '/' + cmd
        prehook = self.configobj['before'+step]
        if prehook:
            cmd = prehook + ' && ' + cmd
        posthook = self.configobj['on'+step]
        if posthook:
            cmd += (' && ' + posthook)
        cmd = cmd % self.configobj
        print >>sys.stderr, "%s: %s" % (step, cmd)
        # TODO set cwd to where config file is located
        p = subprocess.Popen(cmd, shell=True, cwd=self.manager.home,
                             stdout=(outf or sys.stdout),
                             stderr=(outf or sys.stderr))
        return p

    def get_dtprocesses(self):
        excludes = [pinfo.p.pid for pinfo in self.processes]
        result = getdtprocesses(self.config_fname, excludes)
        return result

    def is_draining(self):
        return os.path.isfile(self.DRAINME)

    def drain(self, on):
        if on:
            f = open(self.DRAINME, 'w')
            f.close()
        else:
            os.remove(self.DRAINME)

    def finishdrain(self, on):
        srcdir = self.configobj['job_dir']
        file = os.path.join(srcdir, 'FINISH_DRAIN')
        if on:
            f = open(file, 'w')
            f.close()
        else:
            os.remove(file)

# to be contained within iaupldr module
class UpLoader:

    def __init__(self, configs, sleep=None, home=None, prefix=''):
        """ initialize configuration """
        self.name = os.path.basename(__file__)
        self.sleep = sleep
        self.home = home
        if self.home is None:
            self.home = os.path.dirname(__file__)
        self.home = os.path.abspath(self.home)

        self.prefix = prefix
        self.DT_LAUNCH_TRANSFERS = self.__cmd('s3-launch-transfers.sh')
        self.DT_PACK_WARCS = self.__cmd('pack-warcs.sh')
        self.DT_DRAIN_JOB = self.__cmd('s3-drain-job.sh')
        self.projects = [Project(i, config, self) for i, config
                         in enumerate(configs)]
        #self.init_config(fname)
        for pj in self.projects:
            pj.loadconfig()

        self.wakeupcond = threading.Condition()

    def __cmd(self, name):
        return os.path.join(self.home, (self.prefix or '') + name)

    def run(self, once=False):
        """ if DRAINME file exists, update config, drain job, sleep """
        utils.echo_start(self.name)
        while 1:
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
                if not once:
                    print "sleeping %ds" % sleep_time
                    sys.stdout.flush()
                    with self.wakeupcond:
                        self.wakeupcond.wait(timeout=sleep_time)
            if once: break

    def wakeup(self):
        with self.wakeupcond:
            self.wakeupcond.notify()

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
    opt.add_option('-L', action='store', dest='logfile', default=None,
                   help='after initial check, sends all output '
                   ' (both stdout and stderr) of draintasker and all '
                   ' its subprocesses to specified file.'
                   ' if the file exists, output will be appended to it')
    opt.add_option('--prefix', dest='prefix',
                   default=os.environ.get('DTMON_PREFIX', ''),
                   help='string to prepend to each sub-command '
                   '(intended for test/development aid)')
    opt.add_option('-1', '--once', action='store_true', dest='once',
                   help='run each draining steps just once and exit'
                   ' this option replaces running s3-drain-jobs.sh manually',
                   default=False)

    options, args = opt.parse_args()
    if len(args) < 1:
        opt.print_help(sys.stderr)
        exit(1)

    configs = [os.path.abspath(a) for a in args]
    if os.path.isdir(configs[0]):
        opt.error('%s is a directory' % args[0])

    dt = UpLoader(configs, sleep=options.interval, prefix=options.prefix)

    signal.signal(signal.SIGUSR1, lambda sig, st: dt.wakeup())

    if options.run_http_server:
        import admin
        try:
            admin.Server(dt, options.port).start()
        except Exception as ex:
            if hasattr(ex, 'errno') and ex.errno == os.errno.EADDRINUSE:
                print >>sys.stderr, (
                    "ERRROR:"
                    "port %d is used by other process. you can either specify"
                    " different port with -p (--http-port) option, or disable"
                    " HTTP status monitoring feature with --no-http option" %
                    options.port)
                sys.exit(1)
            else:
                raise
    if options.logfile:
        os.close(1)
        assert os.open(options.logfile, os.O_WRONLY|os.O_CREAT) == 1
        os.lseek(1, 0, 2)
        os.close(2)
        assert os.dup(1) == 2
    try:
        dt.run(options.once)
    except KeyboardInterrupt:
        pass
    finally:
        print "exiting..."
