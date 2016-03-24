#!/usr/bin/python
#
from tornado import ioloop, web, template
import os, re, sys
import yaml
import itertools
import subprocess
from tempfile import NamedTemporaryFile
from datetime import datetime
import time
import threading

class Storage(object):
    def __init__(self, **kwds):
        for k, v in kwds.iteritems():
            setattr(self, k, v)

class WebUI(web.RequestHandler):
    def initialize(self, manager):
        self.manager = manager

    def get(self, action):
        if action == 'favicon.ico':
            self.send_error(404)
            return
        h = getattr(self, 'get_'+(action or 'index'), self.get_index)
        if h:
            h()
        else:
            self.send_error(404)

    def get_index(self):
        self.set_header('content-type', 'text/html')
        self.render('main.html', uname=os.uname(), projects=self.manager.projects)
    def get_startpackwarcs(self):
        try:
            pj = int(self.get_argument('pj'))
            pinfo = self.manager.projects[pj].start_packwarcs()
            self.write(dict(ok=1, pid=pinfo.p.pid, out=pinfo.o.name))
        except Exception as ex:
            self.write(dict(ok=0, error=str(ex)))
    
    def get_starttransfers(self):
        try:
            pj = int(self.get_argument('pj'))
            pinfo = self.manager.projects[pj].start_launchtransfers()
            self.write(dict(ok=1, pid=pinfo.p.pid, out=pinfo.o.name))
        except Exception as ex:
            self.write(dict(ok=0, error=str(ex)))

    def get_retryasap(self):
        try:
            pjid = int(self.get_argument('pj'))
            pj = self.manager.projects[pjid]
            s = self.get_argument('s')
            series = pj.get_series(s)
            if series is None:
                self.write(dict(ok=0, s=s, error='no such series'))
                return
            r = series.retryasap()
            self.write(r)
        except Exception as ex:
            self.write(dict(ok=0, s=p.s, error=str(ex)))

    def get_processes(self):
        result = []
        pjid = int(self.get_argument('pj'))
        pj = self.manager.projects[pjid]
        for proc in pj.processes:
            result.append(dict(st=proc.st, pid=proc.p.pid, o=proc.o.name,
                               c=(proc.p.poll() or 'running'),
                               a=proc.cmdline))
        for proc in pj.get_dtprocesses():
            result.append(dict(st=proc.st, pid=proc.p.pid, o=None,
                               c='running',
                               a=proc.cmdline))
        self.write(result)

    def get_drain(self):
        pjid = int(self.get_argument('pj'))
        sw = self.get_argument('sw', '1')
        pj = self.manager.projects[pjid]
        pj.drain((sw=='1'))
        self.write(dict(pj=pj.id, sw=p.sw, ok=1))
    def get_finishdrain(self):
        pjid = int(self.get_argument('pj'))
        sw = self.get_argument('sw', '1')
        pj = self.manager.projects[pjid]
        pj.finishdrain((sw=='1'))
        self.write(dict(pj=pj.id, sw=sw, ok=1))
            
class Files(web.RequestHandler):
    def initialize(self, manager):
        self.manager = manager
    def get(self, pathinfo):
        try:
            pj, path = pathinfo.split('/', 1)
        except:
            self.send_error(404)
        xferdir = self.manager.projects[int(pj)].xfer_dir
        try:
            f = open(os.path.join(xferdir, path))
            while 1:
                c = f.read(4096)
                if not c: break
                self.write(c)
            f.close()
            self.set_header('Content-Type', 'text/plain')
        except:
            self.send_error(404)

class Server(threading.Thread):
    def __init__(self, manager, port, bindir=None):
        threading.Thread.__init__(self)
        self.daemon = True
        self.manager = manager
        tmpldir = os.path.join(os.path.dirname(__file__), 't')
        appvars = dict(manager=self.manager)
        self.app = web.Application([ (r'/files/(.*)', Files, appvars),
                                     (r'/(.*)', WebUI, appvars)
                                     ],
                                   template_path=tmpldir)
        print >>sys.stderr, "Server listening on port %d" % port
        self.app.listen(port)
    def run(self):
        ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    from optparse import OptionParser
    opt = OptionParser('%prog [OPTIONS] DTMON.CFG...')
    opt.add_option('-p', '--port', action='store', dest='port', type='int',
                   default=8081,
                   help='port to listen on for HTTP connection')
    options, args = opt.parse_args()
    configs = [ dict(dtconf=os.path.abspath(c)) for c in args ]
    Server(args, options.port, None).start()
