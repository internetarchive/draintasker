#!/usr/bin/env python3
import asyncio
from tornado import web
import os, sys

class Storage(object):
    def __init__(self, **kwds):
        for k, v in kwds.items():
            setattr(self, k, v)

class WebUI(web.RequestHandler):
    def initialize(self, manager):
        self.manager = manager
        self.projects = manager.projects

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
        self.render('main.html', uname=os.uname(), projects=self.projects)
    def get_startpackwarcs(self):
        try:
            pj = int(self.get_argument('pj'))
            pinfo = self.projects[pj].start_packwarcs()
            self.write(dict(ok=1, pid=pinfo.p.pid, out=pinfo.o.name))
        except Exception as ex:
            self.write(dict(ok=0, error=str(ex)))
    
    def get_starttransfers(self):
        try:
            pj = int(self.get_argument('pj'))
            pinfo = self.projects[pj].start_launchtransfers()
            self.write(dict(ok=1, pid=pinfo.p.pid, out=pinfo.o.name))
        except Exception as ex:
            self.write(dict(ok=0, error=str(ex)))

    def get_retryasap(self):
        try:
            pjid = int(self.get_argument('pj'))
            pj = self.projects[pjid]
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
        pj = self.projects[pjid]
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
        pj = self.projects[pjid]
        pj.drain((sw=='1'))
        self.write(dict(pj=pj.id, sw=sw, ok=1))
    def get_finishdrain(self):
        pjid = int(self.get_argument('pj'))
        sw = self.get_argument('sw', '1')
        pj = self.projects[pjid]
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
        xferdir = self.projects[int(pj)].xfer_dir
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

class Server:
    def __init__(self, manager, port, bindir=None):
        self.manager = manager
        self.port = port
        self.tmpldir = os.path.join(os.path.dirname(__file__), 't')
        appvars = dict(manager=self.manager)
        self.app = web.Application([ (r'/files/(.*)', Files, appvars),
                                     (r'/(.*)', WebUI, appvars)
                                     ],
                                   template_path=self.tmpldir)
    async def main(self):
        print(f"Server listening on port {self.port}", file=sys.stderr)
        # listen() must be called in the async context.
        self.app.listen(self.port)
        # this is essentially "run forever"
        await asyncio.Event().wait()

    async def test(self):
        while True:
            await asyncio.sleep(2)
            print('tick', file=sys.stderr)

if __name__ == "__main__":
    class DummyUploader:
        projects = []

    from optparse import OptionParser
    opt = OptionParser('%prog [OPTIONS] DTMON.CFG...')
    opt.add_option('-p', '--port', action='store', dest='port', type='int',
                   default=8081,
                   help='port to listen on for HTTP connection')
    options, args = opt.parse_args()
    configs = [ dict(dtconf=os.path.abspath(c)) for c in args ]
    asyncio.run(Server(DummyUploader(), options.port, None).main())
