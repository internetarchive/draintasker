from __future__ import unicode_literals, print_function
import sys
import os
from tempfile import mkdtemp
import shutil
import random
import gzip
from StringIO import StringIO
from hashlib import md5
import subprocess
import py
import pytest

import yaml

__all__ = ['binpath', 'TESTCONF', 'TestSpace']

BINDIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
def binpath(f):
    return os.path.join(BINDIR, f)

STEPMODULE = {
    'pack-warcs': 'draintasker.pack',
    'launch-transfers': 'draintasker.transfer',
    'delete-verified-warcs': 'draintasker.clean'
}
def stepmodule(step):
    return STEPMODULE[step]

def runstep(step, *args):
    if False:
        args = map(str, args)
        p = subprocess.Popen([binpath(step)] + args)
        rc = p.wait()
    elif False:
        args = map(str, args)
        p = subprocess.Popen(['python', '-m', stepmodule(step)] + args)
        rc = p.wait()
    else:
        module = __import__(stepmodule(step), fromlist=['run'])
        rc = module.run(*args)
    return rc

# TESTCONF = dict(
#     crawljob='wide', job_dir='warcs', xfer_dir='sink',
#     sleep_time=300, block_delay=120, max_block_count=120,
#     retry_delay=2400,
#     max_size=1, # 1G for testing
#     WARC_naming=2,
#     description='CRAWLHOST:CRAWLJOB from START_DATE to END_DATE.',
#     collections='test_collection',
#     title_prefix='Webwide Crawldata',
#     derive=1,
#     compact_names=0,
#     metadata=dict(
#         sponsor='Internet Archive',
#         operator='crawl@archive.org',
#         creator='Internet Archive',
#         contributor='Internet Archive',
#         scanningcenter='sanfrancisco'
#         )
#     )

# test warcinfo record. note that Content-Length is not necessarily correct.
TEST_WARCINFO = """WARC/1.0
WARC-Type: warcinfo
WARC-Date: 2013-01-30T22:38:36Z
WARC-Filename: WIDE-20130130223836109-03048-3466~crawl450.us.archive.org~9443.warc.gz
WARC-Record-ID: <urn:uuid:0441315f-edde-42f3-8371-6b245d86ac14>
Content-Type: application/warc-fields
Content-Length: 478

software: Heritrix/3.1.2-SNAPSHOT-20120911.190842 http://crawler.archive.org
ip: 207.241.237.100
hostname: crawl450.us.archive.org
format: WARC File Format 1.0
conformsTo: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf
operator: crawl@archive.org
isPartOf: wide
description: Special crawl on Argentina government sites
robots: obey
http-header-user-agent: Mozilla/5.0 (compatible; archive.org_bot +http://www.archive.org/details/archive.org_bot)
"""

class TestSpace(object):
    def __init__(self, conf, tmpdir='/tmp'):
        self.dir = py.path.local(tmpdir).mkdtemp()
        self.configpath = self.dir.join('dtmon.cfg')
        self.jobdir = self.dir.mkdir('warcs')
        self.xferdir = self.dir.mkdir('sink')
        with self.configpath.open('w') as w:
            yaml.dump(conf, w)

    def __del__(self):
        self.dir.remove(ignore_errors=True)

    def itemdir(self, name):
        return self.xferdir.join(name)
    def itemfile(self, name, fn):
        return self.xferdir.join(name, fn)

    def create_warcs(self, names, size=100*1000*1000):
        """creates test WARC files with names given in jobdir,
        by creating one gzip-compressed file with warcinfo record
        at the beginning, and copy it for the rest.
        """
        print("creating test WARCs in %s" % (self.jobdir,), file=sys.stderr)
        chunksize = max(size / 1000, 1000)
        warcs = []
        reuse = None
        for name in names:
            name += '.warc.gz'
            path = self.jobdir.join(name)
            sys.stderr.write('%s ' % name)
            if reuse:
                # copy the first one. actually, link would be sufficient
                # for test.
                reuse.copy(path)
            else:
                with gzip.open(str(path), 'wb', compresslevel=0) as z:
                    z.write(TEST_WARCINFO)
                while path.size() < size:
                    sys.stderr.write('\r%s %s' % (name, path.size()))
                    with gzip.open(str(path), 'ab') as z:
                        # pack-warcs doesn't care if WARC file content is in fact
                        # WARC records.
                        ss = chunksize
                        while ss > 0:
                            bytes = self.random_bytes(min(ss, 100000))
                            z.write(bytes)
                            ss -= len(bytes)
                reuse = path
            warcs.append(path)
            sys.stderr.write('\r%s %s\n' % (name, path.size()))
        return warcs

    def random_bytes(self, length):
        d = StringIO()
        for i in xrange(length):
            d.write(chr(int(random.random()*256)))
        return d.getvalue()

    def prepare_launch_transfers(self, iid, names):
        """create new item directory, fake WARC files, PACKED, and MANIFEST,
        emulating pack-warcs and make-manifest processes, just true enough for
        testing s3-launch-transfers.
        """
        print("creating test data in %s" % (self.jobdir,), file=sys.stderr)
        SIZE = 1024

        itemdir = self.xferdir.mkdir(iid)

        warcs = []
        totalsize = 0
        for name in names:
            name += '.warc.gz'
            path = itemdir.join(name)
            sys.stdout.write('%s ' % name)
            # files are not even gzipped :-)
            hash = md5()
            with path.open('wb') as w:
                bytes = self.random_bytes(SIZE)
                w.write(bytes)
                totalsize += SIZE
                hash.update(bytes)
            sys.stdout.write('\n')
            warcs.append([name, hash.hexdigest()])

        packed = itemdir.join('PACKED')
        with packed.open('w') as w:
            w.write('%s %d %d\n' % (iid, len(names), totalsize))

        manifest = itemdir.join('MANIFEST')
        with manifest.open('w') as w:
            for warc in warcs:
                w.write('%s  %s\n' % (warc[1], warc[0]))

        return warcs
