#!/usr/bin/env python3

import sys
import os
import shutil
import random
import gzip
from tempfile import mkdtemp
from io import StringIO
from hashlib import md5

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../lib")))

import yaml

__all__ = ['bin', 'TESTCONF', 'TestSpace']

BINDIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
def bin(f):
    return os.path.join(BINDIR, f)

TESTCONF = dict(
    crawljob='wide', job_dir='warcs', xfer_dir='sink',
    sleep_time=300, block_delay=120, max_block_count=120,
    retry_delay=2400,
    max_size=1, # 1G for testing
    WARC_naming=2,
    description='CRAWLHOST:CRAWLJOB from START_DATE to END_DATE.',
    collections='test_collection',
    title_prefix='Webwide Crawldata',
    derive=1,
    compact_names=0,
    metadata=dict(
        sponsor='Internet Archive',
        operator='crawl@archive.org',
        creator='Internet Archive',
        contributor='Internet Archive',
        scanningcenter='sanfrancisco'
        )
    )

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
        self.dir = mkdtemp(prefix='dtmontest', dir=tmpdir)
        self.configpath = os.path.join(self.dir, 'dtmon.cfg')
        os.mkdir(os.path.join(self.dir, 'warcs'))
        os.mkdir(os.path.join(self.dir, 'sink'))
        with open(self.configpath, 'w') as w:
            yaml.dump(conf, w)
    def __del__(self):
        if os.path.isdir(self.dir):
            shutil.rmtree(self.dir)
    @property
    def jobdir(self):
        return os.path.join(self.dir, 'warcs')
    @property
    def xferdir(self):
        return os.path.join(self.dir, 'sink')

    def create_warcs(self, names, size=100*1000*1000):
        """creates test WARC files with names given in jobdir,
        by creating one gzip-compressed file with warcinfo record
        at the beginning, and copy it for the rest.
        """
        assert isinstance(size, int)
        print("creating test WARCs in %s" % self.jobdir, file=sys.stderr)
        chunksize = max(size // 1000, 1000)
        warcs = []
        reuse = None
        for name in names:
            name += '.warc.gz'
            path = os.path.join(self.jobdir, name)
            sys.stdout.write('%s ' % name)
            if reuse:
                # copy the first one. actually, link would be sufficient
                # for test.
                shutil.copy(reuse, path)
            else:
                z = gzip.open(path, 'wb', compresslevel=0)
                z.write(TEST_WARCINFO.encode())
                z.close()
                while os.path.getsize(path) < size:
                    sys.stdout.write("\rwriting: %s - current size: %s" %
                                    (name, os.path.getsize(path)))
                    z = gzip.open(path, 'a')
                    # pack-warcs doesn't care if WARC file content is in fact
                    # WARC records.
                    ss = chunksize
                    while ss > 0:
                        bytes = self.random_bytes(min(ss, 100000))
                        z.write(bytes)
                        ss -= len(bytes)
                    z.close()
                reuse = path
            warcs.append(path)
            sys.stdout.write('\r%s %s\n' % (name, os.path.getsize(path)))
        return warcs

    def random_bytes(self, length):
        assert isinstance(length, int)
        return bytearray(random.getrandbits(8) for i in range(length))

    def prepare_launch_transfers(self, iid, names):
        """create new item directory, fake WARC files, PACKED, and MANIFEST,
        emulating pack-warcs and make-manifest processes, just true enough for
        testing s3-launch-transfers.
        """
        print("creating test data in %s" % self.jobdir, file=sys.stderr)
        SIZE = 1024

        itemdir = os.path.join(self.xferdir, iid)
        os.makedirs(itemdir)

        warcs = []
        totalsize = 0
        for name in names:
            name += '.warc.gz'
            path = os.path.join(itemdir, name)
            sys.stdout.write('%s ' % name)
            # files are not even gzipped :-)
            hash = md5()
            with open(path, 'w') as w:
                bytes = self.random_bytes(SIZE)
                w.write(bytes)
                totalsize += SIZE
                hash.update(bytes.encode('utf-8'))
            sys.stdout.write('\n')
            warcs.append([name, hash.hexdigest()])
        
        packed = os.path.join(itemdir, 'PACKED')
        with open(packed, 'w') as w:
            w.write('%s %d %d\n' % (iid, len(names), totalsize))

        manifest = os.path.join(itemdir, 'MANIFEST')
        with open(manifest, 'w') as w:
            for warc in warcs:
                w.write('%s  %s\n' % (warc[1], warc[0]))

        return warcs
