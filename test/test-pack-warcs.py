#!/usr/bin/python
#
import sys
import os
import unittest
import yaml
from tempfile import NamedTemporaryFile, mkdtemp
import shutil
import random
import gzip
import subprocess
from StringIO import StringIO

BINDIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
print BINDIR
def bin(f):
    return os.path.join(BINDIR, f)

TESTCONF = dict(
    crawljob='wide', job_dir='warcs', xfer_dir='sink',
    sleep_time=300, block_delay=120, max_block_count=120,
    retry_delay=2400,
    max_size=1, # 1G for testing
    WARC_naming=2,
    description='CRAWLHOST:CRAWLJOB from START_DATE to END_DATE.',
    collections='webwidecrawl/widecrawl/wide00004',
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
        print >>sys.stderr, "creating test WARCs in %s" % self.jobdir
        chunksize = max(size / 1000, 1000)
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
                z.write(TEST_WARCINFO)
                z.close()
                while os.path.getsize(path) < size:
                    sys.stdout.write('\r%s %s' %
                                     (name, os.path.getsize(path)))
                    z = gzip.open(path, 'a')
                    # pack-warcs doesn't care if WARC file content is in fact
                    # WARC records.
                    ss = chunksize
                    while ss > 0:
                        d = StringIO()
                        for i in xrange(min(ss, 100000)):
                            d.write(chr(int(random.random()*256)))
                        bytes = d.getvalue()
                        z.write(bytes)
                        ss -= len(bytes)
                    z.close()
                reuse = path
            warcs.append(path)
            sys.stdout.write('\r%s %s\n' % (name, os.path.getsize(path)))
        return warcs

class PackWarcsTest(unittest.TestCase):
    
    def testStandardPack(self):
        """standard packing; no warc renaming, common WARC naming pattern,
        """
        ws = TestSpace(TESTCONF)

        ITEM_SIZE = TESTCONF['max_size']*(1024**3)
        # note that current pack-warcs.sh always leaves the last WARC file
        # even when it fits in max_size=1GB. so here we make 10 WARCs of
        # 1/9 GB. as each WARC file is larger than 1/9 GB, pack-warcs.sh will
        # pack just 8 files, leaving 2.
        wnames = ('WIDE-2010121200%02d00-%05d-2145~localhost~9443' % (n, n)
                  for n in xrange(10))
        warcs = ws.create_warcs(wnames, size=ITEM_SIZE/9+1)
        assert len(warcs) == 10
        warcs_packed = []
        total_size = 0
        for w in warcs:
            size = os.path.getsize(w)
            print "%s %d %d" % (w, size, total_size)
            if total_size + size > ITEM_SIZE: break
            warcs_packed.append(w)
            total_size += size

        assert len(warcs_packed) == 8

        p = subprocess.Popen([bin('pack-warcs.sh'), ws.configpath, '1'])
        #(out, err) = p.communicate()
        rc = p.wait()

        self.assertEqual(0, rc, "pack-warcs.sh exit code, "
                         "expected %d, got %d" % (0, rc))

        assert not os.path.exists(os.path.join(ws.jobdir, 'PACK.open')),\
            "PACK.open was not removed"
        
        # last two files will not be packed.
        for w in warcs_packed:
            assert not os.path.exists(w), "%s was not packed" % w

        EXPECTED_ITEM_NAME = 'WIDE-20101212000000-%05d-%05d-localhost' % (0, 7)

        self.check_item_dir(ws, EXPECTED_ITEM_NAME, warcs_packed, total_size)

        # test run of make-manifests.sh and s3-launch-transfers.sh,
        # until we write separate test-scripts for them. not doing
        # programmatic check of their behavior. script output must be
        # examined.
        p = subprocess.Popen([bin('make-manifests.sh'), ws.xferdir])
        rc = p.wait()
        self.assertEqual(0, rc)

        p = subprocess.Popen([bin('s3-launch-transfers.sh'), ws.configpath, '1', 'test'])
        rc = p.wait()
        self.assertEqual(0, rc)

    def testCustomNaming(self):
        
        CONF = dict(TESTCONF)
        CONF['WARC_naming'] = '{prefix}-part-{serial}'
        CONF['item_naming'] = '{prefix}-{serial}-{lastserial}'

        ws = TestSpace(CONF)

        ITEM_SIZE = CONF['max_size']*(1024**3)
        wnames = ('XTUX-part-%05d' % (n,) for n in xrange(10))
        warcs1 = ws.create_warcs(wnames, size=ITEM_SIZE/9+1)
        # WARC with unmatching name - should not be packed
        warcs2 = ws.create_warcs(['XTUX-par--00000'])

        warcs_packed = []
        total_size = 0
        for w in warcs1:
            size = os.path.getsize(w)
            if total_size + size > ITEM_SIZE: break
            warcs_packed.append(w)
            total_size += size

        p = subprocess.Popen([bin('pack-warcs.sh'), ws.configpath, '1'])
        rc = p.wait()

        self.assertEqual(0, rc, "pack-warcs.sh exit code, "
                         "expected %d, got %d" % (0, rc))
        assert not os.path.exists(os.path.join(ws.jobdir, 'PACK.open')),\
            "PACK.open was not removed"

        for w in warcs_packed:
            assert not os.path.exists(w), "%s was not packed" % w
        # unmatching WARC should remain in jobdir
        for w in warcs2:
            assert os.path.exists(w), "%s is missing" % w

        EXPECTED_ITEM_NAME = 'XTUX-%05d-%05d' % (0, 7)

        self.check_item_dir(ws, EXPECTED_ITEM_NAME, warcs_packed, total_size)

    def check_item_dir(self, ws, item_name, warcs_packed, total_size):
        itemdir = os.path.join(ws.xferdir, item_name)

        assert os.path.isdir(itemdir), "%s was not created" % itemdir
        
        for w in warcs_packed:
            path = os.path.join(itemdir, os.path.basename(w))
            assert os.path.isfile(path), "%s does not exist" % path


        packed = os.path.join(itemdir, 'PACKED')
        assert not os.path.isfile(packed+'.open'), "PACKED.open still exists"
        assert os.path.isfile(packed), "PACKED was not created"
        with open(packed) as f:
            lines = [l for l in f]
        self.assertEqual(1, len(lines), "number of lines in PACKED, "
                         "expected %d, got %d" % (1, len(lines)))
        packed_fields = lines[0].rstrip().split(' ')

        self.assertEqual(3, len(packed_fields), "number of fields in PACKED")
        self.assertEqual(item_name, packed_fields[0],
                         "PACKED 1st field, item name, expected %s, got %s" %
                         (item_name, packed_fields[0]))
        self.assertEqual(str(len(warcs_packed)), packed_fields[1],
                         "PACKED 2nd field, number of WARCs, expected %s, got %s" %
                         (len(warcs_packed), packed_fields[1]))
        self.assertEqual(str(total_size), packed_fields[2],
                         "PACKED 2rd field, total size, expected %s, got %s" %
                         (total_size, packed_fields[2]))

if __name__ == '__main__':
    unittest.main()
