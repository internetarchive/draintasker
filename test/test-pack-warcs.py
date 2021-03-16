#!/usr/bin/env python3

import sys
import os
import unittest
from tempfile import NamedTemporaryFile, mkdtemp
import shutil
import random
import gzip
import subprocess

from testutils import *

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
                  for n in range(10))
        warcs = ws.create_warcs(wnames, size=ITEM_SIZE//9+1)
        assert len(warcs) == 10
        warcs_packed = []
        total_size = 0
        for w in warcs:
            size = os.path.getsize(w)
            print("%s %d %d" % (w, size, total_size))
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
        wnames = ('XTUX-part-%05d' % (n,) for n in range(10))
        warcs1 = ws.create_warcs(wnames, size=ITEM_SIZE//9+1)
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
