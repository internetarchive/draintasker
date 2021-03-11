#!/usr/bin/python
#
import sys
import os
import re
import unittest
from tempfile import NamedTemporaryFile, mkdtemp
import shutil
import random
import gzip
import subprocess

from testutils import *

class LaunchTransfersTest(unittest.TestCase):
    
    def testSubstringFilaneme(self):
        """test for #5 - s3-launch-transfers.sh sends bad Content-MD5 header
        when there are multiple files sharing substring in their filenames.
        """
        conf = dict(TESTCONF, WARC_naming='{prefix}-{serial}',
                    item_naming='{prefix}-{serial}-{lastserial}')
        ws = TestSpace(conf)
        
        ITEMID = 'WIDE-20130209104118'
        FN_A = 'government-00000'
        FN_B = 'us'+FN_A
        ws.prepare_launch_transfers(ITEMID, [ FN_A, FN_B ])
        
        p = subprocess.Popen([bin('s3-launch-transfers.sh'),
                              ws.configpath, '1', 'test'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        rc = p.wait()

        #print err
        # grep for output from curl_fake
        curl_lines = re.findall(r'(?m)^# curl.*', err)

        self.assertEquals(3, len(curl_lines))
        # first two are PUT for item creation and GET for testing item
        # last two are the file uploads. with issue #5, command line gets
        # truncated just after '--header Content-MD5:<hex...>'; there's no
        # --upload-file and URL.
        for l in curl_lines[1:]:
            assert re.search(r'--upload-file \S+\.warc\.gz ', l)
            assert re.search(r' http://s3\.us\.archive\.org/'+ITEMID+'/.*\.warc\.gz ', l)

            
        
        

if __name__ == '__main__':
    unittest.main()
