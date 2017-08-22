import pytest
import sys
import os
from subprocess import check_output
from io import BytesIO

config_py = os.path.join(os.path.dirname(__file__), '../lib/drain/config.py')

TESTCONF_1 = """
crawljob: wide
job_dir: /tmp
xfer_dir: /tmp
sleep_time: 300
max_size: 10
WARC_naming: 2
block_delay: 120
max_block_count: 120
retry_delay: 2400
description: CRAWLHOST:CRAWLJOB from START_DATE to END_DATE.
operator: crawl@archive.org
collections: webwidecrawl/widecrawl/wide00004
title_prefix: Webwide Crawldata
creator: Internet Archive
sponsor: Internet Archive
contributor: Internet Archive
scanningcenter: sanfrancisco
derive: 1
compact_names: 0
metadata:
  sponsor: Other Sponsor
  arbitrary: Metadata
  multivalue:
    - value1
    - value2
    - value3
"""

@pytest.fixture
def testconf(tmpdir):
    def _conf(s):
        conf = tmpdir / 'dtmon.yml'
        conf.write(s)
        return str(conf)
    return _conf

def test_help():
    """Just testing ``config.py -h`` runs without an error.
    (no basic errors in execution path)
    """
    check_output([config_py, '-h'])

def test_check(testconf):
    out = check_output([config_py, testconf(TESTCONF_1)])
    # out can be read back as a dict
    d = eval(out)
    assert d['title_prefix'] == 'Webwide Crawldata'
    assert d['xfer_dir'] == '/tmp'
    assert d['derive'] == 1
    assert d['retry_delay'] == 2400
    assert d['WARC_naming'] == 2
    assert d['metadata']['multivalue'] == ['value1', 'value2', 'value3']

def test_getconf(testconf):
    out = check_output([config_py, testconf(TESTCONF_1), 'xfer_dir'])
    assert out == '/tmp\n'

def test_getconf_object(testconf):
    """dict config parameter is printed in table format."""
    out = check_output([config_py, testconf(TESTCONF_1), 'metadata'])
    outf = BytesIO(out)
    d = {}
    for l in outf:
        k, v = l.rstrip().split('\t', 1)
        d[k] = v
    # note some top-level metadata are incorporated into metadata
    # and 'scanner' gets dafault value of local hostname
    thishost = os.uname()[1]
    assert d == {
        'creator': 'Internet Archive',
        'scanningcenter': 'sanfrancisco',
        'scanner': thishost,
        'contributor': 'Internet Archive',
        'sponsor': 'Other Sponsor',
        'mediatype': 'web',
        'subject': 'crawldata',
        'operator': 'crawl@archive.org',
        'multivalue': 'value1;value2;value3',
        'arbitrary': 'Metadata'
        }

def test_getconf_metadata_header(testconf):
    """printing out metadata in IAS3 metadata header format."""
    out = check_output([config_py, '-m', testconf(TESTCONF_1), 'metadata'])
    outf = BytesIO(out)
    d = {}
    for l in outf:
        k, v = l.rstrip().split(':', 1)
        d[k] = v
    thishost = os.uname()[1]
    assert d == {
        'x-archive-meta-creator': 'Internet Archive',
        'x-archive-meta-scanningcenter': 'sanfrancisco',
        'x-archive-meta-scanner': thishost,
        'x-archive-meta-contributor': 'Internet Archive',
        'x-archive-meta-sponsor': 'Other Sponsor',
        'x-archive-meta-mediatype': 'web',
        'x-archive-meta-subject': 'crawldata',
        'x-archive-meta-operator': 'crawl@archive.org',
        'x-archive-meta-arbitrary': 'Metadata',
        'x-archive-meta01-multivalue': 'value1',
        'x-archive-meta02-multivalue': 'value2',
        'x-archive-meta03-multivalue': 'value3'
        }
