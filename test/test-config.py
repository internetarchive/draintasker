#!/usr/bin/python
#
import sys
import os
import re
import pytest
from StringIO import StringIO

from drain.config import DrainConfig

# job_dir and xfer_dir have"/tmp" to keep validate() happy.
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
TESTCONF_ALL_NEWSTYLE_META = """crawljob: wide
job_dir: /tmp
xfer_dir: /tmp
sleep_time: 300
max_size: 10
WARC_naming: 2
block_delay: 120
max_block_count: 120
retry_delay: 2400
description: CRAWLHOST:CRAWLJOB from START_DATE to END_DATE.
collections: webwidecrawl/widecrawl/wide00004
title_prefix: Webwide Crawldata
derive: 1
compact_names: 0
metadata:
  sponsor: Sponsor
  creator: Internet Archive
  contributor: Internet Archive
  scanningcenter: sanfrancisco
  operator: crawl@archive.org
"""

TESTCONF_ALTCOL_1 = re.sub(
    r'(?m)^collections: .*$',
    'collections: wide00004;widecrawl;webwidecrawl',
    TESTCONF_1
    )
TESTCONF_ALTCOL_2 = re.sub(
    r'(?m)^collections: .*$',
    """collections:
 - wide00004
 - widecrawl
 - webwidecrawl""",
    TESTCONF_1
    )
TESTCONF_SUPPRESS = TESTCONF_1 + """  scanningcenter: null
  scanner: null
"""
TESTCONF_CUSTOM_NAMING = re.sub(
    r'(?m)^WARC_naming: .*$',
    'WARC_naming: "{prefix}-part-{serial}"\n'
    'item_naming: "CRAWL-{prefix}-{serial}-{lastserial}"',
    TESTCONF_1
    )
# item_naming has 'timestamp' field not found in WARC_naming.
TESTCONF_BAD_CUSTOM_NAMING = re.sub(
    r'(?m)^WARC_naming: .*$',
    'WARC_naming: "{prefix}-part-{serial}"\n'
    'item_naming: "CRAWL-{prefix}-{timestamp}-{serial}-{lastserial}"',
    TESTCONF_1
    )

@pytest.fixture
def testconf(tmpdir):
    def _conf(s):
        conf = tmpdir / 'dtmon.yml'
        conf.write(s)
        return str(conf)
    return _conf

def test1(testconf):
    cf = DrainConfig(testconf(TESTCONF_1))
    # this checks if job_dir exists
    cf.validate()

    assert cf['creator'] == 'Internet Archive'
    assert cf['sponsor'] == 'Internet Archive'

    assert cf['warc_name_pattern'] == \
        '{prefix}-{timestamp}-{serial}-{pid}~{host}~{port}'
    assert cf['warc_name_pattern_upload'] == \
        '{prefix}-{timestamp}-{serial}-{pid}~{host}~{port}'
    assert cf['item_name_template'] == \
        '{prefix}-{timestamp}-{serial}-{lastserial}-{shost}'

def testCollectionAlternativeFormat1(testconf):
    cf = DrainConfig(testconf(TESTCONF_ALTCOL_1))
    assert cf['collections'] == 'webwidecrawl/widecrawl/wide00004'

def testCollectionAlternativeFormat2(testconf):
    cf = DrainConfig(testconf(TESTCONF_ALTCOL_2))
    assert cf['collections'] == 'webwidecrawl/widecrawl/wide00004'

def testMetadata(testconf):
    cf = DrainConfig(testconf(TESTCONF_1))

    md = cf['metadata']
    assert isinstance(md, dict)

    # metadata automatically incorporate corresponding metadata
    # specified at the top level. If metadata member has an entry,
    # it gets higher precedence.
    assert md['creator'] == 'Internet Archive'
    #assert md['sponsor'] == 'Internet Archive' # NO
    assert md['sponsor'] == 'Other Sponsor', md['sponsor']
    assert md['contributor'] == 'Internet Archive'
    assert md['operator'] == 'crawl@archive.org'
    assert md['scanningcenter'] == 'sanfrancisco'

    assert md['scanner'] == os.uname()[1]

    assert md['mediatype'] == 'web'
    assert md['subject'] == 'crawldata'

    # arbitrary metadata can be specified this way
    assert md['arbitrary'] == 'Metadata'

def testNewStyleMetadata(testconf):
    cf = DrainConfig(testconf(TESTCONF_ALL_NEWSTYLE_META))
    cf.validate()

    md = cf['metadata']
    assert md['sponsor'] == 'Sponsor'
    assert md['creator'] == 'Internet Archive'
    assert md['contributor'] == 'Internet Archive'
    assert md['scanningcenter'] == 'sanfrancisco'
    assert md['operator'] == 'crawl@archive.org'

def testPrintHeader(testconf):
    cf = DrainConfig(testconf(TESTCONF_1))

    f = StringIO()
    cf.pprint('metadata', format='header', out=f)
    s = f.getvalue()
    f.close()

    headers = {}
    for l in StringIO(s):
        m = re.match(r'x-archive.meta(\d*)-([^:]+):(.*)', l.rstrip())
        assert m, 'unexpected format: %s' % l
        headers[m.group(2)+m.group(1)] = m.group(3)

    assert headers['creator'] == 'Internet Archive'
    assert headers['operator'] == 'crawl@archive.org'
    assert headers['mediatype'] == 'web'
    assert headers['subject'] == 'crawldata'
    assert headers['sponsor'] == 'Other Sponsor'

    assert headers['multivalue01'] == 'value1'
    assert headers['multivalue02'] == 'value2'
    assert headers['multivalue03'] == 'value3'

    # must not include 'collections'
    assert 'collections' not in headers
    assert 'collections01' not in headers

def testPrintHeaderSuppress(testconf):
    """metadata with null/empty value shall not be included in
    output. this is used for supressing default values."""
    cf = DrainConfig(testconf(TESTCONF_SUPPRESS))

    f = StringIO()
    cf.pprint('metadata', format='header', out=f)
    s = f.getvalue()
    f.close()

    headers = {}
    for l in StringIO(s):
        m = re.match(r'x-archive.meta(\d*)-([^:]+):(.*)', l.rstrip())
        assert m, 'unexpected format: %s' % l
        headers[m.group(2)+m.group(1)] = m.group(3)

    assert 'scanningcetner' not in headers, s
    assert 'scanner' not in headers, s

def testCustomNaming(testconf):
    cf = DrainConfig(testconf(TESTCONF_CUSTOM_NAMING))

    assert cf.warc_name_pattern == \
        '{prefix}-part-{serial}'
    assert cf.item_name_template == \
        'CRAWL-{prefix}-{serial}-{lastserial}'
    assert cf.item_name_template_sh == \
        'CRAWL-${prefix}-${serial}-${lastserial}'

def testBadCustomNaming(testconf):
    cf = DrainConfig(testconf(TESTCONF_BAD_CUSTOM_NAMING))

    assert '{prefix}-part-{serial}' == cf['warc_name_pattern']
    assert 'CRAWL-{prefix}-{timestamp}-{serial}-{lastserial}' == \
        cf['item_name_template']

    with pytest.raises(ValueError):
        cf.validate()
