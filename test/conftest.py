import pytest
from testutils import TestSpace

@pytest.fixture
def testconf():
    return dict(
        crawljob='wide',
        job_dir='warcs',
        xfer_dir='sink',
        sleep_time=300,
        block_delay=120,
        max_block_count=120,
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

@pytest.fixture
def testspace(tmpdir):
    def _testspace(conf):
        return TestSpace(conf, str(tmpdir))
    return _testspace
