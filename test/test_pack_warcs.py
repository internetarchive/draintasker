
import sys
import os
import pytest
import random

from testutils import binpath, stepmodule, runstep
from draintasker.pack import PackFiles
from draintasker.config import DrainConfig

def test_standard_pack(testconf, testspace):
    """standard packing; no warc renaming, common WARC naming pattern,
    """
    # much smaller size for testing (1 MiB)
    testconf['max_size'] = '1M'
    ws = testspace(testconf)

    ITEM_SIZE = 1*1024*1024
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
        size = w.size()
        print("%s %d %d" % (w, size, total_size))
        if total_size + size > ITEM_SIZE: break
        warcs_packed.append(w)
        total_size += size

    assert len(warcs_packed) == 8

    rc = runstep('pack-warcs', str(ws.configpath), 1)
    # config = DrainConfig(str(ws.configpath))
    # rc = PackFiles(config, 'single', 0).execute()
    assert rc == 0

    assert not ws.jobdir.join('PACK.open').exists(), 'PACK.open was not removed'

    # last two files will not be packed.
    for w in warcs_packed:
        assert not w.exists(), "%s was not packed" % w

    EXPECTED_ITEM_NAME = 'WIDE-20101212000000-%05d-%05d-localhost' % (0, 7)

    check_item_dir(ws, EXPECTED_ITEM_NAME, warcs_packed, total_size)

    # check MANIFEST file - now make-manifests step is part of pack-warcs step.
    manifest = ws.itemfile(EXPECTED_ITEM_NAME, 'MANIFEST')
    manifest_open = ws.itemfile(EXPECTED_ITEM_NAME, 'MANIFEST.open')

    assert not manifest_open.exists()
    assert manifest.exists()

    manifest_lines = manifest.readlines()
    assert len(manifest_lines) == 8

def test_custom_naming(testconf, testspace):

    testconf.update(
        max_size='1M', WARC_naming='{prefix}-part-{serial}',
        item_naming='{prefix}-{serial}-{lastserial}'
    )

    ws = testspace(testconf)

    ITEM_SIZE = 1*1024*1024
    wnames = ('XTUX-part-%05d' % (n,) for n in xrange(10))
    warcs1 = ws.create_warcs(wnames, size=ITEM_SIZE/9+1)
    # WARC with unmatching name - should not be packed
    warcs2 = ws.create_warcs(['XTUX-par--00000'], size=ITEM_SIZE/9+1)

    warcs_packed = []
    total_size = 0
    for w in warcs1:
        size = w.size()
        if total_size + size > ITEM_SIZE: break
        warcs_packed.append(w)
        total_size += size

    rc = runstep('pack-warcs', str(ws.configpath), 1)
    assert rc == 0, "pack-warcs exit code: expected %d, got %d" % (0, rc)
    assert not ws.jobdir.join('PACK.open').exists(), "PACK.open was not removed"

    for w in warcs_packed:
        assert not w.exists(), "%s was not packed" % w
    # unmatching WARC should remain in jobdir
    for w in warcs2:
        assert w.exists(), "%s is missing" % w

    EXPECTED_ITEM_NAME = 'XTUX-%05d-%05d' % (0, 7)

    check_item_dir(ws, EXPECTED_ITEM_NAME, warcs_packed, total_size)

def check_item_dir(ws, item_name, warcs_packed, total_size):
    itemdir = ws.xferdir.join(item_name)

    assert itemdir.isdir(), "%s was not created" % itemdir

    for w in warcs_packed:
        path = itemdir.join(w.basename)
        assert path.isfile(), "%s does not exist" % path

    packed = itemdir.join('PACKED')
    packed_open = itemdir.join('PACKED.open')
    assert not packed_open.isfile()
    assert packed.isfile()

    lines = packed.readlines()
    assert len(lines) == 1

    packed_fields = lines[0].rstrip().split(' ')
    assert packed_fields == [item_name, str(len(warcs_packed)), str(total_size)]
