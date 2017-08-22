from __future__ import unicode_literals

import pytest
import mock

import sys

from drain.drain import StateFile, Series, IllegalStateError

def test_statefile(tmpdir):
    series = mock.Mock(path=str(tmpdir))
    STATE = StateFile(series, "STATE")

    assert bool(STATE) is False
    # uses series.path
    assert STATE.path == str(tmpdir / "STATE")
    assert str(STATE) == str(tmpdir / "STATE")
    assert STATE.basename == "STATE"
    assert STATE.is_open() is False

    STATE.write("hoge\n")

    assert bool(STATE) is True
    assert (tmpdir / "STATE").isfile()
    assert STATE.is_open() is False

    content = STATE.read()
    assert content == "hoge\n"

    (tmpdir / "STATE").remove()

    with STATE.open() as f:
        assert STATE.is_open() is True
        assert (tmpdir / "STATE.open").isfile()
        f.write("muga")

    assert STATE.is_open() is False
    assert not (tmpdir / "STATE.open").exists()
    assert (tmpdir / "STATE").exists()
    assert STATE.read() == "muga"

    # re-open shall failed
    try:
        ff = STATE.open()
        pytest.fail("re-open did not raise an exception")
    except IllegalStateError as ex:
        pass
