import pyarrow as pa
import pyarrow.fs as pafs
import pytest

from include.lib.lake import Lake


@pytest.fixture
def lake(tmp_path):
    return Lake(pafs.LocalFileSystem(), str(tmp_path))


def test_raw_objects_round_trip_and_list(lake):
    lake.put("raw", "vlr_matches", "event_id=1/match_id=7/20260101T000000Z.html", "<html/>")
    lake.put("raw", "vlr_matches", "event_id=1/match_id=8/20260101T000000Z.html", b"<b/>")
    assert lake.keys("raw", "vlr_matches") == [
        "event_id=1/match_id=7/20260101T000000Z.html",
        "event_id=1/match_id=8/20260101T000000Z.html",
    ]
    assert lake.get("raw", "vlr_matches", "event_id=1/match_id=7/20260101T000000Z.html") == b"<html/>"


def test_keys_of_missing_dataset_is_empty(lake):
    assert lake.keys("raw", "nothing_here") == []


def test_write_dataset_replaces_previous_files(lake):
    lake.write_dataset("silver", "agents", pa.table({"name": ["Jett", "Sage"]}))
    lake.write_dataset("silver", "agents", pa.table({"name": ["Omen"]}))
    assert lake.read_dataset("silver", "agents")["name"].to_pylist() == ["Omen"]
    assert len(lake.keys("silver", "agents")) == 1


def test_unknown_layer_is_rejected(lake):
    with pytest.raises(ValueError, match="unknown layer"):
        lake.path("platinum", "agents")
