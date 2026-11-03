from datetime import datetime, timezone
from pathlib import Path

import pyarrow.fs as pafs
import pytest

from dags.vlr_matches import (
    MAPS_SCHEMA,
    ROUNDS_SCHEMA,
    _to_arrow,
    latest_keys,
    load_raw,
    parse_bronze,
    raw_key,
)
from include.lib import vlr
from include.lib.contracts import ContractViolation, contract_path, validate
from include.lib.lake import Lake

FIXTURES = Path(__file__).parent / "fixtures"
MATCH_HTML = (FIXTURES / "vlr_match_753444.html").read_text()  # 100 Thieves vs T1: Summit 13-8, Ascent 13-9
EVENT_HTML = (FIXTURES / "vlr_event_matches_2766.html").read_text()


def test_event_listing_has_all_matches_once():
    ids = vlr.parse_event_match_ids(EVENT_HTML)
    assert len(ids) == 34 and len(set(ids)) == 34


def test_match_metadata_and_maps():
    parsed = vlr.parse_match(MATCH_HTML, "753444")
    assert parsed["match"]["stage"] == "Group Stage: Opening (A)"
    assert [(m["map_name"], m["team1_score"], m["team2_score"]) for m in parsed["maps"]] == [("Summit", 13, 8), ("Ascent", 13, 9)]
    summit = parsed["maps"][0]
    assert len(summit["team1_agents"]) == len(summit["team2_agents"]) == 5
    assert summit["duration_s"] == 3412


def test_rounds_are_consistent_with_map_score():
    parsed = vlr.parse_match(MATCH_HTML, "753444")
    for m in parsed["maps"]:
        rounds = [r for r in parsed["rounds"] if r["game_id"] == m["game_id"]]
        assert [r["round_no"] for r in rounds] == list(range(1, m["n_rounds"] + 1))
        assert (rounds[-1]["team1_score_after"], rounds[-1]["team2_score_after"]) == (m["team1_score"], m["team2_score"])
        # sides swap at halftime
        assert rounds[11]["team1_side"] != rounds[12]["team1_side"]
        assert all(r["team1_side"] != r["team2_side"] for r in rounds)


def test_latest_keys_picks_the_newest_copy_of_each_match():
    old = datetime(2026, 11, 1, tzinfo=timezone.utc)
    new = datetime(2026, 11, 2, tzinfo=timezone.utc)
    keys = [raw_key("1", old), raw_key("1", new), raw_key("2", old)]
    assert latest_keys(keys) == {"1": raw_key("1", new), "2": raw_key("2", old)}


def test_load_raw_reads_the_latest_copy_and_parses_the_timestamp(tmp_path):
    lake = Lake(pafs.LocalFileSystem(), str(tmp_path))
    old = datetime(2026, 11, 1, tzinfo=timezone.utc)
    new = datetime(2026, 11, 2, tzinfo=timezone.utc)
    lake.put("raw", "vlr_matches", raw_key("753444", old), "<html>stale</html>")
    lake.put("raw", "vlr_matches", raw_key("753444", new), MATCH_HTML)
    [row] = load_raw(lake)
    assert row["match_id"] == "753444" and row["ingested_at"] == new and row["html"] == MATCH_HTML


def test_parse_bronze_scopes_to_summit():
    ingested_at = datetime(2026, 11, 2, tzinfo=timezone.utc)
    maps, rounds = parse_bronze([{"match_id": "753444", "html": "<html></html>", "ingested_at": ingested_at}])
    assert maps == [] and rounds == []

    maps, rounds = parse_bronze([{"match_id": "753444", "html": MATCH_HTML, "ingested_at": ingested_at}])
    assert [m["map_name"] for m in maps] == ["Summit"]  # the match also has an Ascent map
    assert {r["map_name"] for r in rounds} == {"Summit"} and len(rounds) == 21


def test_silver_tables_satisfy_contracts_except_volume():
    """The fixture is a single match, so only the min_rows quality rule may fail: schema/value checks must pass."""
    maps, rounds = parse_bronze(
        [{"match_id": "753444", "html": MATCH_HTML, "ingested_at": datetime.now(timezone.utc)}]
    )
    for name, records, schema in [("silver_vlr_maps", maps, MAPS_SCHEMA), ("silver_vlr_rounds", rounds, ROUNDS_SCHEMA)]:
        with pytest.raises(ContractViolation) as err:
            validate(_to_arrow(records, schema), contract_path(name))
        assert str(err.value).count(";") == 0 and "at least" in str(err.value), err.value
