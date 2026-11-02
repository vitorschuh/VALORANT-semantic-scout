from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from dags.vlr_matches import BRONZE_SCHEMA, MAPS_SCHEMA, ROUNDS_SCHEMA, _to_arrow, latest_per_match, parse_bronze
from include.lib import vlr
from include.lib.contracts import ContractViolation, contract_path, validate

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


def test_parse_bronze_scopes_to_summit_and_dedupes_latest():
    old = datetime(2026, 11, 1, tzinfo=timezone.utc)
    new = datetime(2026, 11, 2, tzinfo=timezone.utc)
    bronze = pa.table(
        {
            "match_id": ["753444", "753444"],
            "event_id": [2766, 2766],
            "source_url": ["u", "u"],
            "html": ["<html></html>", MATCH_HTML],
            "ingested_at": pa.array([new, old], pa.timestamp("us", tz="UTC")),
        },
        schema=BRONZE_SCHEMA,
    )
    assert latest_per_match(bronze).num_rows == 1
    maps, rounds = parse_bronze(bronze)  # latest copy is the empty page -> nothing
    assert maps == [] and rounds == []

    bronze = bronze.set_column(3, "html", pa.array([MATCH_HTML, "<html></html>"]))
    maps, rounds = parse_bronze(bronze)
    assert [m["map_name"] for m in maps] == ["Summit"]
    assert {r["map_name"] for r in rounds} == {"Summit"} and len(rounds) == 21


def test_silver_tables_satisfy_contracts_except_volume():
    """The fixture is a single match, so only the min_rows quality rule may fail: schema/value checks must pass."""
    bronze = pa.table(
        {
            "match_id": ["753444"],
            "event_id": [2766],
            "source_url": ["u"],
            "html": [MATCH_HTML],
            "ingested_at": pa.array([datetime.now(timezone.utc)], pa.timestamp("us", tz="UTC")),
        },
        schema=BRONZE_SCHEMA,
    )
    maps, rounds = parse_bronze(bronze)
    for name, records, schema in [("silver_vlr_maps", maps, MAPS_SCHEMA), ("silver_vlr_rounds", rounds, ROUNDS_SCHEMA)]:
        with pytest.raises(ContractViolation) as err:
            validate(_to_arrow(records, schema), contract_path(name))
        assert str(err.value).count(";") == 0 and "at least" in str(err.value), err.value
