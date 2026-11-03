"""Phase 2: VALORANT Champions 2026 (Shanghai) from vlr.gg: raw HTML -> contract -> silver (maps, rounds).

raw/vlr_matches: one HTML object per match fetch (event_id=/match_id=/<utc timestamp>.html), complete (all maps);
scope filtering is silver's job. Completed matches are immutable, so each is fetched once.
silver/vlr_maps and silver/vlr_rounds: parsed from the latest raw copy of each match, Summit only (initial scope).
"""
from datetime import datetime, timezone

import pyarrow as pa
from airflow.sdk import asset

from include.lib import vlr
from include.lib.contracts import contract_path, validate
from include.lib.lake import Lake, get_lake

EVENT_ID = 2766  # VALORANT Champions 2026, Shanghai
SCOPE_MAP = "Summit"

MAPS_SCHEMA = pa.schema(
    [
        ("game_id", pa.string()),
        ("match_id", pa.string()),
        ("stage", pa.string()),
        ("played_at_raw", pa.string()),
        ("map_order", pa.int64()),
        ("map_name", pa.string()),
        ("team1_name", pa.string()),
        ("team2_name", pa.string()),
        ("team1_score", pa.int64()),
        ("team2_score", pa.int64()),
        ("duration_s", pa.int64()),
        ("team1_agents", pa.list_(pa.string())),
        ("team2_agents", pa.list_(pa.string())),
        ("n_rounds", pa.int64()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)

ROUNDS_SCHEMA = pa.schema(
    [
        ("round_id", pa.string()),
        ("match_id", pa.string()),
        ("game_id", pa.string()),
        ("map_name", pa.string()),
        ("round_no", pa.int64()),
        ("half", pa.string()),
        ("team1_name", pa.string()),
        ("team2_name", pa.string()),
        ("team1_side", pa.string()),
        ("team2_side", pa.string()),
        ("winner_name", pa.string()),
        ("winning_side", pa.string()),
        ("win_type", pa.string()),
        ("team1_score_after", pa.int64()),
        ("team2_score_after", pa.int64()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


DATASET = "vlr_matches"
TIMESTAMP_FORMAT = "%Y%m%dT%H%M%SZ"


def raw_key(match_id: str, ingested_at: datetime) -> str:
    return f"event_id={EVENT_ID}/match_id={match_id}/{ingested_at.strftime(TIMESTAMP_FORMAT)}.html"


def parse_raw_key(key: str) -> tuple[str, datetime]:
    """(match_id, ingested_at) encoded in a raw object key."""
    _, match_part, file_name = key.split("/")
    ingested_at = datetime.strptime(file_name.removesuffix(".html"), TIMESTAMP_FORMAT).replace(tzinfo=timezone.utc)
    return match_part.removeprefix("match_id="), ingested_at


def latest_keys(keys: list[str]) -> dict[str, str]:
    """Raw objects are append-only; pick the most recent copy of each match."""
    latest: dict[str, tuple[datetime, str]] = {}
    for key in keys:
        match_id, ingested_at = parse_raw_key(key)
        if match_id not in latest or ingested_at > latest[match_id][0]:
            latest[match_id] = (ingested_at, key)
    return {match_id: key for match_id, (_, key) in latest.items()}


def load_raw(lake: Lake) -> list[dict]:
    """Latest raw copy of every match as {"match_id", "html", "ingested_at"}."""
    rows = []
    for match_id, key in latest_keys(lake.keys("raw", DATASET)).items():
        rows.append(
            {
                "match_id": match_id,
                "html": lake.get("raw", DATASET, key).decode(),
                "ingested_at": parse_raw_key(key)[1],
            }
        )
    return rows


@asset(schedule=None)
def bronze_vlr_matches():
    """Fetch match pages not yet in the lake and store them untouched."""
    lake = get_lake()
    already = set(latest_keys(lake.keys("raw", DATASET)))
    listed = vlr.parse_event_match_ids(vlr.fetch(vlr.event_matches_path(EVENT_ID)))
    for match_id in (m for m in listed if m not in already):
        html = vlr.fetch(f"/{match_id}/")
        lake.put("raw", DATASET, raw_key(match_id, datetime.now(timezone.utc)), html)


def parse_bronze(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Parse raw match pages into (maps, rounds) records, Summit only."""
    maps, rounds = [], []
    for row in rows:
        parsed = vlr.parse_match(row["html"], row["match_id"])
        for m in parsed["maps"]:
            if m["map_name"] == SCOPE_MAP:
                maps.append({**m, "stage": parsed["match"]["stage"], "played_at_raw": parsed["match"]["played_at_raw"], "ingested_at": row["ingested_at"]})
        for r in parsed["rounds"]:
            if r["map_name"] == SCOPE_MAP:
                rounds.append({**r, "ingested_at": row["ingested_at"]})
    return maps, rounds


def _to_arrow(records: list[dict], schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pylist(records, schema=schema)


@asset(schedule=[bronze_vlr_matches])
def silver_vlr_maps():
    lake = get_lake()
    maps, _ = parse_bronze(load_raw(lake))
    silver = _to_arrow(maps, MAPS_SCHEMA)
    validate(silver, contract_path("silver_vlr_maps"))
    lake.write_dataset("silver", "vlr_maps", silver)


@asset(schedule=[bronze_vlr_matches])
def silver_vlr_rounds():
    lake = get_lake()
    _, rounds = parse_bronze(load_raw(lake))
    silver = _to_arrow(rounds, ROUNDS_SCHEMA)
    validate(silver, contract_path("silver_vlr_rounds"))
    lake.write_dataset("silver", "vlr_rounds", silver)
