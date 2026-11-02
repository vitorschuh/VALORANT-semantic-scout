"""Phase 2: VALORANT Champions 2026 (Shanghai) from vlr.gg: bronze (raw HTML) -> contract -> silver (maps, rounds).

bronze.vlr_matches: one row per match page, raw HTML + lineage, append-only. Complete (all maps); scope filtering is silver's job.
silver.vlr_maps / silver.vlr_rounds: parsed from the latest bronze copy of each match, Summit only (initial scope).
"""
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
from airflow.sdk import asset

from include.lib import vlr
from include.lib.contracts import contract_path, validate
from include.lib.iceberg import append_table, get_catalog, overwrite_table

EVENT_ID = 2766  # VALORANT Champions 2026, Shanghai
SCOPE_MAP = "Summit"

BRONZE_SCHEMA = pa.schema(
    [
        ("match_id", pa.string()),
        ("event_id", pa.int64()),
        ("source_url", pa.string()),
        ("html", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)

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


def _ingested_match_ids() -> set[str]:
    try:
        return set(get_catalog().load_table("bronze.vlr_matches").scan(selected_fields=("match_id",)).to_arrow()["match_id"].to_pylist())
    except Exception as exc:  # table not created yet
        if type(exc).__name__ == "NoSuchTableError":
            return set()
        raise


@asset(schedule=None)
def bronze_vlr_matches():
    """Fetch match pages not yet in bronze. Completed matches are immutable, so they are fetched once."""
    listing_path = vlr.event_matches_path(EVENT_ID)
    match_ids = vlr.parse_event_match_ids(vlr.fetch(listing_path))
    todo = [m for m in match_ids if m not in _ingested_match_ids()]
    if not todo:
        return
    rows = []
    for match_id in todo:
        path = f"/{match_id}/"
        rows.append((match_id, f"{vlr.BASE_URL}{path}", vlr.fetch(path), datetime.now(timezone.utc)))
    table = pa.table(
        {
            "match_id": [r[0] for r in rows],
            "event_id": [EVENT_ID] * len(rows),
            "source_url": [r[1] for r in rows],
            "html": [r[2] for r in rows],
            "ingested_at": [r[3] for r in rows],
        },
        schema=BRONZE_SCHEMA,
    )
    append_table("bronze.vlr_matches", table)


def latest_per_match(bronze: pa.Table) -> pa.Table:
    """Bronze is append-only; keep the most recent copy of each match page."""
    ordered = bronze.sort_by([("match_id", "ascending"), ("ingested_at", "descending")])
    first = {}
    for i, mid in enumerate(ordered["match_id"].to_pylist()):
        first.setdefault(mid, i)
    return ordered.take(sorted(first.values()))


def parse_bronze(bronze: pa.Table) -> tuple[list[dict], list[dict]]:
    """Parse the latest bronze copy of every match into (maps, rounds) records, Summit only."""
    maps, rounds = [], []
    for row in latest_per_match(bronze).to_pylist():
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
    bronze = get_catalog().load_table("bronze.vlr_matches").scan().to_arrow()
    maps, _ = parse_bronze(bronze)
    silver = _to_arrow(maps, MAPS_SCHEMA)
    validate(silver, contract_path("silver_vlr_maps"))
    overwrite_table("silver.vlr_maps", silver)


@asset(schedule=[bronze_vlr_matches])
def silver_vlr_rounds():
    bronze = get_catalog().load_table("bronze.vlr_matches").scan().to_arrow()
    _, rounds = parse_bronze(bronze)
    silver = _to_arrow(rounds, ROUNDS_SCHEMA)
    validate(silver, contract_path("silver_vlr_rounds"))
    overwrite_table("silver.vlr_rounds", silver)
