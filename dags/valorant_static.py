"""Phase 1: static metadata (agents) through bronze -> contract -> silver, modelled as Airflow 3 assets.

bronze.agents: raw API payload, one row per agent, append-only, with lineage columns.
silver.agents: flattened + typed from the latest bronze ingestion, gated by the silver_agents contract.
"""
import json
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import requests
from airflow.sdk import asset

from include.lib.contracts import contract_path, validate
from include.lib.iceberg import append_table, get_catalog, overwrite_table

AGENTS_URL = "https://valorant-api.com/v1/agents?isPlayableCharacter=true"

BRONZE_SCHEMA = pa.schema(
    [
        ("payload", pa.string()),
        ("source_url", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)


@asset(schedule="@daily")
def bronze_agents():
    response = requests.get(AGENTS_URL, timeout=30)
    response.raise_for_status()
    records = response.json()["data"]
    ingested_at = datetime.now(timezone.utc)
    table = pa.table(
        {
            "payload": [json.dumps(r) for r in records],
            "source_url": [AGENTS_URL] * len(records),
            "ingested_at": [ingested_at] * len(records),
        },
        schema=BRONZE_SCHEMA,
    )
    append_table("bronze.agents", table)


def flatten_agents(bronze: pa.Table) -> pa.Table:
    """Keep only the latest ingestion and project the raw payload into typed silver columns."""
    latest = pc.max(bronze["ingested_at"])
    bronze = bronze.filter(pc.equal(bronze["ingested_at"], latest))
    payloads = [json.loads(p) for p in bronze["payload"].to_pylist()]
    return pa.table(
        {
            "agent_uuid": [a["uuid"] for a in payloads],
            "name": [a["displayName"] for a in payloads],
            "role": [(a.get("role") or {}).get("displayName") for a in payloads],
            "ingested_at": bronze["ingested_at"],
        }
    )


@asset(schedule=[bronze_agents])
def silver_agents():
    bronze = get_catalog().load_table("bronze.agents").scan().to_arrow()
    silver = flatten_agents(bronze)
    validate(silver, contract_path("silver_agents"))
    overwrite_table("silver.agents", silver)
