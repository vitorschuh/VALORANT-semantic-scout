"""Phase 1: static metadata (agents) through raw -> contract -> silver, modelled as Airflow 3 assets.

raw/agents: the API response exactly as received, wrapped with lineage (source_url, ingested_at); one object per run.
silver/agents: flattened + typed from the latest raw object, gated by the silver_agents contract.
"""
import json
from datetime import datetime, timezone

import pyarrow as pa
import requests
from airflow.sdk import asset

from include.lib.contracts import contract_path, validate
from include.lib.lake import get_lake

AGENTS_URL = "https://valorant-api.com/v1/agents?isPlayableCharacter=true"


@asset(schedule="@daily")
def bronze_agents():
    response = requests.get(AGENTS_URL, timeout=30)
    response.raise_for_status()
    ingested_at = datetime.now(timezone.utc)
    envelope = {
        "source_url": AGENTS_URL,
        "ingested_at": ingested_at.isoformat(),
        "records": response.json()["data"],
    }
    get_lake().put("raw", "agents", f"ingested_at={ingested_at:%Y%m%dT%H%M%SZ}/agents.json", json.dumps(envelope))


def flatten_agents(envelope: dict) -> pa.Table:
    """Project the raw API records into typed silver columns."""
    records = envelope["records"]
    ingested_at = datetime.fromisoformat(envelope["ingested_at"])
    return pa.table(
        {
            "agent_uuid": [a["uuid"] for a in records],
            "name": [a["displayName"] for a in records],
            "role": [(a.get("role") or {}).get("displayName") for a in records],
            "ingested_at": pa.array([ingested_at] * len(records), pa.timestamp("us", tz="UTC")),
        }
    )


@asset(schedule=[bronze_agents])
def silver_agents():
    lake = get_lake()
    latest = max(lake.keys("raw", "agents"))  # keys embed a sortable UTC timestamp
    silver = flatten_agents(json.loads(lake.get("raw", "agents", latest)))
    validate(silver, contract_path("silver_agents"))
    lake.write_dataset("silver", "agents", silver)
