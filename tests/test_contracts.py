from datetime import datetime, timezone

import pyarrow as pa
import pytest

from include.lib.contracts import ContractViolation, contract_path, validate

CONTRACT = contract_path("silver_agents")
ROLES = ["Duelist", "Initiator", "Controller", "Sentinel"]
NOW = datetime.now(timezone.utc)


def silver_table(n=24, **overrides):
    cols = {
        "agent_uuid": [f"uuid-{i}" for i in range(n)],
        "name": [f"agent-{i}" for i in range(n)],
        "role": [ROLES[i % 4] for i in range(n)],
        "ingested_at": pa.array([NOW] * n, pa.timestamp("us", tz="UTC")),
    }
    cols.update(overrides)
    return pa.table(cols)


def test_valid_table_passes():
    validate(silver_table(), CONTRACT)


def test_rejects_duplicates_and_too_few_rows():
    table = silver_table(n=2, agent_uuid=["a", "a"])
    with pytest.raises(ContractViolation, match="not unique.*at least 20 rows"):
        validate(table, CONTRACT)


def test_rejects_wrong_type():
    table = silver_table(ingested_at=[NOW.isoformat()] * 24)
    with pytest.raises(ContractViolation, match="ingested_at: expected timestamp"):
        validate(table, CONTRACT)


def test_rejects_role_outside_allowed_set():
    table = silver_table(role=["Duelist"] * 23 + ["Support"])
    with pytest.raises(ContractViolation, match="Support"):
        validate(table, CONTRACT)


def test_rejects_missing_lineage_column():
    with pytest.raises(ContractViolation, match="missing column: ingested_at"):
        validate(silver_table().drop_columns(["ingested_at"]), CONTRACT)


def test_contract_path_unknown_name():
    with pytest.raises(FileNotFoundError):
        contract_path("does_not_exist")
