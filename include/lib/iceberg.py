import os

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table


class SchemaMismatch(Exception):
    pass


def get_catalog():
    return load_catalog(
        "rest",
        **{
            "type": "rest",
            "uri": os.environ["ICEBERG_CATALOG_URI"],
            "warehouse": os.environ["ICEBERG_WAREHOUSE"],
            "s3.endpoint": os.environ["ICEBERG_S3_ENDPOINT"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.path-style-access": "true",
        },
    )


def _prepare_table(
    identifier: str,
    schema: pa.Schema,
    partition_by: list[str] | None,
    evolve: bool,
) -> Table:
    """Load or create the table, failing loudly on schema/partition drift.

    - Missing table: created from the Arrow schema, with identity partitions on `partition_by`.
    - New columns in the incoming data: added only if `evolve=True`, otherwise SchemaMismatch.
    - Columns missing from the incoming data, or a different partition spec: always SchemaMismatch.
    Type compatibility of shared columns is enforced by pyiceberg on write.
    """
    catalog = get_catalog()
    partition_by = partition_by or []
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        catalog.create_namespace_if_not_exists(identifier.split(".")[0])
        table = catalog.create_table(identifier, schema=schema)
        if partition_by:
            with table.update_spec() as spec:
                for col in partition_by:
                    spec.add_identity(col)
        return table

    current = {f.name for f in table.schema().fields}
    incoming = set(schema.names)
    added, removed = incoming - current, current - incoming
    if removed:
        raise SchemaMismatch(f"{identifier}: incoming data is missing columns {sorted(removed)}")
    if added:
        if not evolve:
            raise SchemaMismatch(
                f"{identifier}: incoming data has new columns {sorted(added)}; pass evolve=True to add them"
            )
        with table.update_schema() as update:
            update.union_by_name(schema)

    id_to_name = {f.field_id: f.name for f in table.schema().fields}
    current_parts = [id_to_name[p.source_id] for p in table.spec().fields]
    if current_parts != partition_by:
        raise SchemaMismatch(f"{identifier}: partitioned by {current_parts}, expected {partition_by}")
    return table


def overwrite_table(identifier: str, df: pa.Table, partition_by: list[str] | None = None, evolve: bool = False):
    """Full replace. Meant for small dimensions (agents, maps)."""
    table = _prepare_table(identifier, df.schema, partition_by, evolve)
    table.overwrite(df)
    return table


def append_table(identifier: str, df: pa.Table, partition_by: list[str] | None = None, evolve: bool = False):
    """Append-only write. Meant for facts (matches, rounds) partitioned by e.g. ["map", "patch"]."""
    table = _prepare_table(identifier, df.schema, partition_by, evolve)
    table.append(df)
    return table
