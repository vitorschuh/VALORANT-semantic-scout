"""Data lake on S3 (MinIO locally): raw objects and Parquet datasets under <bucket>/{raw,silver,gold,quarantine}.

Layout:
    raw/<dataset>/<key>               source payloads exactly as received (HTML/JSON), never rewritten
    silver/<dataset>/part-*.parquet   parsed and typed, contract-checked
    gold/<dataset>/part-*.parquet     serving-ready
    quarantine/<dataset>/<key>        batches rejected by a data contract, with their violation report

The filesystem is injected (pyarrow.fs), so tests run against a temporary directory instead of S3.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

LAYERS = ("raw", "silver", "gold", "quarantine")


class Lake:
    def __init__(self, fs: pafs.FileSystem, root: str):
        self.fs = fs
        self.root = root.rstrip("/")

    def path(self, layer: str, dataset: str, *parts: str) -> str:
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}, expected one of {LAYERS}")
        return "/".join([self.root, layer, dataset, *parts])

    def _ensure_parent(self, path: str) -> None:
        # S3 has no directories; a local filesystem needs them to exist before writing.
        if isinstance(self.fs, pafs.LocalFileSystem):
            self.fs.create_dir(path.rsplit("/", 1)[0], recursive=True)

    def _files(self, base: str) -> list[str]:
        selector = pafs.FileSelector(base, recursive=True, allow_not_found=True)
        return sorted(info.path for info in self.fs.get_file_info(selector) if info.type == pafs.FileType.File)

    # --- raw and quarantine: opaque objects ---------------------------------------------------
    def put(self, layer: str, dataset: str, key: str, data: bytes | str) -> str:
        path = self.path(layer, dataset, key)
        self._ensure_parent(path)
        with self.fs.open_output_stream(path) as out:
            out.write(data.encode() if isinstance(data, str) else data)
        return path

    def keys(self, layer: str, dataset: str) -> list[str]:
        """Object keys of a dataset, relative to its prefix, sorted."""
        base = self.path(layer, dataset)
        return [p[len(base) + 1 :] for p in self._files(base)]

    def get(self, layer: str, dataset: str, key: str) -> bytes:
        with self.fs.open_input_stream(self.path(layer, dataset, key)) as src:
            return src.read()

    # --- silver and gold: Parquet datasets ----------------------------------------------------
    def write_dataset(self, layer: str, dataset: str, table: pa.Table) -> str:
        """Replace a dataset. The new file is written before the old ones are deleted."""
        base = self.path(layer, dataset)
        previous = self._files(base)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        path = f"{base}/part-{stamp}-{uuid.uuid4().hex[:8]}.parquet"
        self._ensure_parent(path)
        pq.write_table(table, path, filesystem=self.fs)
        for old in previous:
            self.fs.delete_file(old)
        return path

    def read_dataset(self, layer: str, dataset: str) -> pa.Table:
        return pq.read_table(self.path(layer, dataset), filesystem=self.fs)


def get_lake() -> Lake:
    """Lake on the MinIO/S3 endpoint configured through environment variables."""
    endpoint = os.environ["LAKE_S3_ENDPOINT"]
    fs = pafs.S3FileSystem(
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        endpoint_override=endpoint.split("://", 1)[-1],
        scheme=endpoint.split("://", 1)[0] if "://" in endpoint else "http",
        region=os.environ.get("AWS_REGION", "us-east-1"),
    )
    return Lake(fs, os.environ.get("LAKE_BUCKET", "lake"))
