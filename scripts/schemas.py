"""Pipeline metadata schemas.

Every YAML metadata file written or read by the pipeline should pass through
one of these models, so a typo or a drift in shape fails loudly at the boundary
rather than propagating silently downstream.
"""
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Checksums(_Strict):
    csv_xxh3_64: str
    json_xxh3_64: str


class SnapshotMeta(_Strict):
    """Per-version snapshot metadata: data/snapshots/<source>/<dataset>/<version>/<dataset>.meta.yaml"""
    dataset: str
    source: str
    download_date: str
    original_url: str
    etag: Optional[str] = None
    checksums: Checksums


class CurrentPointer(_Strict):
    """Latest-version pointer: data/snapshots/<source>/<dataset>/current.yaml"""
    version: str
    etag: Optional[str] = None
    csv_hash: str


class ColumnMeta(_Strict):
    name: str
    source_title: Optional[str] = None
    title_short: Optional[str] = None
    unit: Optional[str] = None
    short_unit: Optional[str] = None
    owid_variable_id: Optional[int] = None
    last_updated: Optional[str] = None
    next_update: Optional[str] = None
    citation_short: Optional[str] = None
    citation_long: Optional[str] = None
    description_short: Optional[str] = None


class StandardisedMeta(_Strict):
    """Standardised dataset metadata: data/standardised/<source>/<dataset>/<dataset>.meta.yaml"""
    dataset: str
    source: str
    snapshot_version: str
    title: Optional[str] = None
    citation: Optional[str] = None
    original_chart_url: Optional[str] = None
    default_selection: list[str] = Field(default_factory=list)
    columns: list[ColumnMeta]


def write_yaml(model: BaseModel, path: Path) -> None:
    path.write_text(
        yaml.dump(model.model_dump(mode="json"), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def read_yaml(model_cls: type[BaseModel], path: Path) -> BaseModel:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return model_cls.model_validate(data)
