"""Represent tabular source files without losing column identity.

CSV and TSV are file formats at the SDK boundary. Inside the client, columns are
identified by position-derived keys so duplicate display headers remain distinct.
"""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class TabularFormat(str, Enum):
    CSV = "csv"
    TSV = "tsv"

    @property
    def delimiter(self) -> str:
        if self == TabularFormat.CSV:
            return ","
        return "\t"

    @property
    def suffix(self) -> str:
        return f".{self.value}"

    @property
    def content_type(self) -> str:
        if self == TabularFormat.CSV:
            return "text/csv"
        return "text/tab-separated-values"


@dataclass(frozen=True)
class TabularColumn:
    key: str
    index: int
    header: str


@dataclass(frozen=True)
class TabularDataset:
    columns: list[TabularColumn]
    rows: list[list[str]]
    source_format: TabularFormat

    @property
    def headers(self) -> list[str]:
        return [column.header for column in self.columns]

    def backend_column_names(self) -> list[str]:
        return [_backend_column_name(column) for column in self.columns]


SUPPORTED_TABULAR_SUFFIXES: dict[str, TabularFormat] = {
    ".csv": TabularFormat.CSV,
    ".tsv": TabularFormat.TSV,
}
_CSV_CONTENT_TYPES = {"text/csv", "application/csv", "application/vnd.ms-excel"}
_TSV_CONTENT_TYPES = {"text/tab-separated-values", "text/tsv", "text/plain"}


def column_key_for_index(index: int) -> str:
    return f"col_{index:04d}"


def column_index_from_key(column_key: str) -> int | None:
    prefix = "col_"
    if not column_key.startswith(prefix):
        return None
    suffix = column_key[len(prefix):]
    if not suffix.isdigit():
        return None
    return int(suffix)


def tabular_format_for_path(path: Path) -> TabularFormat:
    file_format = SUPPORTED_TABULAR_SUFFIXES.get(path.suffix.lower())
    if file_format is None:
        raise ValueError(f"unsupported tabular file extension: {path.suffix}")
    return file_format


def get_tabular_format(path: Path, content_type: str | None = None) -> TabularFormat:
    file_format = SUPPORTED_TABULAR_SUFFIXES.get(path.suffix.lower())
    if file_format is not None:
        return file_format

    normalized_content_type = (content_type or "").lower()
    if normalized_content_type in _CSV_CONTENT_TYPES:
        return TabularFormat.CSV
    if normalized_content_type in _TSV_CONTENT_TYPES:
        return TabularFormat.TSV

    raise ValueError(f"unsupported tabular file extension: {path.suffix or '<none>'}")


def is_supported_tabular_content_type(content_type: str, file_format: TabularFormat) -> bool:
    normalized = content_type.lower()
    if file_format == TabularFormat.CSV:
        return normalized in _CSV_CONTENT_TYPES
    return normalized in _TSV_CONTENT_TYPES


def read_tabular(path: Path) -> TabularDataset:
    source_format = tabular_format_for_path(path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=source_format.delimiter)
        default_headers: list[str] = []
        headers = next(reader, default_headers)
        raw_rows = [list(row) for row in reader]

    width = max([len(headers), *(len(row) for row in raw_rows)] or [0])
    normalized_headers = [*headers, *([""] * (width - len(headers)))]
    columns = [
        TabularColumn(key=column_key_for_index(index), index=index, header=header)
        for index, header in enumerate(normalized_headers)
    ]
    return TabularDataset(
        columns=columns,
        rows=[_normalize_row(row, width) for row in raw_rows],
        source_format=source_format,
    )


def write_tabular(path: Path, dataset: TabularDataset) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=dataset.source_format.delimiter, lineterminator="\n")
        writer.writerow(dataset.headers)
        writer.writerows(dataset.rows)


def dataset_from_rows(
    *,
    columns: list[TabularColumn] | None = None,
    headers: list[str] | None = None,
    rows: list[list[str]],
    source_format: TabularFormat,
) -> TabularDataset:
    base_headers = [column.header for column in columns] if columns is not None else (headers or [])
    width = max([len(base_headers), *(len(row) for row in rows)] or [0])
    normalized_headers = [*base_headers, *([""] * (width - len(base_headers)))]
    output_columns = _columns_for_headers(normalized_headers, columns)
    return TabularDataset(
        columns=output_columns,
        rows=[_normalize_row(row, width) for row in rows],
        source_format=source_format,
    )


def csv_bytes_to_dataset(content: bytes, source_format: TabularFormat) -> TabularDataset:
    text = content.decode("utf-8-sig")
    rows = list(csv.reader(io.StringIO(text)))
    headers = rows[0] if rows else []
    data_rows = rows[1:] if len(rows) > 1 else []
    return dataset_from_rows(headers=headers, rows=data_rows, source_format=source_format)


def _backend_column_name(column: TabularColumn) -> str:
    return f"{column.key}__{_safe_header_token(column.header)}"


def _columns_for_headers(headers: list[str], columns: list[TabularColumn] | None) -> list[TabularColumn]:
    if columns is None:
        return [
            TabularColumn(key=column_key_for_index(index), index=index, header=header)
            for index, header in enumerate(headers)
        ]
    if len(columns) >= len(headers):
        return columns[:len(headers)]
    extra_columns = [
        TabularColumn(key=column_key_for_index(index), index=index, header=headers[index])
        for index in range(len(columns), len(headers))
    ]
    return [*columns, *extra_columns]


def _safe_header_token(header: str) -> str:
    cleaned = "".join(char if char.isalnum() else "_" for char in header.strip().lower())
    collapsed = "_".join(part for part in cleaned.split("_") if part)
    return collapsed or "blank"


def _normalize_row(row: list[str], width: int) -> list[str]:
    if len(row) >= width:
        return row
    return [*row, *([""] * (width - len(row)))]


__all__ = [
    "SUPPORTED_TABULAR_SUFFIXES",
    "TabularColumn",
    "TabularDataset",
    "TabularFormat",
    "column_index_from_key",
    "column_key_for_index",
    "csv_bytes_to_dataset",
    "dataset_from_rows",
    "get_tabular_format",
    "is_supported_tabular_content_type",
    "read_tabular",
    "tabular_format_for_path",
    "write_tabular",
]
