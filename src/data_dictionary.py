from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import pandas as pd

from . import config, schema

DATA_DICTIONARY_COLUMNS = [
    "dataset",
    "field_name",
    "field_path",
    "description",
    "data_type_observed",
    "data_type_expected",
    "nullable",
    "allowed_values_or_range",
    "example_redacted",
    "pii_classification",
    "transform_lineage",
    "used_in_rules",
    "present_in_outputs",
]


def _is_blank(value: Any) -> bool:
    """Return True when a scalar value is null-like or blank string-like."""
    if value is None:
        return True
    if isinstance(value, float) and np.isnan(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _mask_example(field_name: str, value: Any) -> str:
    """Return a redacted string representation for example values."""
    if _is_blank(value):
        return ""
    key = field_name.lower()
    if key.endswith("_flag") or key.startswith("flag_") or isinstance(value, (bool, np.bool_)):
        return str(value)
    text = str(value)
    if "ssn" in key:
        return "***-**-" + text[-4:]
    if "email" in key:
        if "@" in text:
            local, domain = text.split("@", 1)
            local_mask = local[:1] + "***" if local else "***"
            return f"{local_mask}@{domain}"
        return "[REDACTED_EMAIL]"
    if "ip_address" in key or key.endswith("_ip") or key.startswith("ip_") or "_ip_" in key:
        return "[REDACTED_IP]"
    if "name" in key:
        return "[REDACTED_NAME]"
    if "date_of_birth" in key:
        year = text[:4] if len(text) >= 4 else "XXXX"
        return f"{year}-**-**"
    if len(text) > 40:
        return text[:37] + "..."
    return text


def _infer_observed_dtype_from_values(values: list[Any]) -> str:
    """Infer a concise observed dtype label from a list of python values."""
    non_blank = [v for v in values if not _is_blank(v)]
    if not non_blank:
        return "unknown"
    type_names = sorted({type(v).__name__ for v in non_blank})
    if len(type_names) == 1:
        return type_names[0]
    return "|".join(type_names)


def _infer_observed_dtype_from_series(series: pd.Series) -> str:
    """Infer observed dtype for a pandas Series with object-aware handling."""
    if series.empty:
        return "unknown"
    if pd.api.types.is_bool_dtype(series):
        return "bool"
    if pd.api.types.is_integer_dtype(series):
        return "int"
    if pd.api.types.is_float_dtype(series):
        return "float"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    non_blank = series.dropna()
    if non_blank.empty:
        return str(series.dtype)
    sample_types = sorted({type(v).__name__ for v in non_blank.head(100)})
    if len(sample_types) == 1:
        return sample_types[0]
    return "|".join(sample_types)


def _raw_field_tracker() -> dict[str, dict[str, Any]]:
    """Create tracking storage for raw JSON field observations."""
    return defaultdict(
        lambda: {
            "types": set(),
            "values": [],
            "null_count": 0,
            "total_count": 0,
            "example": None,
        }
    )


def _record_scalar_field(tracker: dict[str, dict[str, Any]], path: str, value: Any) -> None:
    """Record one scalar observation for a raw JSON path."""
    bucket = tracker[path]
    bucket["total_count"] += 1
    bucket["values"].append(value)
    if _is_blank(value):
        bucket["null_count"] += 1
    else:
        bucket["types"].add(type(value).__name__)
        if bucket["example"] is None:
            bucket["example"] = value


def _walk_raw_record(
    tracker: dict[str, dict[str, Any]],
    value: Any,
    path: str = "",
) -> None:
    """Recursively walk raw JSON structures and record scalar/list field paths."""
    if isinstance(value, dict):
        for key, item in value.items():
            next_path = f"{path}.{key}" if path else key
            _walk_raw_record(tracker=tracker, value=item, path=next_path)
        return

    if isinstance(value, list):
        list_path = f"{path}[]"
        if not value:
            _record_scalar_field(tracker, list_path, None)
            return
        for item in value:
            if isinstance(item, dict):
                _walk_raw_record(tracker=tracker, value=item, path=list_path)
            else:
                _record_scalar_field(tracker, list_path, item)
        return

    _record_scalar_field(tracker, path, value)


def _collect_raw_field_inventory(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Build raw JSON field inventory with observed nullability and type hints."""
    tracker = _raw_field_tracker()
    for record in records:
        _walk_raw_record(tracker=tracker, value=record)

    rows: list[dict[str, Any]] = []
    for field_path, meta in sorted(tracker.items()):
        if field_path == "":
            continue
        rows.append(
            {
                "field_path": field_path,
                "data_type_observed": _infer_observed_dtype_from_values(meta["values"]),
                "nullable": bool(meta["null_count"] > 0),
                "example_value": meta["example"],
            }
        )
    return pd.DataFrame(rows)


def _build_rule_usage_map(rule_catalog_df: pd.DataFrame) -> dict[str, set[str]]:
    """Map source column tokens to associated rule IDs."""
    usage: dict[str, set[str]] = defaultdict(set)
    for _, row in rule_catalog_df.iterrows():
        rule_id = str(row.get("rule_id", "")).strip()
        source_columns = str(row.get("source_columns", "")).strip()
        if not rule_id or not source_columns:
            continue
        for token in source_columns.split("|"):
            token_clean = token.strip()
            if token_clean:
                usage[token_clean].add(rule_id)
    return usage


def _default_allowed_hint(field_name: str) -> str:
    """Return static allowed-value/range hints for common governed fields."""
    key = field_name.lower()
    if "gender" in key:
        return "Male|Female"
    if "debt_to_income" in key:
        return "[0,1]"
    if "credit_history_months" in key:
        return ">=0"
    if "savings_balance" in key:
        return ">=0"
    if field_name.endswith("_flag") or field_name.startswith("flag_"):
        return "True|False"
    return ""


def _allowed_hint_from_series(series: pd.Series, field_name: str) -> str:
    """Derive compact allowed-value hint from observed data where practical."""
    default_hint = _default_allowed_hint(field_name)
    if default_hint:
        return default_hint
    non_null = series.dropna()
    if non_null.empty:
        return ""
    if non_null.nunique() <= 10 and not pd.api.types.is_numeric_dtype(non_null):
        values = sorted({str(v) for v in non_null.tolist()})
        return "|".join(values)
    if pd.api.types.is_numeric_dtype(non_null):
        return f"[{non_null.min()}, {non_null.max()}]"
    return ""


def _classification_for_column(
    column: str,
    col_meta_lookup: dict[str, dict[str, Any]],
    pii_field_lookup: dict[str, str],
) -> str:
    """Infer PII class for a dataframe column."""
    if column in col_meta_lookup:
        return str(col_meta_lookup[column].get("classification", "Non-PII"))
    if column in pii_field_lookup:
        return pii_field_lookup[column]
    key = column.lower()
    if any(token in key for token in ("ssn", "email", "ip_address", "full_name", "date_of_birth")):
        return "PII"
    if any(token in key for token in ("zip", "pseudo_id", "application_id")):
        return "Quasi-PII"
    return "Non-PII"


def _description_for_column(column: str, col_meta_lookup: dict[str, dict[str, Any]]) -> str:
    """Build a human-readable description for an output column."""
    if column in col_meta_lookup:
        return str(col_meta_lookup[column].get("notes", ""))
    if column.startswith("clean_"):
        return "Standardized/cleaned field derived during remediation."
    if column.startswith("raw_"):
        return "Raw passthrough field from source JSON."
    if column.endswith("_flag") or column.startswith("flag_"):
        return "Derived validation/remediation indicator."
    if column in {"applicant_pseudo_id", "pseudo_id_source", "pseudo_id_fallback_used_flag"}:
        return "Privacy-preserving pseudonymization output."
    if column == "age_band":
        return "Derived privacy-preserving age grouping."
    return "Pipeline output attribute."


def _lineage_for_column(column: str, dataset_columns: set[str]) -> str:
    """Infer a concise lineage statement for output columns."""
    if column.startswith("raw_"):
        return "source_json -> raw passthrough"
    if column.startswith("clean_"):
        candidate = "raw_" + column.removeprefix("clean_")
        if candidate in dataset_columns:
            return f"{candidate} -> {column}"
        return f"cleaning_logic -> {column}"
    if column.endswith("_flag") or column.startswith("flag_"):
        return "validation_or_cleaning_logic -> flag"
    if column in {"applicant_pseudo_id", "pseudo_id_source", "age_band"}:
        return "derived_for_analysis_privacy"
    return "pipeline_derived_or_passthrough"


def _present_in_outputs_for_column(
    column: str,
    output_datasets: dict[str, pd.DataFrame],
) -> str:
    """List output datasets that include a given column."""
    present = [name for name, df in output_datasets.items() if column in df.columns]
    return "|".join(sorted(present))


def _build_column_meta_lookup(schema_dictionary_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    """Build lookup from schema column names to schema metadata rows."""
    lookup: dict[str, dict[str, Any]] = {}
    for _, row in schema_dictionary_df.iterrows():
        column = str(row.get("column", "")).strip()
        if column and column not in lookup:
            lookup[column] = row.to_dict()
    return lookup


def _build_path_meta_lookup(schema_dictionary_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    """Build lookup from schema field paths to schema metadata rows."""
    lookup: dict[str, dict[str, Any]] = {}
    for _, row in schema_dictionary_df.iterrows():
        field_path = str(row.get("field_path", "")).strip()
        if field_path and field_path not in lookup:
            lookup[field_path] = row.to_dict()
    return lookup


def _build_pii_field_lookup(pii_inventory_df: pd.DataFrame) -> dict[str, str]:
    """Build lookup from field identifier to pii classification from pii inventory."""
    if pii_inventory_df.empty:
        return {}
    return {
        str(row["field_path"]): str(row["classification"])
        for _, row in pii_inventory_df.iterrows()
        if str(row.get("field_path", "")).strip() != ""
    }


def build_data_dictionary(
    *,
    records: list[dict[str, Any]],
    output_datasets: dict[str, pd.DataFrame],
    rule_catalog_df: pd.DataFrame,
    schema_dictionary_df: pd.DataFrame | None = None,
    pii_inventory_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build canonical data dictionary across raw JSON attributes and output datasets."""
    schema_df = schema_dictionary_df if schema_dictionary_df is not None else schema.schema_dictionary_df()
    pii_df = pii_inventory_df if pii_inventory_df is not None else pd.DataFrame()
    rule_usage = _build_rule_usage_map(rule_catalog_df=rule_catalog_df)
    col_meta_lookup = _build_column_meta_lookup(schema_df)
    path_meta_lookup = _build_path_meta_lookup(schema_df)
    pii_lookup = _build_pii_field_lookup(pii_df)

    path_to_columns: dict[str, list[str]] = defaultdict(list)
    for _, row in schema_df.iterrows():
        path = str(row.get("field_path", "")).strip()
        column = str(row.get("column", "")).strip()
        if path and column:
            path_to_columns[path].append(column)

    rows: list[dict[str, Any]] = []

    raw_inventory = _collect_raw_field_inventory(records=records)
    for _, raw_row in raw_inventory.iterrows():
        field_path = str(raw_row["field_path"])
        field_name = field_path.split(".")[-1]
        field_meta = path_meta_lookup.get(field_path, {})
        mapped_columns = path_to_columns.get(field_path, [])

        rule_ids = set()
        for token in [field_path, *mapped_columns]:
            rule_ids.update(rule_usage.get(token, set()))

        present_outputs: set[str] = set()
        for column in mapped_columns:
            for dataset_name, df in output_datasets.items():
                if column in df.columns:
                    present_outputs.add(dataset_name)

        rows.append(
            {
                "dataset": "raw_json",
                "field_name": field_name,
                "field_path": field_path,
                "description": str(field_meta.get("notes", "Raw JSON attribute.")),
                "data_type_observed": str(raw_row["data_type_observed"]),
                "data_type_expected": str(field_meta.get("expected_dtype", "")),
                "nullable": bool(raw_row["nullable"]),
                "allowed_values_or_range": _default_allowed_hint(field_name),
                "example_redacted": _mask_example(field_name, raw_row["example_value"]),
                "pii_classification": str(field_meta.get("classification", pii_lookup.get(field_path, "Non-PII"))),
                "transform_lineage": "source_json",
                "used_in_rules": "|".join(sorted(rule_ids)),
                "present_in_outputs": "|".join(sorted(present_outputs)),
            }
        )

    for dataset_name, df in output_datasets.items():
        dataset_columns = set(df.columns)
        for column in df.columns:
            series = df[column]
            col_meta = col_meta_lookup.get(column, {})
            field_path = str(col_meta.get("field_path", column))
            used_in_rules = sorted(rule_usage.get(column, set()))
            example_value = None
            non_blank = series[~series.apply(_is_blank)] if len(series.index) else pd.Series(dtype="object")
            if len(non_blank.index):
                example_value = non_blank.iloc[0]

            rows.append(
                {
                    "dataset": dataset_name,
                    "field_name": column,
                    "field_path": field_path,
                    "description": _description_for_column(column, col_meta_lookup),
                    "data_type_observed": _infer_observed_dtype_from_series(series),
                    "data_type_expected": str(col_meta.get("expected_dtype", "")),
                    "nullable": bool(series.isna().any()),
                    "allowed_values_or_range": _allowed_hint_from_series(series, column),
                    "example_redacted": _mask_example(column, example_value),
                    "pii_classification": _classification_for_column(column, col_meta_lookup, pii_lookup),
                    "transform_lineage": _lineage_for_column(column, dataset_columns),
                    "used_in_rules": "|".join(used_in_rules),
                    "present_in_outputs": _present_in_outputs_for_column(column, output_datasets),
                }
            )

    dictionary_df = pd.DataFrame(rows, columns=DATA_DICTIONARY_COLUMNS)
    dictionary_df = dictionary_df.sort_values(["dataset", "field_name"]).reset_index(drop=True)
    return dictionary_df


def build_data_dictionary_business_view(data_dictionary_df: pd.DataFrame) -> pd.DataFrame:
    """Build business-friendly dictionary view focused on meaning and privacy."""
    columns = [
        "dataset",
        "field_name",
        "description",
        "data_type_expected",
        "allowed_values_or_range",
        "pii_classification",
        "present_in_outputs",
    ]
    return data_dictionary_df[columns].drop_duplicates().reset_index(drop=True)


def build_data_dictionary_lineage_view(data_dictionary_df: pd.DataFrame) -> pd.DataFrame:
    """Build technical lineage-oriented dictionary view for engineering users."""
    columns = [
        "dataset",
        "field_name",
        "field_path",
        "transform_lineage",
        "used_in_rules",
        "present_in_outputs",
        "pii_classification",
    ]
    return data_dictionary_df[columns].drop_duplicates().reset_index(drop=True)
