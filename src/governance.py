from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import hashlib
import re

import pandas as pd


# =============================================================================
# Privacy demo helpers (Governance Officer)
# =============================================================================

def find_project_root(start: Path | None = None) -> Path:
    """
    Locate the project root by searching upwards for src/__init__.py.
    This makes path handling robust regardless of the current working directory.
    """
    cwd = (start or Path.cwd()).resolve()
    for p in [cwd, *cwd.parents]:
        if (p / "src" / "__init__.py").exists():
            return p
    raise FileNotFoundError("Could not find project root containing src/__init__.py")


def default_privacy_paths() -> "PrivacyPaths":
    """
    Build standard PrivacyPaths using the project root.
    Use this in notebooks to avoid hardcoding relative paths.
    """
    root = find_project_root()
    return PrivacyPaths(
        pii_inventory=root / "data" / "quality" / "pii_inventory.csv",
        applications_analysis=root / "data" / "curated" / "applications_analysis.csv",
        applications_curated_full=root / "data" / "curated" / "applications_curated_full.csv",
        dq_postclean=root / "data" / "quality" / "reports" / "post" / "data_quality_report_postclean.csv",
    )
    

@dataclass(frozen=True)
class PrivacyPaths:
    """Typed container for the file paths used in the privacy notebook."""

    pii_inventory: Path
    applications_analysis: Path
    applications_curated_full: Path
    dq_postclean: Path


def load_privacy_inputs(paths: PrivacyPaths) -> dict[str, pd.DataFrame]:
    """
    Load all datasets required for the privacy notebook section.

    This function is intentionally strict:
    - Fails early if any file is missing (strong reproducibility / control evidence).
    - Returns a dict of DataFrames for convenient notebook usage.
    """
    missing = [p for p in paths.__dict__.values() if not Path(p).exists()]
    if missing:
        raise FileNotFoundError("Missing required files:\n" + "\n".join([f"- {m}" for m in missing]))

    return {
        "pii": pd.read_csv(paths.pii_inventory),
        "analysis": pd.read_csv(paths.applications_analysis),
        "curated": pd.read_csv(paths.applications_curated_full),
        "dq_post": pd.read_csv(paths.dq_postclean),
    }


# =============================================================================
# PII inventory: schema alignment + normalization
# =============================================================================


def infer_inventory_columns(df: pd.DataFrame) -> dict[str, str | None]:
    """Infer pii_inventory.csv schema using your project column names."""
    cols = {c.lower(): c for c in df.columns}

    def get(name: str) -> str | None:
        return cols.get(name.lower())

    return {
        "field": get("field_path"),
        "class": get("classification"),
        "raw": get("present_in_raw"),
        "curated": get("present_in_curated"),
        "analysis": get("present_in_analysis"),
    }


def normalize_pii_inventory(pii: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str], dict[str, str | None]]:
    """
    Normalize the PII inventory to a canonical representation.

    Output:
    - inv: inventory with normalized helper columns:
        * _field: stripped field name
        * _class: normalized class label (lowercased; quasi-identifier -> quasi)
    - direct_fields: list of "direct" PII fields
    - quasi_fields: list of quasi/proxy fields
    - inv_map: inferred schema mapping from infer_inventory_columns()
    """
    inv = pii.copy()
    inv_map = infer_inventory_columns(inv)

    if inv_map["field"] is None or inv_map["class"] is None:
        raise ValueError(
            "Could not infer PII inventory schema. Expected columns: "
            "field_path, classification, present_in_raw, present_in_curated, present_in_analysis. "
            f"Found columns: {list(inv.columns)}"
        )

    inv["_field"] = inv[inv_map["field"]].astype(str).str.strip()
    inv["_class"] = inv[inv_map["class"]].astype(str).str.strip().str.lower()

    # Normalize common label variants
    inv["_class"] = (
        inv["_class"]
        .str.replace("quasi-identifier", "quasi", regex=False)
        .str.replace("quasi identifier", "quasi", regex=False)
        .str.replace("direct pii", "direct", regex=False)
    )

    direct_fields = sorted(inv.loc[inv["_class"].eq("direct"), "_field"].dropna().unique().tolist())
    quasi_fields = sorted(inv.loc[inv["_class"].isin(["quasi", "proxy"]), "_field"].dropna().unique().tolist())

    return inv, direct_fields, quasi_fields, inv_map


def build_pii_presence_table(
    inv: pd.DataFrame,
    inv_map: dict[str, str | None],
    analysis_cols: set[str],
    curated_cols: set[str],
) -> pd.DataFrame:
    """
    Build a governance-friendly table: "PII class -> where it appears (raw/curated/analysis)".

    Uses the inventory presence flags (present_in_raw/curated/analysis).
    If flags are missing, falls back to column intersection (defensive).
    """
    has_layer_flags = all(inv_map.get(layer) is not None for layer in ["raw", "curated", "analysis"])

    if has_layer_flags:
        tbl = inv[["_field", "_class"]].copy()

        def to_bool(series: pd.Series) -> pd.Series:
            if series.dtype == bool:
                return series
            return series.astype(str).str.strip().str.lower().isin(["1", "true", "yes", "y", "present"])

        tbl["raw"] = to_bool(inv[inv_map["raw"]])  # type: ignore[index]
        tbl["curated"] = to_bool(inv[inv_map["curated"]])  # type: ignore[index]
        tbl["analysis"] = to_bool(inv[inv_map["analysis"]])  # type: ignore[index]
    else:
        # Fallback: infer presence by dataset column intersection
        tbl = inv[["_field", "_class"]].copy()
        tbl["raw"] = pd.NA
        tbl["curated"] = tbl["_field"].isin(curated_cols)
        tbl["analysis"] = tbl["_field"].isin(analysis_cols)

    return (
        tbl.rename(columns={"_field": "field", "_class": "pii_class"})
        .sort_values(["pii_class", "field"])
        .reset_index(drop=True)
    )


# =============================================================================
# Data minimisation + defence-in-depth
# =============================================================================


def assert_no_direct_pii_in_analysis(direct_fields: list[str], analysis: pd.DataFrame) -> list[str]:
    """
    Enforce the key privacy control: Direct PII must not be present in the analysis/model dataset.

    Returns the list of direct PII fields found in analysis (should be empty).
    Raises AssertionError if any are found (fail fast).
    """
    analysis_cols = set(map(str, analysis.columns))
    direct_in_analysis = sorted(set(direct_fields).intersection(analysis_cols))

    if direct_in_analysis:
        raise AssertionError(
            "DATA MINIMISATION FAILURE: Direct PII columns found in applications_analysis.csv: "
            + ", ".join(direct_in_analysis)
        )

    return direct_in_analysis


def heuristic_suspicious_columns(df: pd.DataFrame) -> list[str]:
    """
    Defence-in-depth heuristic scan for columns that *look like* they might contain direct identifiers.

    This is not the authoritative check (the inventory is), but it helps detect naming drift.
    """
    pattern = re.compile(
        r"(name|email|e-mail|ssn|social|ip|dob|date_of_birth|birth|phone|passport|national_id)",
        re.IGNORECASE,
    )
    return sorted([c for c in df.columns if pattern.search(str(c))])


# =============================================================================
# Safe preview utilities (avoid PII leakage in screenshots)
# =============================================================================


def safe_preview_curated(curated: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """
    Produce a small preview of the curated dataset that is safe for screenshots.

    Rationale:
    - Curated layers may contain direct PII for auditability/traceability.
    - Governance practice: never display raw identifiers in notebooks/slides.

    Implementation:
    - Mask common PII columns by keeping only the last 3 characters.
    - Uses Series.map() to avoid Pylance overload/type-check warnings.
    """
    preview = curated.head(n).copy()

    common_pii_cols = {
        "raw_applicant_full_name",
        "raw_applicant_email",
        "clean_email",
        "raw_applicant_ssn",
        "raw_applicant_ip_address",
        "raw_applicant_date_of_birth",
        "clean_date_of_birth",
    }

    def mask_value(v: Any) -> Any:
        if pd.isna(v):
            return pd.NA
        s = str(v)
        if len(s) <= 3:
            return "***"
        return "***" + s[-3:]

    for c in preview.columns:
        if c in common_pii_cols:
            preview[c] = preview[c].astype("object").map(mask_value)

    return preview


# =============================================================================
# Pseudonymisation demo (educational evidence)
# =============================================================================


def demo_pseudonymise(value: Any, salt: str) -> str | pd.NA:
    """
    Minimal deterministic pseudonymisation demo (SHA-256 + salt).

    Educational evidence only:
    - In production, the salt must come from a secrets manager (not hardcoded).
    - Analysts should not have access to the salt/mapping logic (separation of duties).
    """
    if pd.isna(value):
        return pd.NA
    text = str(value).strip().lower()
    if text == "":
        return pd.NA

    msg = (salt + text).encode("utf-8")
    return hashlib.sha256(msg).hexdigest()


# =============================================================================
# DQ report helpers (post-clean) — replaces the "deleted notebook" logic
# =============================================================================


def _require_columns(df: pd.DataFrame, required: list[str], context: str) -> None:
    """Raise a clear error if required columns are missing."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"{context}: missing required columns: {missing}")


def build_data_quality_report_postclean(
    applications_clean: pd.DataFrame,
    spending_clean: pd.DataFrame | None = None,
    *,
    application_id_col: str = "application_id",
    sample_n: int = 5,
) -> pd.DataFrame:
    """
    Build a post-clean data quality report as an "issue registry" style table.

    How it works:
    - Uses policy-as-code validators from src/schema.py:
        * validate_applications_postclean()
        * validate_spending_postclean() (optional)
    - Aggregates rule failures into counts and percentages per rule.
    - Joins rule metadata via build_rule_catalog() when available.

    Notes:
    - This function is designed to recreate data_quality_report_postclean.csv
      without relying on the notebook that originally produced it.
    - Duplicate/conflict classification is not implemented here (that belongs in quality.py);
      this focuses on schema-rule failures that appear in the post-clean report.
    """
    # Lazy import to avoid circular dependencies and keep notebook imports lightweight
    from .schema import (
        validate_applications_postclean,
        validate_spending_postclean,
        build_rule_catalog,
        APPLICATION_RULES,
        SPENDING_RULES,
    )

    # Run post-clean validation flags
    app_flags = validate_applications_postclean(applications_clean)
    app_n = len(app_flags)

    # Map flag_name -> rule_id (from schema.py catalogs)
    app_flag_to_rule = {k: v.rule_id for k, v in APPLICATION_RULES.items()}
    spn_flag_to_rule = {k: v.rule_id for k, v in SPENDING_RULES.items()}

    rows: list[dict[str, Any]] = []

    def add_rows_from_flags(
        flags: pd.DataFrame,
        n_rows: int,
        flag_to_rule: dict[str, str],
        entity: str,
        id_series: pd.Series | None,
    ) -> None:
        for flag_name in flags.columns:
            affected = int(flags[flag_name].astype(bool).sum())
            pct = (affected / n_rows * 100.0) if n_rows > 0 else 0.0

            rule_id = flag_to_rule.get(flag_name, flag_name)

            example_ids: list[Any] = []
            if affected > 0 and id_series is not None and application_id_col in applications_clean.columns:
                idx = flags.index[flags[flag_name].astype(bool)]
                example_ids = (
                    applications_clean.loc[idx, application_id_col]  # type: ignore[index]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()[:sample_n]
                )

            rows.append(
                {
                    "stage": "post",
                    "entity": entity,
                    "rule_id": rule_id,
                    "flag_name": flag_name,
                    "affected_count": affected,
                    "affected_pct": pct,
                    "n_rows": n_rows,
                    "example_ids": ", ".join(example_ids) if example_ids else pd.NA,
                }
            )

    id_series = applications_clean[application_id_col] if application_id_col in applications_clean.columns else None
    add_rows_from_flags(app_flags, app_n, app_flag_to_rule, "applications", id_series)

    # Optional spending validation
    if spending_clean is not None:
        sp_flags = validate_spending_postclean(spending_clean)
        sp_n = len(sp_flags)
        add_rows_from_flags(sp_flags, sp_n, spn_flag_to_rule, "spending", None)

    report = pd.DataFrame(rows)

    # Join rule metadata if possible (field_path, severity, description, value_source)
    try:
        catalog = build_rule_catalog()
        catalog_post = catalog.loc[catalog["stage"].astype(str).str.lower().eq("post")].copy()

        report = report.merge(
            catalog_post[
                [
                    "rule_id",
                    "issue_group",
                    "field_path",
                    "field_path_annotated",
                    "value_source",
                    "severity",
                    "description",
                ]
            ],
            how="left",
            on="rule_id",
        )
    except Exception:
        # If the catalog shape differs, keep the report usable anyway
        pass

    # Sort to make the report governance-friendly (high severity first if available)
    if "severity" in report.columns:
        sev_order = {"high": 0, "medium": 1, "low": 2}
        report["_sev_rank"] = report["severity"].astype(str).str.lower().map(sev_order).fillna(9)
        report = report.sort_values(["_sev_rank", "affected_count"], ascending=[True, False]).drop(columns=["_sev_rank"])
    else:
        report = report.sort_values(["affected_count"], ascending=False)

    return report.reset_index(drop=True)


def write_data_quality_report_postclean(
    *,
    applications_curated_full_path: Path,
    output_path: Path,
    spending_items_clean_path: Path | None = None,
    application_id_col: str = "application_id",
    sample_n: int = 5,
) -> Path:
    """
    Convenience wrapper to create and write data_quality_report_postclean.csv to disk.

    Typical usage (script or notebook):
        write_data_quality_report_postclean(
            applications_curated_full_path=Path("data/curated/applications_curated_full.csv"),
            output_path=Path("data/quality/reports/post/data_quality_report_postclean.csv"),
        )
    """
    apps = pd.read_csv(applications_curated_full_path)
    spending = pd.read_csv(spending_items_clean_path) if spending_items_clean_path else None

    report = build_data_quality_report_postclean(
        applications_clean=apps,
        spending_clean=spending,
        application_id_col=application_id_col,
        sample_n=sample_n,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(output_path, index=False)
    return output_path


# =============================================================================
# Targeted extractor used in the privacy notebook (auditability / provenance)
# =============================================================================


def extract_processing_timestamp_rows(dq_post: pd.DataFrame) -> pd.DataFrame:
    """
    Extract post-clean quality report rows related to missing processing_timestamp.

    Why this matters for governance:
    - Missing processing_timestamp is a provenance/audit-trail gap.
    - It is typically an upstream instrumentation/process deficiency.

    The function searches across common columns (rule_id, description, field_path).
    """
    masks: list[pd.Series] = []

    keys = ["R_APP_001", "flag_missing_processing_timestamp", "processing_timestamp"]
    for key in keys:
        if "rule_id" in dq_post.columns:
            masks.append(dq_post["rule_id"].astype(str).str.contains(key, case=False, na=False))
        if "description" in dq_post.columns:
            masks.append(dq_post["description"].astype(str).str.contains(key, case=False, na=False))
        if "field_path" in dq_post.columns:
            masks.append(dq_post["field_path"].astype(str).str.contains(key, case=False, na=False))

    if not masks:
        return dq_post.head(0)

    mask = masks[0]
    for m in masks[1:]:
        mask = mask | m

    return dq_post.loc[mask].copy()
