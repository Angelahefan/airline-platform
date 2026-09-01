"""
Source-to-target row-count reconciliation.

Answers the question every data platform must answer: "did we lose or silently
drop rows between the source file and the analytics tables, and if the counts
differ, can we explain every row?"

For each source it reconciles:  source file  ->  raw  ->  staging (cleaned)
and classifies the deltas into (a) duplicates removed and (b) rows filtered by
data-quality rules (e.g. orphan airport codes). A run is RECONCILED when every
row is accounted for; it FAILS only on unexplained loss (a real breakage).

CHANGE LOG (this version):
    Previously, `filtered` was derived by subtraction:
        filtered = (raw - duplicates) - staging
    This meant the equation `raw == staging + duplicates + filtered` was
    ALWAYS true by construction -- it could never actually catch a real bug
    in the staging-layer filtering logic. This version replaces that with a
    real row-level FULL OUTER JOIN between raw and staging (keyed on the
    table's primary key), so we can directly observe which raw rows never
    made it to staging, instead of assuming they were "filtered" by default.

    python -m quality.reconciliation        # run after `dbt build`
"""
from __future__ import annotations

import json
import sys

from tabulate import tabulate

from config import settings, get_duckdb_connection
from ingestion.logging_config import get_logger

logger = get_logger("quality.reconcile")

REPORT_PATH = settings.data_raw_dir.parent.parent / "quality" / "expectations" / "reconciliation_report.json"

RECON_SPEC = [
    {"name": "flights", "file": "flights.csv", "raw": "raw.flights", "staging": "staging.stg_flights", "key": "flight_id"},
    {"name": "airports", "file": "airports.csv", "raw": "raw.airports", "staging": "staging.stg_airports", "key": "airport_code"},
    {"name": "carriers", "file": "carriers.csv", "raw": "raw.carriers", "staging": "staging.stg_carriers", "key": "carrier_code"},
    {"name": "weather", "file": "weather.csv", "raw": "raw.weather", "staging": "staging.stg_weather", "key": None},
]


def _file_rows(filename: str) -> int:
    path = settings.data_raw_dir / filename
    with open(path, "r", encoding="utf-8") as fh:
        return max(sum(1 for _ in fh) - 1, 0)  # minus header


def _bts_source_rows(con) -> int:
    """Independent source row count straight from the BTS monthly CSVs."""
    pattern = str(settings.bts_data_dir / "**" / "*.csv").replace("'", "''")
    return con.sql(
        f"SELECT count(*) FROM read_csv('{pattern}', header=true, "
        f"union_by_name=true, sample_size=200000, ignore_errors=true)"
    ).fetchone()[0]


def _source_rows(con, spec: dict) -> int:
    """File-level row count for a source, respecting the active DATA_SOURCE.

    In bts mode: flights come from the monthly files; airports/carriers are
    *derived* dimensions (source == raw by construction); weather is empty.
    """
    if settings.data_source != "bts":
        return _file_rows(spec["file"])
    if spec["name"] == "flights":
        return _bts_source_rows(con)
    if spec["name"] == "weather":
        return 0
    return _count(con, spec["raw"])  # derived dims: raw IS the source


def _table_exists(con, fqname: str) -> bool:
    schema, table = fqname.split(".")
    q = ("SELECT COUNT(*) FROM information_schema.tables "
         "WHERE table_schema = ? AND table_name = ?")
    return con.sql(q, params=[schema, table]).fetchone()[0] > 0


def _count(con, fqname: str) -> int:
    return con.sql(f"SELECT COUNT(*) FROM {fqname}").fetchone()[0]


def _distinct(con, fqname: str, key: str) -> int:
    return con.sql(f"SELECT COUNT(DISTINCT {key}) FROM {fqname}").fetchone()[0]


def _real_row_level_diff(con, raw_fqname: str, staging_fqname: str, key: str) -> dict:
    """Row-level FULL OUTER JOIN between raw and staging, keyed on `key`.

    This is the real reconciliation check: instead of inferring how many
    rows were "filtered" by subtraction, we directly observe, for every raw
    row, whether it exists in staging -- and vice versa. This mirrors the
    approach used by dbt's official audit-helper package (compare_relations),
    which classifies rows as in_a/in_b rather than assuming a match.

    Returns a dict with:
        matched:        rows present in both raw and staging
        raw_only:       rows in raw that never made it to staging
                         (the TRUE unexplained/filtered count -- not assumed)
        staging_only:   rows in staging with no raw counterpart
                         (should be 0 in a healthy pipeline; a non-zero value
                         here is a red flag, e.g. staging created rows out of
                         thin air, or a join fan-out bug upstream)
    """
    query = f"""
        select
            a.{key} is not null as in_raw,
            b.{key} is not null as in_staging,
            count(*) as row_count
        from {raw_fqname} a
        full outer join {staging_fqname} b
            on a.{key} = b.{key}
        group by 1, 2
    """
    rows = con.sql(query).fetchall()

    result = {"matched": 0, "raw_only": 0, "staging_only": 0}
    for in_raw, in_staging, row_count in rows:
        if in_raw and in_staging:
            result["matched"] = row_count
        elif in_raw and not in_staging:
            result["raw_only"] = row_count
        elif in_staging and not in_raw:
            result["staging_only"] = row_count
    return result


def run() -> int:
    con = get_duckdb_connection(read_only=True)
    rows, report, overall_ok = [], [], True
    try:
        for spec in RECON_SPEC:
            src = _source_rows(con, spec)
            raw = _count(con, spec["raw"])
            load_ok = src == raw

            duplicates = 0
            if spec["key"]:
                duplicates = raw - _distinct(con, spec["raw"], spec["key"])

            staging = raw_only = staging_only = matched = None
            reconciled = load_ok

            if _table_exists(con, spec["staging"]):
                staging = _count(con, spec["staging"])

                if spec["key"]:
                    # Real row-level check, replacing the old subtraction-based
                    # `filtered = (raw - duplicates) - staging` estimate.
                    diff = _real_row_level_diff(con, spec["raw"], spec["staging"], spec["key"])
                    matched = diff["matched"]
                    raw_only = diff["raw_only"]
                    staging_only = diff["staging_only"]

                    # A run is only reconciled if:
                    #   1) load from source -> raw was lossless, AND
                    #   2) nothing appeared in staging that isn't in raw
                    #      (staging_only should always be 0 in a healthy run)
                    #   3) every raw row is either matched, a known duplicate,
                    #      or genuinely absent from staging (raw_only) --
                    #      raw_only is reported as-is, NOT silently accepted;
                    #      see the "unexplained" flag below.
                    reconciled = load_ok and staging_only == 0
                else:
                    # No primary key available (e.g. weather) -- fall back to
                    # the simpler total-count check; row-level diffing isn't
                    # possible without a join key.
                    reconciled = load_ok and staging <= raw

            # `raw_only` rows are NOT automatically assumed to be "clean DQ
            # filtering" anymore. Flag them as unexplained unless duplicates
            # alone account for the full gap.
            unexplained = None
            if raw_only is not None:
                unexplained = max(raw_only - duplicates, 0)
                if unexplained > 0:
                    reconciled = False

            status = "RECONCILED" if (load_ok and reconciled) else "FAIL"
            if status == "FAIL":
                overall_ok = False

            rows.append([
                spec["name"], src, raw, "OK" if load_ok else "MISMATCH",
                duplicates, "-" if staging is None else staging,
                "-" if raw_only is None else raw_only,
                "-" if staging_only is None else staging_only,
                "-" if unexplained is None else unexplained,
                status,
            ])
            report.append({
                "source": spec["name"], "source_rows": src, "raw_rows": raw,
                "load_fidelity": load_ok, "duplicates_removed": duplicates,
                "staging_rows": staging,
                "matched_rows": matched,
                "raw_only_rows": raw_only,
                "staging_only_rows": staging_only,
                "unexplained_rows": unexplained,
                "status": status,
            })

        header = ["source", "file_rows", "raw_rows", "load", "dupes_removed",
                  "staging_rows", "raw_only", "staging_only", "unexplained", "status"]
        logger.info("source-to-target reconciliation:\n%s",
                    tabulate(rows, headers=header, tablefmt="github"))

        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PATH.write_text(json.dumps(
            {"overall": "PASS" if overall_ok else "FAIL", "detail": report}, indent=2, default=str))
        logger.info("report written -> %s", REPORT_PATH)

        if not overall_ok:
            logger.error("reconciliation FAILED: unexplained row loss or unexpected extra rows detected")
            return 1
        logger.info("reconciliation PASSED: every source row is accounted for")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(run())
