"""
Kaggle source adapter: public "Airline Delay and Cancellation" style dataset.

Replaces the synthetic generator / BTS adapter when DATA_SOURCE=kaggle. Reads
a Kaggle-style flights CSV (or set of yearly CSVs) and lands it in the
warehouse `raw` schema with the SAME shape the rest of the platform expects,
so staging, dbt, GE and the dashboards run unchanged.

WHY A SEPARATE ADAPTER, NOT A BRANCH INSIDE bts_adapter.py:
    The Kaggle schema differs from BTS in ways that matter downstream:
      * No tail_number  -> flight_id must be hashed from ONE FEWER field
        than the BTS natural key. Using the same 6-field hash as BTS would
        silently produce a constant/garbage tail_number component.
      * No origin/dest CITY or STATE columns -> raw.airports cannot be
        enriched with city/state derived from the source data (BTS could).
        Airport city/state here comes ENTIRELY from the curated reference
        table; airports missing from that reference get an empty city/state,
        not a guessed one -- this is called out explicitly so downstream
        consumers know the difference between "known" and "unknown" airports
        rather than silently defaulting both cases to the same-looking value.

Design decisions (mirrors bts_adapter.py where the schema allows):
  * Loaded directly via DuckDB `read_csv` (same single-scan pattern as BTS)
    since Kaggle exports of this dataset commonly run into the millions of
    rows across multiple yearly files.
  * `dim_airport` / `dim_airline` are DERIVED FROM THE DATA (distinct
    origins/dests, distinct carriers) and enriched from the curated
    reference, same approach as BTS, for the same reason: avoids a fixed
    whitelist silently dropping real flights.
  * `flight_id` is synthesized as a 64-bit hash of a 5-field natural key
    (FL_DATE, OP_CARRIER, OP_CARRIER_FL_NUM, ORIGIN, CRS_DEP_TIME) --
    intentionally ONE FEWER field than the BTS 6-field key, because this
    source has no tail number to include. This means a Kaggle flight_id and
    a BTS flight_id for what is conceptually "the same flight" will NOT
    match -- the two sources are not natural-key-compatible, and any future
    cross-source reconciliation must join on (flight_date, carrier_code,
    flight_number, origin) instead of flight_id.
  * Weather has no equivalent in this dataset either: an empty
    weather.csv-shaped table keeps the contract, same as BTS.

Usage:
    DATA_SOURCE=kaggle python -m ingestion.run_ingestion
"""
from __future__ import annotations

from datetime import datetime

from config import settings
from data.reference import AIRPORTS, CARRIERS
from ingestion.load import AUDIT_TABLE, ensure_audit_table, new_batch_id
from ingestion.logging_config import get_logger

logger = get_logger("ingestion.kaggle")

# Core columns this adapter requires (extra Kaggle columns are ignored).
# NOTE: no tail-number / city / state columns exist in this source -- see
# the module docstring for how that changes flight_id and raw.airports.
REQUIRED_COLUMNS = [
    "FL_DATE", "OP_CARRIER", "OP_CARRIER_FL_NUM",
    "ORIGIN", "DEST", "CRS_DEP_TIME", "DISTANCE", "CANCELLED", "DIVERTED",
]


def kaggle_files() -> list:
    """All Kaggle csvs under the data dir (dataset ships as one file per
    year -- no need to move files around)."""
    return sorted(settings.kaggle_data_dir.rglob("*.csv"))


def _glob_pattern() -> str:
    return str(settings.kaggle_data_dir / "**" / "*.csv").replace("'", "''")


def _rel(with_filename: bool = False) -> str:
    """read_csv relation over every Kaggle csv under the data dir."""
    fn = ", filename=true" if with_filename else ""
    return (
        f"read_csv('{_glob_pattern()}', header=true, union_by_name=true, "
        f"sample_size=200000, ignore_errors=true{fn})"
    )


# Columns pulled out of the raw Kaggle extract into the working set.
_SELECT_COLS = """
    FL_DATE, OP_CARRIER, OP_CARRIER_FL_NUM,
    ORIGIN, DEST, CRS_DEP_TIME, DEP_DELAY, ARR_DELAY,
    CANCELLED, DIVERTED, DISTANCE
"""


def materialize_source(con) -> int:
    """Scan the CSVs ONCE into a temp working table (needed columns only).

    Same single-scan pattern as bts_adapter.materialize_source: every
    downstream query (flights load, airport/carrier derivation, counts)
    hits this temp table instead of re-parsing CSVs.
    """
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _kaggle_src AS
        SELECT {_SELECT_COLS},
               'kaggle/' || regexp_extract(filename, '([^/\\\\]+)$', 1) AS _src_file
        FROM {_rel(with_filename=True)}
    """)
    return con.execute("SELECT count(*) FROM _kaggle_src").fetchone()[0]


def validate_schema(con) -> list:
    """Return the list of required columns missing from the source files."""
    cols = [r[0] for r in con.execute(
        f"SELECT column_name FROM (DESCRIBE SELECT * FROM {_rel()} LIMIT 1)"
    ).fetchall()]
    return [c for c in REQUIRED_COLUMNS if c not in cols]


def load_kaggle(con) -> dict:
    """Land raw.flights / raw.airports / raw.carriers / raw.weather from Kaggle."""
    batch_id = new_batch_id()
    ensure_audit_table(con)

    missing = validate_schema(con)
    if missing:
        raise ValueError(f"Kaggle files are missing required columns: {missing}")

    src_rows = materialize_source(con)
    logger.info("Kaggle source rows across %d files: %s (single-scan materialized)",
                len(kaggle_files()), f"{src_rows:,}")

    # ---------------- flights (direct DuckDB load) ----------------
    # NOTE: 5-field hash (no tail_number available) -- see module docstring.
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.flights AS
        SELECT
            (hash(
                coalesce(FL_DATE::VARCHAR,'') || '|' ||
                coalesce(OP_CARRIER,'')       || '|' ||
                coalesce(OP_CARRIER_FL_NUM::VARCHAR,'') || '|' ||
                coalesce(ORIGIN,'') || '|' ||
                coalesce(CRS_DEP_TIME::VARCHAR,'')
            ) >> 1)::BIGINT                                  AS flight_id,
            FL_DATE::DATE                                    AS flight_date,
            OP_CARRIER                                       AS carrier_code,
            OP_CARRIER_FL_NUM::INTEGER                       AS flight_number,
            NULL::VARCHAR                                    AS tail_number,  -- not in this source
            ORIGIN                                           AS origin,
            DEST                                              AS dest,
            (floor(CRS_DEP_TIME::INTEGER / 100) * 60
             + CRS_DEP_TIME::INTEGER % 100)::INTEGER         AS scheduled_departure_min,
            DISTANCE::DOUBLE                                 AS distance_miles,
            NULL::INTEGER                                     AS scheduled_air_time_min,  -- not in this source
            DEP_DELAY::INTEGER                               AS dep_delay_min,
            ARR_DELAY::INTEGER                               AS arr_delay_min,
            CANCELLED::DOUBLE::INTEGER                       AS cancelled,
            ''                                                AS cancellation_code,  -- not in this source
            DIVERTED::DOUBLE::INTEGER                        AS diverted,
            0                                                 AS carrier_delay_min,   -- delay-cause
            0                                                 AS weather_delay_min,   -- breakdown not
            0                                                 AS nas_delay_min,       -- in this source
            0                                                 AS security_delay_min,
            0                                                 AS late_aircraft_delay_min,
            CURRENT_TIMESTAMP                                AS _loaded_at,
            _src_file                                        AS _source_file,
            '{batch_id}'                                     AS _batch_id
        FROM _kaggle_src
    """)
    n_flights = con.execute("SELECT count(*) FROM raw.flights").fetchone()[0]

    # -------- airports: derived from the data, enriched from the reference ----
    # NOTE: unlike BTS, this source has no city/state columns, so city/state
    # can ONLY come from the reference table. Airports missing from the
    # reference get an EMPTY city/state (not a guess) so downstream
    # consumers can tell "known" from "unknown" airports explicitly.
    con.execute("CREATE OR REPLACE TEMP TABLE _airport_ref (code VARCHAR, name VARCHAR, "
                "lat DOUBLE, lon DOUBLE, tz INTEGER, hub INTEGER)")
    con.executemany(
        "INSERT INTO _airport_ref VALUES (?,?,?,?,?,?)",
        [[a[0], a[1], a[4], a[5], a[6], a[7]] for a in AIRPORTS],
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.airports AS
        WITH seen AS (
            SELECT ORIGIN AS code FROM _kaggle_src GROUP BY 1
            UNION
            SELECT DEST AS code FROM _kaggle_src GROUP BY 1
        )
        SELECT
            s.code                                       AS airport_code,
            coalesce(r.name, s.code || ' Airport')        AS airport_name,
            ''                                             AS city,   -- not derivable from this source
            ''                                             AS state,  -- not derivable from this source
            r.lat  AS latitude,
            r.lon  AS longitude,
            coalesce(r.tz, -6)  AS tz_offset,
            coalesce(r.hub, 1)  AS hub_weight,
            CURRENT_TIMESTAMP AS _loaded_at, 'kaggle:derived' AS _source_file,
            '{batch_id}' AS _batch_id
        FROM seen s LEFT JOIN _airport_ref r ON s.code = r.code
    """)
    n_airports = con.execute("SELECT count(*) FROM raw.airports").fetchone()[0]

    # -------- carriers: derived + enrichment (same pattern as BTS) ----------
    con.execute("CREATE OR REPLACE TEMP TABLE _carrier_ref (code VARCHAR, name VARCHAR, "
                "lc BOOLEAN, fleet INTEGER, founded INTEGER)")
    con.executemany(
        "INSERT INTO _carrier_ref VALUES (?,?,?,?,?)",
        [[c[0], c[1], c[2], c[3], c[4]] for c in CARRIERS],
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.carriers AS
        SELECT
            s.code                                 AS carrier_code,
            coalesce(r.name, 'Carrier ' || s.code) AS carrier_name,
            coalesce(r.lc, false)                  AS is_low_cost,
            coalesce(r.fleet, 0)                   AS fleet_size,
            coalesce(r.founded, 0)                 AS founded_year,
            CURRENT_TIMESTAMP AS _loaded_at, 'kaggle:derived' AS _source_file,
            '{batch_id}' AS _batch_id
        FROM (SELECT DISTINCT OP_CARRIER AS code FROM _kaggle_src) s
        LEFT JOIN _carrier_ref r ON s.code = r.code
    """)
    n_carriers = con.execute("SELECT count(*) FROM raw.carriers").fetchone()[0]

    # -------- weather: no equivalent in this dataset -> empty contract table ----------
    con.execute("""
        CREATE OR REPLACE TABLE raw.weather (
            weather_date DATE, airport_code VARCHAR, temperature_f DOUBLE,
            precipitation_in DOUBLE, wind_speed_mph DOUBLE, visibility_mi DOUBLE,
            conditions VARCHAR, is_severe INTEGER,
            _loaded_at TIMESTAMP, _source_file VARCHAR, _batch_id VARCHAR
        )
    """)

    # -------- audit rows ----------
    now = datetime.utcnow()
    for table, extracted, loaded in [
        ("raw.flights", src_rows, n_flights),
        ("raw.airports", n_airports, n_airports),
        ("raw.carriers", n_carriers, n_carriers),
        ("raw.weather", 0, 0),
    ]:
        con.execute(f"INSERT INTO {AUDIT_TABLE} VALUES (?,?,?,?,?,?,?)",
                    [batch_id, "kaggle_csvs", table, extracted, loaded, now,
                     "OK" if extracted == loaded else "ROW_COUNT_MISMATCH"])

    logger.info("Kaggle load complete: flights=%s airports=%s carriers=%s",
                f"{n_flights:,}", n_airports, n_carriers)
    return {"source_rows": src_rows, "flights": n_flights,
            "airports": n_airports, "carriers": n_carriers, "batch_id": batch_id}