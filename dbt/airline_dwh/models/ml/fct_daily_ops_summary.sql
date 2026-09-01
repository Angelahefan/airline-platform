-- fct_daily_ops_summary: one row per flight_date
-- INCREMENTAL: on scheduled runs only new flight_dates are processed instead
-- with a {{ var('lookback_days', 3) }}-day lookback window, so late-arriving
-- or corrected rows inside the window are replaced idempotently (no dupes).
-- `dbt build --full-refresh` rebuilds from scratch.




{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='flight_date'
) }}

with daily_agg as (
    

    select
        flight_date,
        count(*) as total_flights,
        sum(
            case
                when dep_delay_min >= 15
                  or arr_delay_min >= 15
                then 1
                else 0
            end
        ) as delayed_flights,
        avg(dep_delay_min + arr_delay_min) as avg_delay_min

    from {{ ref('fact_flights') }}

    {% if is_incremental() %}
    where flight_date >= (
        select max(flight_date) - interval '3 days'
        from {{ this }}
    )
    {% endif %}

    group by flight_date
)

select
    flight_date,
    total_flights,
    delayed_flights,
    delayed_flights * 1.0 / nullif(total_flights, 0) as delay_rate,
    avg_delay_min

from daily_agg
  

 