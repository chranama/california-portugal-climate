{{ config(
    materialized = 'table'
) }}

with base as (
    select
        date_trunc('day', started_at) as run_date,
        flow_name,
        run_mode,
        status,
        rows_bronze,
        rows_gold_ml,
        rows_bronze_delta,
        rows_gold_ml_delta
    from pipeline_run_log
),

agg as (
    select
        run_date,
        flow_name,
        run_mode,
        count(*) as runs_total,
        sum(case when status = 'success' then 1 else 0 end) as runs_success,
        sum(case when status <> 'success' then 1 else 0 end) as runs_failed,
        max(rows_bronze) as rows_bronze_max,
        max(rows_gold_ml) as rows_gold_ml_max,
        max(rows_bronze_delta) as rows_bronze_delta_max,
        max(rows_gold_ml_delta) as rows_gold_ml_delta_max
    from base
    group by
        run_date,
        flow_name,
        run_mode
)

select
    run_date,
    flow_name,
    run_mode,
    runs_total,
    runs_success,
    runs_failed,
    rows_bronze_max,
    rows_gold_ml_max,
    rows_bronze_delta_max,
    rows_gold_ml_delta_max
from agg
order by
    run_date desc,
    flow_name,
    run_mode