{{ config(
    materialized = 'view'
) }}

select
    id,
    flow_name,
    run_mode,
    status,
    started_at,
    finished_at,
    rows_bronze,
    rows_gold_ml,
    rows_bronze_delta,
    rows_gold_ml_delta,
    bronze_max_date,
    gold_ml_max_date,
    freshness_status
from pipeline_run_log
order by started_at desc