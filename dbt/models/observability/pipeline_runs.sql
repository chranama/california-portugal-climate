-- dbt model: main.pipeline_runs_model
-- Thin wrapper around the raw pipeline_runs table

select
    id,
    flow_name,
    run_mode,
    status,
    started_at,
    finished_at,
    rows_bronze,
    rows_gold_ml
from {{ source('main', 'pipeline_runs') }}