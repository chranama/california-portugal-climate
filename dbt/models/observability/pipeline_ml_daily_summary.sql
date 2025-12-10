{{ config(
    materialized = 'table'
) }}

-- Daily summary of ML metrics (including minority-class / event=1 metrics)

select
    cast(created_at as date) as run_date,
    run_mode,
    count(*)                      as n_runs,
    max(created_at)              as last_run_at,
    max(train_size)              as last_n_train,
    avg(accuracy)                as avg_accuracy,
    avg(roc_auc)                 as avg_roc_auc,

    -- Minority-class (event = 1) metrics, averaged across runs
    avg(precision_1)             as avg_precision_pos,
    avg(recall_1)                as avg_recall_pos,
    avg(f1_1)                    as avg_f1_pos

from pipeline_ml_metrics
group by
    run_date,
    run_mode
order by
    run_date,
    run_mode