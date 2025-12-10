{{ config(
    materialized = 'view',
    alias = 'pipeline_ml_metrics_view'
) }}

-- Expose ML metrics logged from the training script.
-- This view just renames a couple of fields to nicer analytics names.

select
    created_at   as run_at,
    run_mode,
    train_size   as n_train,
    accuracy,
    roc_auc
from pipeline_ml_metrics
order by created_at desc