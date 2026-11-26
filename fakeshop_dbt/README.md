# fakeshop_dbt

dbt models built on top of the raw `transactions` table loaded by Airflow.

| Model | Grain | Used by |
|---|---|---|
| `dim_customers` | one row per `user_id` | dashboard (segments, KPIs) |
| `fct_daily_sales` | one row per (`transaction_date`, `category`) | dashboard (revenue charts) |

Tests: column tests in `models/*.yml` plus singular tests in `tests/`
(unique fact grain, revenue reconciliation between source and marts).

```bash
# from this folder; DUCKDB_PATH defaults to ../warehouse/fakeshop.duckdb
dbt build --profiles-dir .
```
