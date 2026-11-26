# infra/ — planned, not wired into the pipeline

This folder holds a **draft** of a cloud version of the warehouse layer.
Nothing here is called by the Airflow DAGs; the running pipeline uses DuckDB.

| Path | What it is | Status |
|---|---|---|
| `snowflake/terraform/` | Terraform for a Snowflake database, schema, X-Small warehouse and the raw `FAKESHOP_TRANSACTIONS` table | Draft. Credentials come from a git-ignored `terraform.tfvars` |
| `snowflake/sql/` | DDL and a `COPY INTO` from an S3 external stage | Draft; the S3 stage it references is not defined anywhere yet |

Known gaps before this could replace DuckDB:

- No S3 bucket / storage integration / external stage in Terraform.
- No Airflow task uses the Snowflake provider.
- Table names differ between files (`transactions` in `create_table.sql`
  vs `FAKESHOP_TRANSACTIONS` in Terraform and `copy_into.sql`).
- Password authentication; key-pair auth would be preferred.
