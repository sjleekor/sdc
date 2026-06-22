"""Research / model-ETL package (analysis-only, not bundled into the prod image).

This package reads the exporter's Parquet lake (``data_lake/``) and never touches
PostgreSQL or ``krx_collector``. See ``docs/target/00_shared_etl_platform.md`` §6
and ``docs/target/01_20_access_return_rank/etl_03_implementation_plan.md``.
"""
