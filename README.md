# Finalto Data Engineering Assessment

A medallion lakehouse on **Databricks + PySpark + Delta Lake** that ingests the [Yelp dataset](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset), models it for BI, and answers the "rising star" analytics question.

## How the assessment maps to this repo

**1.** Understand the sources / data model | [`docs/01_data_model.md`](docs/01_data_model.md) — ERD + entity grain |
**2a.** Ingestion (PySpark) + stack rationale | [`docs/02_architecture_and_stack.md`](docs/02_architecture_and_stack.md), [`notebooks/01_bronze_ingestion.py`](notebooks/01_bronze_ingestion.py) |
**2b.** Data modeling for BI + scenarios | [`docs/03_dimensional_model.md`](docs/03_dimensional_model.md), [`notebooks/03_gold_dimensional_model.py`](notebooks/03_gold_dimensional_model.py) |
**2c.** Data transformation (PySpark) | [`notebooks/02_silver_transformations.py`](notebooks/02_silver_transformations.py), [`notebooks/03_gold_dimensional_model.py`](notebooks/03_gold_dimensional_model.py) |
**2d.** Scalability, performance, monitoring | [`docs/04_scalability_performance.md`](docs/04_scalability_performance.md) |
**3.** "Rising star" SQL query | [`sql/rising_star_businesses.sql`](sql/rising_star_businesses.sql) |


## Repo layout


yelp-de/
  README.md
  docs/
    01_data_model.md                  # sources, ERD, relationships
    02_architecture_and_stack.md      # medallion design + why this stack
    03_dimensional_model.md           # star schema + 8 BI scenarios
    04_scalability_performance.md     # monitor / benchmark / scale
    05_databricks_free_edition_setup.md
  notebooks/                            # Databricks source format; import & run in order
    00_setup.py                       # catalog/schema/volume + shared config
    01_bronze_ingestion.py            # raw JSON -> Delta (+ Auto Loader variant)
    02_silver_transformations.py      # clean, type, dedupe, explode
    03_gold_dimensional_model.py      # star schema facts + dims
  sql/
      rising_star_businesses.sql
