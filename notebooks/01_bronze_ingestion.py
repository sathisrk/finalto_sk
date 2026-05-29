# # 01 · Bronze — Raw Ingestion
# 
# Land the five Yelp JSON files into Delta, **1:1, no business logic**. Bronze is the immutable system-of-record: if a downstream bug is found, we re-derive silver/gold from bronze without re-reading source files.


import uuid
from pyspark.sql import functions as F

BATCH_ID = str(uuid.uuid4())  # one id per pipeline run; lets us audit / roll back a load
print(f"batch_id = {BATCH_ID}")


# Explicit source schemas
# `attributes` and `hours` on business are deeply/irregularly nested, so we land them as `MAP<STRING,STRING>` — robust to missing keys and parsed properly in silver.



SCHEMAS_DDL = {
    "business": """
        business_id STRING, name STRING, address STRING, city STRING, state STRING,
        postal_code STRING, latitude DOUBLE, longitude DOUBLE, stars DOUBLE,
        review_count LONG, is_open INT, attributes MAP<STRING,STRING>,
        categories STRING, hours MAP<STRING,STRING>
    """,
    "review": """
        review_id STRING, user_id STRING, business_id STRING, stars DOUBLE,
        useful LONG, funny LONG, cool LONG, text STRING, date STRING
    """,
    "user": """
        user_id STRING, name STRING, review_count LONG, yelping_since STRING,
        friends STRING, useful LONG, funny LONG, cool LONG, fans LONG, elite STRING,
        average_stars DOUBLE, compliment_hot LONG, compliment_more LONG,
        compliment_profile LONG, compliment_cute LONG, compliment_list LONG,
        compliment_note LONG, compliment_plain LONG, compliment_cool LONG,
        compliment_funny LONG, compliment_writer LONG, compliment_photos LONG
    """,
    "checkin": "business_id STRING, date STRING",
    "tip": """
        user_id STRING, business_id STRING, text STRING, date STRING,
        compliment_count LONG
    """,
}

# Reusable ingestion routine - One function, called per entity — keeps the loader DRY and consistent.

def ingest_to_bronze(entity: str) -> None:
    src_path = f"{LANDING_PATH}/{SOURCE_FILES[entity]}"
    target   = f"{SCHEMAS['bronze']}.{entity}"

    df = (
        spark.read
        .schema(SCHEMAS_DDL[entity])
        .json(src_path)                     # Yelp ships newline-delimited JSON (one obj/line)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id",    F.lit(BATCH_ID))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")                  # full snapshot reload; Auto Loader path appends
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    cnt = spark.table(target).count()
    print(f"  {entity:<9} -> {target}  ({cnt:,} rows)")


for entity in SOURCE_FILES:
    ingest_to_bronze(entity)

# Sanity checks - Cheap row counts + a peek. Heavier data-quality checks belong in silver.


for entity in SOURCE_FILES:
    spark.table(f"{SCHEMAS['bronze']}.{entity}").createOrReplaceTempView(f"bronze_{entity}")

display(spark.table(f"{SCHEMAS['bronze']}.business").limit(5))


# (Production) Incremental ingestion with Auto Loader
# or a live feed where Yelp drops new/updated files into the landing volume, swap the batch read above for Auto Loader. It tracks which files it has already seen, so each run only processes new arrivals — exactly-once, no manual bookkeeping.

# def autoload_to_bronze(entity: str):
#     target = f"{SCHEMAS['bronze']}.{entity}"
#     chk    = f"{LANDING_PATH}/_checkpoints/{entity}"
#     (spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .schema(SCHEMAS_DDL[entity])
#         .load(f"{LANDING_PATH}/{entity}/")
#         .withColumn("_source_file", F.col("_metadata.file_path"))
#         .withColumn("_ingested_at", F.current_timestamp())
#      .writeStream
#         .option("checkpointLocation", chk)
#         .trigger(availableNow=True)      # process backlog then stop; cheap to schedule
#         .toTable(target))

