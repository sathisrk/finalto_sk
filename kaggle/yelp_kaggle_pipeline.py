"""
Finalto Data Engineering

Working through the assessment: ingest the Yelp dataset, model it for BI and answer the rising-star question.

Plan:
  1. Look at what's actually in the files before writing any schema
  2. Land it raw (bronze) with explicit schemas derived from step 1
  3. Clean & conform — one entity at a time, verify each before moving on
  4. Build a star schema (gold)
  5. Run the rising-star query
  6. A couple of BI sanity queries on the gold model
"""


# Installing Spark
# Kaggle's base image ships pyspark 4. I want 3.5 LTS — matches what Databricks Runtime

PYSPARK_VERSION = "3.5.3"
import subprocess, sys
from importlib.metadata import version as _pkg_version, PackageNotFoundError

try:
    _installed = _pkg_version("pyspark")
except PackageNotFoundError:
    _installed = None

if _installed != PYSPARK_VERSION:
    subprocess.run([sys.executable, "-m", "pip", "install", "-q",
                    f"pyspark=={PYSPARK_VERSION}"], check=True)
    if "pyspark" in sys.modules:
        raise RuntimeError(
            "pyspark was already imported at a different version — restart the kernel:\n"
            "  Kaggle ▸ Run ▸ Restart & Clear Outputs ▸ Run All"
        )

import pyspark
assert pyspark.__version__.startswith("3.5"), f"got {pyspark.__version__}"
print("pyspark", pyspark.__version__)



# Spark session

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("yelp-de")
    .master("local[*]")
    .config("spark.driver.memory", "16g")
    .config("spark.sql.shuffle.partitions", "64")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.adaptive.enabled", "true")    # AQE handles skew/coalescing at runtime
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("Spark", spark.version)



# 1. Locate the input files
# Five JSON files. List their sizes and declaring schemas.

import os, glob, json

# auto-locate the dataset — Kaggle's mount path varies based on how it was attached
hits = glob.glob("/kaggle/input/**/yelp_academic_dataset_business.json", recursive=True)
assert hits, "Yelp dataset not attached — Add Input on the right rail"
BASE = os.path.dirname(hits[0])

files = {
    "business": "yelp_academic_dataset_business.json",
    "review":   "yelp_academic_dataset_review.json",
    "user":     "yelp_academic_dataset_user.json",
    "checkin":  "yelp_academic_dataset_checkin.json",
    "tip":      "yelp_academic_dataset_tip.json",
}
paths = {k: os.path.join(BASE, v) for k, v in files.items()
         if os.path.exists(os.path.join(BASE, v))}

print(f"dataset at: {BASE}\n")
for k, p in paths.items():
    print(f"  {k:<9}  {os.path.getsize(p) / 1024**2:>8.1f} MB")



# Initial data exploration (kept as a record of how the schemas were derived
# commented out on re-runs because the output was reviewed already)

# def peek(path):
#     with open(path) as f:
#         return json.loads(f.readline())
#
# print("--- business ---")
# print(json.dumps(peek(paths["business"]), indent=2))
#
# print("--- review ---")
# r = peek(paths["review"])
# r["text"] = r["text"][:120] + "..."          
# print(json.dumps(r, indent=2))
#
# print("--- user ---")
# u = peek(paths["user"])
# u["friends"] = u["friends"][:120] + "..."    
# print(json.dumps(u, indent=2))
#
# print("--- checkin ---")
# c = peek(paths["checkin"])
# c["date"] = c["date"][:200] + "..."
# print(json.dumps(c, indent=2))
#
# print("--- tip ---")
# print(json.dumps(peek(paths["tip"]), indent=2))

# 2. Bronze — raw load with explicit schemas

# Schema inference on multi-GB JSON is slow and drifty. The DDL below was written directly off the field lists observed in the peek above.

SCHEMAS_DDL = {
    "business": """business_id STRING, name STRING, address STRING, city STRING, state STRING,
        postal_code STRING, latitude DOUBLE, longitude DOUBLE, stars DOUBLE, review_count LONG,
        is_open INT, attributes MAP<STRING,STRING>, categories STRING, hours MAP<STRING,STRING>""",
    "review": """review_id STRING, user_id STRING, business_id STRING, stars DOUBLE,
        useful LONG, funny LONG, cool LONG, text STRING, date STRING""",
    "user": """user_id STRING, name STRING, review_count LONG, yelping_since STRING,
        friends STRING, useful LONG, funny LONG, cool LONG, fans LONG, elite STRING,
        average_stars DOUBLE, compliment_hot LONG, compliment_more LONG, compliment_profile LONG,
        compliment_cute LONG, compliment_list LONG, compliment_note LONG, compliment_plain LONG,
        compliment_cool LONG, compliment_funny LONG, compliment_writer LONG, compliment_photos LONG""",
    "checkin": "business_id STRING, date STRING",
    "tip": "user_id STRING, business_id STRING, text STRING, date STRING, compliment_count LONG",
}

bronze = {k: spark.read.schema(SCHEMAS_DDL[k]).json(p) for k, p in paths.items()}
print("loaded:", list(bronze))

# Row counts — confirms each file parsed under the declared schema (a wrong schema would show up as nulls in the PK columns, not as a parse error).
for k, df in bronze.items():
    print(f"  {k:<9}  {df.count():>10,} rows")

# Quick look at a typed business row (attributes/hours as maps as intended).
bronze["business"].show(3, truncate=80)



# 3. Silver — clean & conform

# One entity at a time. After each, a quick .show() or sanity stat to verify the transform did what I expected before moving on.

silver = {}


# 3.1 business

# Trim text, derive category_array from the comma string (used both in dim_business and to build the bridge), dedupe on business_id.

silver["business"] = (
    bronze["business"]
    .where(F.col("business_id").isNotNull())
    .withColumn("name",  F.trim("name"))
    .withColumn("city",  F.trim("city"))
    .withColumn("state", F.trim("state"))
    .withColumn("category_array",
        F.when(F.col("categories").isNotNull(),
               F.expr("transform(split(categories, ','), x -> trim(x))"))
         .otherwise(F.array()))
    .dropDuplicates(["business_id"])
)
silver["business"].createOrReplaceTempView("business")

silver["business"].select("business_id", "name", "city", "state", "category_array").show(5, truncate=False)


# 3.2 business_category (bridge)

# Explode the category array. Now "businesses by category" is a clean join instead string-parsing every query.

silver["business_category"] = (
    silver["business"]
    .select("business_id", F.explode("category_array").alias("category"))
    .where(F.length(F.trim("category")) > 0)
    .dropDuplicates(["business_id", "category"])
)
silver["business_category"].createOrReplaceTempView("business_category")

# Sanity check: 
spark.sql("""
    SELECT category, COUNT(*) AS n_businesses
    FROM business_category
    GROUP BY category
    ORDER BY n_businesses DESC
    LIMIT 10
""").show(truncate=False)


# 3.3 review

# Parse the date, derive review_year (we'll partition by it later — virtually every BI question filters by time). Stars between 1 and 5 as a sanity guard.

silver["review"] = (
    bronze["review"]
    .where(F.col("review_id").isNotNull() & F.col("business_id").isNotNull())
    .withColumn("review_ts",   F.to_timestamp("date"))
    .withColumn("review_date", F.to_date("review_ts"))
    .withColumn("review_year", F.year("review_ts"))
    .where(F.col("stars").between(1, 5))
    .dropDuplicates(["review_id"])
    .select("review_id", "business_id", "user_id", "stars",
            "useful", "funny", "cool", "review_ts", "review_date", "review_year")
)
silver["review"].createOrReplaceTempView("review")

silver["review"].select("review_id", "business_id", "stars", "review_date").show(5, truncate=False)

# Date span — matters for the rising-star query. We anchor "past year" to max_date because the dataset is a static historical snapshot, not live.
silver["review"].agg(
    F.min("review_date").alias("min_date"),
    F.max("review_date").alias("max_date"),
).show()


# 3.4 user

# `friends` is a huge comma-separated list of IDs — keep a count, drop the raw string.
# `elite` becomes an array of years, plus a boolean for easy filtering.

silver["user"] = (
    bronze["user"]
    .where(F.col("user_id").isNotNull())
    .withColumn("yelping_since_date", F.to_date("yelping_since"))
    .withColumn("elite_years",
        F.when((F.col("elite").isNotNull()) & (F.length(F.trim("elite")) > 0),
               F.expr("transform(split(elite, ','), x -> trim(x))"))
         .otherwise(F.array()))
    .withColumn("is_elite", F.size("elite_years") > 0)
    .withColumn("friend_count",
        F.when((F.col("friends").isNotNull()) & (F.col("friends") != "None"),
               F.size(F.split("friends", ","))).otherwise(F.lit(0)))
    .dropDuplicates(["user_id"])
    .drop("friends", "elite", "yelping_since")
)
silver["user"].createOrReplaceTempView("user")

silver["user"].select(
    "user_id", "name", "yelping_since_date", "friend_count",
    "is_elite", "elite_years", "fans"
).show(5, truncate=False)


# 3.5 checkin — the explode

# From the peek: one row per business, all timestamps packed into a single string. Blow it out to one row per individual event.

silver["checkin"] = (
    bronze["checkin"]
    .where(F.col("business_id").isNotNull())
    .withColumn("checkin_ts_str", F.explode(F.split("date", ",")))   # explode in its own step
    .withColumn("checkin_ts", F.to_timestamp(F.trim("checkin_ts_str")))
    .where(F.col("checkin_ts").isNotNull())
    .withColumn("checkin_date", F.to_date("checkin_ts"))
    .select("business_id", "checkin_ts", "checkin_date")
)
silver["checkin"].createOrReplaceTempView("checkin")

# verify: silver should have many more rows than bronze (1 row in bronze = N events)
print(f"bronze checkin (business rows): {bronze['checkin'].count():>12,}")
print(f"silver checkin (event rows)   : {silver['checkin'].count():>12,}")
silver["checkin"].show(5)


# 3.6 tip

# Easy one — parse the date, that's it.

silver["tip"] = (
    bronze["tip"]
    .where(F.col("business_id").isNotNull())
    .withColumn("tip_ts",   F.to_timestamp("date"))
    .withColumn("tip_date", F.to_date("tip_ts"))
    .select("user_id", "business_id", "text", "compliment_count", "tip_ts", "tip_date")
)
silver["tip"].createOrReplaceTempView("tip")
silver["tip"].show(5, truncate=80)


# Silver row-count recap
for k, df in silver.items():
    print(f"  {k:<18}  {df.count():>12,}")



# 4. Gold — star schema

# Facts in the middle (fact_review, fact_checkin, fact_tip), conformed dimensions around them (dim_business, dim_user, dim_date) plus the category bridge.

# 4.1 dim_date

# Generated from the actual data span across reviews/tips/check-ins. A real date dimension turns month-over-month and weekday/weekend reporting from a chore into a one-line join.

span = (
    silver["review"].select(F.col("review_date").alias("d"))
    .union(silver["tip"].select(F.col("tip_date").alias("d")))
    .union(silver["checkin"].select(F.col("checkin_date").alias("d")))
    .agg(F.min("d").alias("min_d"), F.max("d").alias("max_d"))
    .first()
)
print(f"date span: {span.min_d}  ->  {span.max_d}")

dim_date = (
    spark.sql(
        f"SELECT explode(sequence(to_date('{span.min_d}'), to_date('{span.max_d}'),"
        f" interval 1 day)) AS date"
    )
    .withColumn("date_key",     F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year",         F.year("date"))
    .withColumn("quarter",      F.quarter("date"))
    .withColumn("month",        F.month("date"))
    .withColumn("month_name",   F.date_format("date", "MMMM"))
    .withColumn("day_of_week",  F.date_format("date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("is_weekend",   F.dayofweek("date").isin(1, 7))
)
dim_date.createOrReplaceTempView("dim_date")
dim_date.show(5)


# 4.2 dim_business, dim_user, dim_category, bridge

dim_business = silver["business"].select(
    "business_id", "name", "address", "city", "state", "postal_code",
    "latitude", "longitude",
    F.col("stars").alias("avg_stars_alltime"),
    F.col("review_count").alias("review_count_alltime"),
    "is_open", "category_array",
)
dim_business.createOrReplaceTempView("dim_business")

dim_user = silver["user"].select(
    "user_id", "name", "yelping_since_date",
    F.col("review_count").alias("review_count_alltime"),
    F.col("average_stars").alias("avg_stars_given"),
    "fans", "friend_count", "is_elite", "elite_years",
)
dim_user.createOrReplaceTempView("dim_user")

dim_category = silver["business_category"].select("category").distinct()
dim_category.createOrReplaceTempView("dim_category")

# bridge — same data as silver business_category, exposed under the gold name
silver["business_category"].createOrReplaceTempView("bridge_business_category")

print(f"dim_business: {dim_business.count():>10,}")
print(f"dim_user    : {dim_user.count():>10,}")
print(f"dim_category: {dim_category.count():>10,}")


# 4.3 fact_review (central fact)

fact_review = silver["review"].select(
    "review_id", "business_id", "user_id",
    F.date_format("review_date", "yyyyMMdd").cast("int").alias("date_key"),
    "review_date", "review_year",
    "stars", "useful", "funny", "cool",
)
fact_review.createOrReplaceTempView("fact_review")
print(f"fact_review: {fact_review.count():>12,}")
fact_review.show(3)


# 4.4 fact_checkin (pre-aggregated to business × day)

# No dashboard wants 13M individual event rows at row level — daily counts are what BI needs.

fact_checkin = (
    silver["checkin"]
    .groupBy("business_id", "checkin_date")
    .agg(F.count("*").alias("checkin_count"))
    .withColumn("date_key", F.date_format("checkin_date", "yyyyMMdd").cast("int"))
    .select("business_id", "date_key", "checkin_date", "checkin_count")
)
fact_checkin.createOrReplaceTempView("fact_checkin")
print(f"fact_checkin: {fact_checkin.count():>12,}")


# 4.5 fact_tip

fact_tip = (
    silver["tip"]
    .withColumn("date_key", F.date_format("tip_date", "yyyyMMdd").cast("int"))
    .select("business_id", "user_id", "date_key", "tip_date", "compliment_count")
)
fact_tip.createOrReplaceTempView("fact_tip")
print(f"fact_tip: {fact_tip.count():>12,}")



# 5. The rising-star query

# Criteria:
#   - >= 10 reviews in the past year
#   - past-year avg rating >= 1 star higher than the prior period's avg


rising = spark.sql("""
    WITH bounds AS (
        SELECT date_sub(MAX(review_date), 365) AS cutoff FROM fact_review
    ),
    m AS (
        SELECT business_id,
               COUNT(CASE WHEN review_date >= cutoff THEN 1 END)     AS reviews_past_year,
               AVG(  CASE WHEN review_date >= cutoff THEN stars END) AS avg_past_year,
               AVG(  CASE WHEN review_date <  cutoff THEN stars END) AS avg_prior
        FROM fact_review CROSS JOIN bounds
        GROUP BY business_id
    )
    SELECT m.business_id,
           b.name,
           m.reviews_past_year,
           ROUND(m.avg_past_year, 2)                 AS avg_rating_past_year,
           ROUND(m.avg_prior,     2)                 AS avg_rating_prior,
           ROUND(m.avg_past_year - m.avg_prior, 2)   AS rating_improvement
    FROM m
    JOIN dim_business b USING (business_id)
    WHERE m.reviews_past_year >= 10
      AND m.avg_prior IS NOT NULL
      AND m.avg_past_year >= m.avg_prior + 1.0
    ORDER BY rating_improvement DESC, reviews_past_year DESC
""")
print(f"Rising-star businesses found: {rising.count():,}\n")
rising.show(25, truncate=False)



# 6. BI sanity queries

print("=== Top 10 categories by # businesses ===")
spark.sql("""
    SELECT category, COUNT(*) AS n
    FROM bridge_business_category
    GROUP BY category
    ORDER BY n DESC
    LIMIT 10
""").show(truncate=False)

print("=== Review volume & avg rating by year ===")
spark.sql("""
    SELECT review_year, COUNT(*) AS reviews, ROUND(AVG(stars), 3) AS avg_stars
    FROM fact_review
    GROUP BY review_year
    ORDER BY review_year
""").show(50)



# 7. Persist gold to Parquet


WRITE_GOLD = True
OUT = "/kaggle/working/gold"
if WRITE_GOLD:
    dim_date.write.mode("overwrite").parquet(f"{OUT}/dim_date")
    dim_business.write.mode("overwrite").parquet(f"{OUT}/dim_business")
    dim_user.write.mode("overwrite").parquet(f"{OUT}/dim_user")
    silver["business_category"].write.mode("overwrite").parquet(f"{OUT}/bridge_business_category")
    fact_review.write.mode("overwrite").partitionBy("review_year").parquet(f"{OUT}/fact_review")
    fact_checkin.write.mode("overwrite").parquet(f"{OUT}/fact_checkin")
    fact_tip.write.mode("overwrite").parquet(f"{OUT}/fact_tip")
    rising.write.mode("overwrite").parquet(f"{OUT}/rising_star_businesses")
    print("gold written to", OUT)

spark.stop()
