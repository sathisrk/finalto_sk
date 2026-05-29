# 02 · Silver — Clean & Conform

# Turn raw bronze into trustworthy, query-ready entities. This is where all the "tax" lives: type casting, date parsing, de-duplication, dropping orphan/invalid rows, and exploding the multi-valued fields Yelp crams into single columns.



# %run ./00_setup



from pyspark.sql import functions as F, DataFrame

B = SCHEMAS["bronze"]
S = SCHEMAS["silver"]

def write_silver(df: DataFrame, name: str, partition_by: str | None = None) -> None:
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        w = w.partitionBy(partition_by)
    w.saveAsTable(f"{S}.{name}")
    print(f"  wrote {S}.{name}  ({spark.table(f'{S}.{name}').count():,} rows)")

# business
# Trim text, keep nested maps for ad-hoc use, derive a clean category array. `business_id` is the PK — drop nulls and de-dupe defensively.



business = (
    spark.table(f"{B}.business")
    .where(F.col("business_id").isNotNull())
    .withColumn("name",  F.trim("name"))
    .withColumn("city",  F.trim("city"))
    .withColumn("state", F.trim("state"))
    # "Pizza, Italian, Restaurants" -> ["Pizza","Italian","Restaurants"]
    .withColumn(
        "category_array",
        F.when(
            F.col("categories").isNotNull(),
            F.expr("transform(split(categories, ','), x -> trim(x))"),
        ).otherwise(F.array()),
    )
    .dropDuplicates(["business_id"])
)
write_silver(business, "business")

# business_category (bridge)
# `categories` is multi-valued, which breaks any "reviews by category" report. Explode it into a proper bridge so categories can be joined and filtered relationally.



business_category = (
    spark.table(f"{S}.business")
    .select("business_id", F.explode("category_array").alias("category"))
    .where(F.length(F.trim("category")) > 0)
    .dropDuplicates(["business_id", "category"])
)
write_silver(business_category, "business_category")

# review
# Parse the timestamp to a real `timestamp`/`date`, constrain stars to 1–5, dedupe on `review_id`. Partitioned by `review_year` — almost every BI question and the rising-star query filter on time, so this gives us partition pruning for free.



review = (
    spark.table(f"{B}.review")
    .where(F.col("review_id").isNotNull() & F.col("business_id").isNotNull())
    .withColumn("review_ts",   F.to_timestamp("date"))
    .withColumn("review_date", F.to_date("review_ts"))
    .withColumn("review_year", F.year("review_ts"))
    .where(F.col("stars").between(1, 5))
    .dropDuplicates(["review_id"])
    .select(
        "review_id", "business_id", "user_id", "stars",
        "useful", "funny", "cool", "review_ts", "review_date", "review_year",
    )
)
write_silver(review, "review", partition_by="review_year")

# user
# Parse `yelping_since`, turn the comma-separated `elite` years and `friends` lists into structured columns (raw strings dropped — they're not analytically useful).



user = (
    spark.table(f"{B}.user")
    .where(F.col("user_id").isNotNull())
    .withColumn("yelping_since_date", F.to_date("yelping_since"))
    .withColumn(
        "elite_years",
        F.when(
            (F.col("elite").isNotNull()) & (F.length(F.trim("elite")) > 0),
            F.expr("transform(split(elite, ','), x -> trim(x))"),
        ).otherwise(F.array()),
    )
    .withColumn("is_elite", F.size("elite_years") > 0)
    .withColumn(
        "friend_count",
        F.when(
            (F.col("friends").isNotNull()) & (F.col("friends") != "None"),
            F.size(F.split("friends", ",")),
        ).otherwise(F.lit(0)),
    )
    .dropDuplicates(["user_id"])
    .drop("friends", "elite", "yelping_since")
)
write_silver(user, "user")

# checkin (exploded)
# Bronze stores all check-ins for a business as one giant comma-separated timestamp string. Explode to one row per event so it behaves like a normal fact source.



checkin = (
    spark.table(f"{B}.checkin")
    .where(F.col("business_id").isNotNull())
    .withColumn("checkin_ts_str", F.explode(F.split("date", ",")))
    .withColumn("checkin_ts", F.to_timestamp(F.trim("checkin_ts_str")))
    .where(F.col("checkin_ts").isNotNull())
    .withColumn("checkin_date", F.to_date("checkin_ts"))
    .select("business_id", "checkin_ts", "checkin_date")
)
write_silver(checkin, "checkin")

# tip



tip = (
    spark.table(f"{B}.tip")
    .where(F.col("business_id").isNotNull())
    .withColumn("tip_ts",   F.to_timestamp("date"))
    .withColumn("tip_date", F.to_date("tip_ts"))
    .select("user_id", "business_id", "text", "compliment_count", "tip_ts", "tip_date")
)
write_silver(tip, "tip")

# Lightweight data-quality 
# A compact audit so the BI team can trust what's in silver. In a scheduled job these become assertions (fail the run) or land in a DQ table for Lakehouse Monitoring.



checks = []
for name in ["business", "business_category", "review", "user", "checkin", "tip"]:
    df = spark.table(f"{S}.{name}")
    checks.append((name, df.count()))

# orphan check: reviews whose business_id has no matching business row
orphans = (
    spark.table(f"{S}.review").select("business_id").distinct()
    .join(spark.table(f"{S}.business").select("business_id"), "business_id", "left_anti")
    .count()
)

display(spark.createDataFrame(checks, ["table", "row_count"]))
print(f"reviews referencing an unknown business_id: {orphans:,}")
