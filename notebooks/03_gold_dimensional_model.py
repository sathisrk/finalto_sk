# 03 · Gold — Dimensional Model (Star Schema)
# Reshape conformed silver entities into a **star schema** the BI team queries directly. Facts hold the measures and foreign keys; dimensions hold descriptive attributes. This is what dashboards, Genie, and the SQL editor sit on top of.

# **Dimensions**
# - `dim_date`      — full calendar, generated from the data's date span
# - `dim_business`  — business attributes + location (conformed)
# - `dim_user`      — reviewer attributes + engagement flags
# - `dim_category`  — distinct categories
# - `bridge_business_category` — resolves the business↔category many-to-many

# **Facts**
# - `fact_review`   — grain: one review (the central fact)
# - `fact_checkin`  — grain: one (business, date) with a check-in count
# - `fact_tip`      — grain: one tip



from pyspark.sql import functions as F

S = SCHEMAS["silver"]
G = SCHEMAS["gold"]

def write_gold(df, name, partition_by=None, zorder=None):
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        w = w.partitionBy(partition_by)
    w.saveAsTable(f"{G}.{name}")
    if zorder:
        spark.sql(f"OPTIMIZE {G}.{name} ZORDER BY ({zorder})")
    print(f"  wrote {G}.{name}  ({spark.table(f'{G}.{name}').count():,} rows)")

# dim_date
# Generated to span every event date we hold (reviews, tips, check-ins). A real date dimension is what makes "month-over-month", "weekday vs weekend", and fiscal-period reporting trivial instead of a pile of ad-hoc `date_format` calls.



span = (
    spark.table(f"{S}.review").select(F.col("review_date").alias("d"))
    .union(spark.table(f"{S}.tip").select(F.col("tip_date").alias("d")))
    .union(spark.table(f"{S}.checkin").select(F.col("checkin_date").alias("d")))
    .agg(F.min("d").alias("min_d"), F.max("d").alias("max_d"))
    .first()
)

dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{span.min_d}'), to_date('{span.max_d}'), interval 1 day)) AS date")
    .withColumn("date_key",     F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year",         F.year("date"))
    .withColumn("quarter",      F.quarter("date"))
    .withColumn("month",        F.month("date"))
    .withColumn("month_name",   F.date_format("date", "MMMM"))
    .withColumn("day",          F.dayofmonth("date"))
    .withColumn("day_of_week",  F.date_format("date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("is_weekend",   F.dayofweek("date").isin(1, 7))
)
write_gold(dim_date, "dim_date")

# dim_business
# Business + location attributes in one wide, denormalised dimension (star-schema dimensions are intentionally denormalised for read performance).



dim_business = (
    spark.table(f"{S}.business")
    .select(
        "business_id", "name", "address", "city", "state", "postal_code",
        "latitude", "longitude",
        F.col("stars").alias("avg_stars_alltime"),
        F.col("review_count").alias("review_count_alltime"),
        "is_open", "category_array",
    )
)
write_gold(dim_business, "dim_business", zorder="city,state")

# dim_user



dim_user = (
    spark.table(f"{S}.user")
    .select(
        "user_id", "name", "yelping_since_date",
        F.col("review_count").alias("review_count_alltime"),
        F.col("average_stars").alias("avg_stars_given"),
        "fans", "friend_count", "is_elite", "elite_years",
    )
)
write_gold(dim_user, "dim_user")

# dim_category + bridge



dim_category = (
    spark.table(f"{S}.business_category")
    .select("category").distinct()
    .withColumn("category_key", F.sha2(F.col("category"), 256).substr(1, 12))
)
write_gold(dim_category, "dim_category")

bridge = spark.table(f"{S}.business_category").select("business_id", "category")
write_gold(bridge, "bridge_business_category")

# fact_review  (central fact)
# Grain = one review. FKs to business, user, and date. Partitioned by `review_year` and Z-ordered on `business_id` because the heaviest reports (incl. rising-star) filter by time then group by business.



fact_review = (
    spark.table(f"{S}.review")
    .select(
        "review_id",
        "business_id",
        "user_id",
        F.date_format("review_date", "yyyyMMdd").cast("int").alias("date_key"),
        "review_date",
        "review_year",
        "stars",
        "useful", "funny", "cool",
    )
)
write_gold(fact_review, "fact_review", partition_by="review_year", zorder="business_id")

# fact_checkin
# Pre-aggregated to (business, date) — dashboards want daily check-in counts, not millions of individual event rows.



fact_checkin = (
    spark.table(f"{S}.checkin")
    .groupBy("business_id", "checkin_date")
    .agg(F.count("*").alias("checkin_count"))
    .withColumn("date_key", F.date_format("checkin_date", "yyyyMMdd").cast("int"))
    .select("business_id", "date_key", "checkin_date", "checkin_count")
)
write_gold(fact_checkin, "fact_checkin")

# fact_tip



fact_tip = (
    spark.table(f"{S}.tip")
    .withColumn("date_key", F.date_format("tip_date", "yyyyMMdd").cast("int"))
    .select("business_id", "user_id", "date_key", "tip_date", "compliment_count")
)
write_gold(fact_tip, "fact_tip")

# Example BI query — "rising stars" on the gold model
# Same logic as `sql/rising_star_businesses.sql`, expressed against `fact_review` + `dim_business` to show the star schema answering a real question end-to-end.



display(spark.sql(f"""
    WITH bounds AS (
        SELECT date_sub(MAX(review_date), 365) AS cutoff FROM {G}.fact_review
    ),
    m AS (
        SELECT business_id,
               COUNT(CASE WHEN review_date >= cutoff THEN 1 END)        AS reviews_py,
               AVG(CASE WHEN review_date >= cutoff THEN stars END)       AS avg_py,
               AVG(CASE WHEN review_date <  cutoff THEN stars END)       AS avg_prior
        FROM {G}.fact_review CROSS JOIN bounds
        GROUP BY business_id
    )
    SELECT m.business_id, b.name, m.reviews_py,
           ROUND(m.avg_py,2) AS avg_past_year,
           ROUND(m.avg_prior,2) AS avg_prior,
           ROUND(m.avg_py - m.avg_prior,2) AS improvement
    FROM m JOIN {G}.dim_business b USING (business_id)
    WHERE m.reviews_py >= 10 AND m.avg_prior IS NOT NULL
      AND m.avg_py >= m.avg_prior + 1.0
    ORDER BY improvement DESC, reviews_py DESC
    LIMIT 50
"""))
