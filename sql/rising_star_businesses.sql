
-- Rising Star Businesses

-- Identify businesses whose reputation has materially improved over the last year.

-- A "rising star" must satisfy BOTH:
--   (a) >= 10 reviews in the past year
--   (b) avg rating in the past year is at least 1.0 star higher than its avg rating in the period BEFORE the past year

-- Returns: business_id, name (+ the supporting metrics, which a reviewer/BI user will almost always want to see alongside the answer).

-- Engine: Databricks SQL / Spark SQL. Runs unchanged on the gold tables produced by the pipeline (gold.dim_business + a review fact), or directly on the silver review/business tables. Table names below assume the silver layer; swap the schema prefix if you point it at gold.

-- Design decisions:

-- 1. "Past year" is anchored to the LATEST review date in the dataset, not CURRENT_DATE.
--    The public Yelp dataset is a static historical snapshot; anchoring to today's date would return zero rows because every review is years old. In a live system you would replace `(SELECT MAX(review_date) FROM ...)` with CURRENT_DATE().

-- 2. A business with NO reviews before the past-year window is excluded. You cannot"rise" without a prior baseline to rise from. This is enforced by `avg_rating_prior IS NOT NULL`.

-- 3. The 1-star threshold is inclusive (>=). Tie-break ordering surfaces the biggest movers first.

WITH window_bounds AS (
    -- Cut-off = the start of the trailing 12-month window.
    SELECT date_sub(MAX(review_date), 365) AS cutoff_date
    FROM silver.review
),

reviews_tagged AS (
    SELECT
        r.business_id,
        r.stars,
        CASE
            WHEN r.review_date >= w.cutoff_date THEN 'past_year'
            ELSE 'prior'
        END AS period
    FROM silver.review r
    CROSS JOIN window_bounds w          
),

business_metrics AS (
    SELECT
        business_id,
        COUNT(CASE WHEN period = 'past_year' THEN 1 END)               AS reviews_past_year,
        AVG (CASE WHEN period = 'past_year' THEN stars END)            AS avg_rating_past_year,
        AVG (CASE WHEN period = 'prior'     THEN stars END)            AS avg_rating_prior
    FROM reviews_tagged
    GROUP BY business_id
)

SELECT
    m.business_id,
    b.name,
    m.reviews_past_year,
    ROUND(m.avg_rating_past_year, 2)                          AS avg_rating_past_year,
    ROUND(m.avg_rating_prior,     2)                          AS avg_rating_prior,
    ROUND(m.avg_rating_past_year - m.avg_rating_prior, 2)     AS rating_improvement
FROM business_metrics m
JOIN silver.business b
    ON b.business_id = m.business_id
WHERE m.reviews_past_year >= 10                               
  AND m.avg_rating_prior IS NOT NULL                          
  AND m.avg_rating_past_year >= m.avg_rating_prior + 1.0      
ORDER BY rating_improvement DESC, reviews_past_year DESC;
