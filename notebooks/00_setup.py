# Databricks notebook source and Environment Setup & Config
# Layer   Schema           Contents                                             
#-------------------------------------------------------------------------------
# Landing `yelp.landing`   Volume holding the raw `*.json` files                
# Bronze  `yelp.bronze`    Raw JSON loaded 1:1 into Delta + ingestion metadata  
# Silver  `yelp.silver`    Cleaned, typed, de-duplicated, conformed entities    
# Gold    `yelp.gold`      Star-schema facts & dimensions for BI                


# Centralised here so the other notebooks just `%run ./00_setup` (or import these).



# On Databricks Free Edition use the workspace catalog (run the next cell to detect it).
# On a managed/enterprise workspace, set this to a dedicated catalog
CATALOG = spark.sql("SELECT current_catalog()").first()[0]

SCHEMAS = {
    "landing": f"{CATALOG}.yelp_landing",
    "bronze":  f"{CATALOG}.yelp_bronze",
    "silver":  f"{CATALOG}.yelp_silver",
    "gold":    f"{CATALOG}.yelp_gold",
}

# Volume that holds the raw Yelp JSON files (upload target).
LANDING_VOLUME = f"{SCHEMAS['landing']}.raw"
LANDING_PATH   = f"/Volumes/{CATALOG}/yelp_landing/raw"

# The five Yelp source files -> logical entity names used throughout the pipeline.
SOURCE_FILES = {
    "business": "yelp_academic_dataset_business.json",
    "review":   "yelp_academic_dataset_review.json",
    "user":     "yelp_academic_dataset_user.json",
    "checkin":  "yelp_academic_dataset_checkin.json",
    "tip":      "yelp_academic_dataset_tip.json",
}

print(f"Catalog        : {CATALOG}")
print(f"Landing volume : {LANDING_VOLUME}")
print(f"Landing path   : {LANDING_PATH}")


# Create schemas & landing volume



for layer, schema in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"schema ready: {schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {LANDING_VOLUME}")
print(f"volume ready: {LANDING_VOLUME}")


display(dbutils.fs.ls(LANDING_PATH))
