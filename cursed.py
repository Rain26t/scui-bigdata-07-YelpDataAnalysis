%pyspark
from pyspark.sql import functions as F

# 1. Group by exact address and city to find turnover
storefront_lifecycle = spark.table("yelp_db.business") \
    .filter(F.col("state").isin("PA", "FL", "LA")) \
    .groupBy("address", "city", "state") \
    .agg(
        F.count("business_id").alias("tenant_count"),
        F.sum(F.col("is_open").cast("int")).alias("currently_open"),
        F.collect_list("name").alias("previous_tenants")
    ) \
    .filter((F.col("tenant_count") >= 3) & (F.col("currently_open") == 0)) \
    .orderBy(F.desc("tenant_count"))

print("Top 50 Cursed Storefronts found:")
z.show(storefront_lifecycle.limit(50))




%pyspark
from pyspark.sql import functions as F

# Re-define biz just in case
biz = spark.table("yelp_db.business").filter(F.col("state").isin("PA", "FL", "LA"))

# PASTE ONE ADDRESS FROM THE STEP 1 RESULT HERE
target_address = "3131 Walnut St"

# Look at the shared attributes (Parking, Noise, etc.) of all failures at this spot
flaw_check = biz.filter(F.col("address") == target_address) \
    .select("name", "stars", "attributes", "categories", "is_open")

z.show(flaw_check)



%pyspark
from pyspark.sql import functions as F

reviews = spark.table("yelp_db.review")
biz = spark.table("yelp_db.business")

# Join reviews to the specific cursed address
death_signals = reviews.join(biz, reviews.rev_business_id == biz.business_id) \
    .filter(F.col("address") == "3131 Walnut St") \
    .withColumn("issue_type",
        F.when(F.col("rev_text").rlike("(?i)parking|car|garage|valet"), "Parking/Access")
         .when(F.col("rev_text").rlike("(?i)hidden|find|entrance|locate|back alley"), "Visibility")
         .when(F.col("rev_text").rlike("(?i)expensive|price|rent|overpriced"), "Cost/Value")
         .otherwise("Service/Food"))

# Summary of why this specific storefront keeps failing
issue_summary = death_signals.groupBy("issue_type").count().orderBy(F.desc("count"))
z.show(issue_summary)