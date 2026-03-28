%pyspark
from pyspark.sql import functions as F

# 1. Load your now-working Hive tables
weather = spark.table("yelp_db.weather_pilot")
biz = spark.table("yelp_db.business").filter(F.col("state").isin("PA", "FL", "LA"))
reviews = spark.table("yelp_db.review")

# 2. Clean & Prepare Weather Data
# Regex extracts state from "NAME" and converts DATE_STR to a Date type
weather_clean = weather.withColumn("state_ext", F.regexp_extract(F.col("NAME"), r"([A-Z]{2})\sUS$", 1)) \
                       .withColumn("w_date", F.to_date(F.col("DATE_STR"), "yyyy-MM-dd")) \
                       .select("state_ext", "w_date", "PRCP", "TMAX")

# 3. Join logic
# Standardizing city to uppercase during the join for cleaner grouping
final_join = reviews.join(biz, reviews.rev_business_id == biz.business_id) \
    .join(weather_clean, (reviews.rev_date == weather_clean.w_date) &
                         (biz.state == weather_clean.state_ext))

# 4. Final Aggregation with 1-Star and 5-Star counts
# We define 'Rainy' as PRCP > 0
analysis = final_join.withColumn("condition", F.when(F.col("PRCP") > 0, "Rainy").otherwise("Sunny/Dry")) \
    .groupBy("state", F.upper(F.col("city")).alias("city_name"), "condition") \
    .agg(
        F.avg("rev_stars").alias("avg_rating"),
        F.count(F.when(F.col("rev_stars") == 1, 1)).alias("count_1_star"),
        F.count(F.when(F.col("rev_stars") == 5, 1)).alias("count_5_star"),
        F.count("*").alias("total_reviews")
    ).orderBy("state", "city_name", "condition")

# 5. Output
z.show(analysis)






