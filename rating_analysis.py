%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- INITIALIZATION for Section IV ---
spark.sql("USE yelp_db")
review_df = spark.table("review")
biz_df = spark.table("business")

# =============================================================================
# IV. 1: Analyze the overall distribution of star ratings (1 to 5)
# =============================================================================
def run_iv1():
    rating_dist = review_df.groupBy("rev_stars").count().orderBy("rev_stars")
    z.show(rating_dist)

# =============================================================================
# IV. 2: Identify which days of the week users are most likely to leave reviews
# =============================================================================
def run_iv2():
    weekly_freq = review_df.withColumn("day_name", F.date_format("rev_date", "EEEE")) \
        .groupBy("day_name").count().orderBy(F.desc("count"))
    z.show(weekly_freq)

# =============================================================================
# IV. 3: Find businesses with the highest volume of 5-star feedback
# =============================================================================
def run_iv3():
    top_5_star = review_df.filter("rev_stars = 5") \
        .groupBy("rev_business_id").count() \
        .join(biz_df.select("business_id", "name"), review_df.rev_business_id == biz_df.business_id) \
        .orderBy(F.desc("count"))
    z.show(top_5_star.limit(20))

# =============================================================================
# IV. 4: Identify the top 10 cities with the highest average star ratings
# Filtered for cities with >50 businesses
# =============================================================================
def run_iv4():
    top_cities = biz_df.groupBy("city") \
        .agg(F.avg("stars").alias("avg_rating"), F.count("business_id").alias("biz_count")) \
        .filter("biz_count > 50") \
        .orderBy(F.desc("avg_rating"))
    z.show(top_cities.limit(10))

# =============================================================================
# IV. 5: Calculate business performance vs. local cuisine average
# =============================================================================
def run_iv5():
    window_spec = Window.partitionBy("city", "categories")
    diff_df = biz_df.withColumn("cat_avg", F.avg("stars").over(window_spec)) \
        .withColumn("differential", F.col("stars") - F.col("cat_avg")) \
        .select("name", "city", "categories", "stars", "differential")
    z.show(diff_df.limit(20))


# =============================================================================
# IV. 6: Compare weekend vs. weekday satisfaction for "Nightlife"
# =============================================================================
def run_iv6():
    nightlife_sat = review_df.join(biz_df.filter(F.col("categories").contains("Nightlife")),
                                   review_df.rev_business_id == biz_df.business_id) \
        .withColumn("period", F.when(F.date_format("rev_date", "EEEE").isin("Saturday", "Sunday"), "Weekend").otherwise(
        "Weekday")) \
        .groupBy("period").agg(F.avg("rev_stars").alias("avg_satisfaction"))
    z.show(nightlife_sat)


# --- EXECUTION ---
# Uncomment the line you want to run
run_iv1()
# run_iv2()
# run_iv3()
# run_iv4()
# run_iv5()
# run_iv6()

