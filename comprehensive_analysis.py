%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- INITIALIZATION for Section VI ---
spark.sql("USE yelp_db")
biz_df = spark.table("business")
checkin_df = spark.table("checkin")
review_df = spark.table("review")

# =============================================================================
# VI. 1. Identify the top 5 merchants in each city
# Metrics: rating frequency, average rating, and check-in frequency.
# =============================================================================
def run_vi1():
    # Calculate total check-ins per business by exploding the date string
    ci_totals = checkin_df.withColumn("ts", F.explode(F.split(F.col("checkin_dates"), ", "))) \
        .groupBy("business_id").count().withColumnRenamed("count", "ci_count")

    # Window function to rank businesses within each city based on Stars, Check-ins, and Review Count
    window_city = Window.partitionBy("city").orderBy(
        F.desc("stars"),
        F.desc("ci_count"),
        F.desc("review_count")
    )

    # Join business data with check-in totals and apply the ranking
    top_5_merchants = biz_df.join(ci_totals, "business_id", "left").fillna(0) \
        .withColumn("rank", F.row_number().over(window_city)) \
        .filter("rank <= 5") \
        .select("city", "name", "stars", "ci_count", "review_count")

    z.show(top_5_merchants)

# =============================================================================
# VI. 2. Calculate the review conversion rate
# Ratio of total check-ins to total reviews for top 100 most checked-in businesses.
# =============================================================================
def run_vi2():
    # Calculate conversion (Check-ins divided by Review Count) for the most visited places
    conversion_df = checkin_df.withColumn("ts", F.explode(F.split(F.col("checkin_dates"), ", "))) \
        .groupBy("business_id").count().withColumnRenamed("count", "total_checkins") \
        .join(biz_df.select("business_id", "name", "review_count"), "business_id") \
        .withColumn("conversion_rate", F.col("total_checkins") / F.col("review_count")) \
        .orderBy(F.desc("total_checkins")) \
        .limit(100)

    z.show(conversion_df)

# =============================================================================
# VI. 3.Analyze post-review check-in drop-off
# Impact of 1-star review spikes on monthly check-in volume.
# =============================================================================
def run_vi3():
    # 1. Identify businesses with a "spike" (more than 5 one-star reviews in a week)
    weekly_spikes = review_df.filter("rev_stars = 1") \
        .withColumn("year", F.year("rev_date")) \
        .withColumn("week", F.weekofyear("rev_date")) \
        .groupBy("rev_business_id", "year", "week").count() \
        .filter("count > 5")

    # 2. Calculate monthly check-in volumes using 'checkin_dates'
    checkin_monthly = checkin_df.withColumn("ts", F.explode(F.split(F.col("checkin_dates"), ", "))) \
        .withColumn("month_val", F.date_format("ts", "yyyy-MM")) \
        .groupBy("business_id", "month_val").count().withColumnRenamed("count", "monthly_ci")

    # 3. Join spikes with check-in data to observe the impact on foot traffic
    drop_off_analysis = weekly_spikes.join(
        checkin_monthly,
        weekly_spikes.rev_business_id == checkin_monthly.business_id
    ).select(
        F.col("rev_business_id").alias("business_id"),
        "year",
        "week",
        "month_val",
        "monthly_ci"
    ).orderBy("business_id", "year", "week", "month_val")

    z.show(drop_off_analysis)

# --- EXECUTION ---
# Uncomment the line you want to run in this Zeppelin paragraph
run_vi1()
# run_vi2()
# run_vi3()