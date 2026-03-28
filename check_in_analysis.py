%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- INITIALIZATION for Section V ---
spark.sql("USE yelp_db")
biz_df = spark.table("business")
checkin_df = spark.table("checkin")

# =============================================================================
# V. 1. Count the number of check-ins per year
# =============================================================================
def run_v1():
    ci_year = checkin_df.withColumn("ts", F.explode(F.split(F.col("checkin_dates"), ", "))) \
        .groupBy(F.year("ts").alias("year")).count().orderBy("year")
    z.show(ci_year)

# =============================================================================
# V. 2. Count the number of check-ins per hour within a 24-hour period
# =============================================================================
def run_v2():
    ci_hour = checkin_df.withColumn("ts", F.explode(F.split(F.col("checkin_dates"), ", "))) \
        .groupBy(F.hour("ts").alias("hour")).count().orderBy("hour")
    z.show(ci_hour)

# =============================================================================
# V. 3. Identify the most popular city for check-ins
# =============================================================================
def run_v3():
    popular_city = checkin_df.withColumn("ts", F.explode(F.split(F.col("checkin_dates"), ", "))) \
        .join(biz_df, "business_id") \
        .groupBy("city").count().orderBy(F.desc("count"))
    z.show(popular_city.limit(10))

# =============================================================================
# V. 4. Rank all businesses based on check-in counts
# =============================================================================
def qV4():
    biz_rank = checkin_df.withColumn("ts", F.explode(F.split(F.col("checkin_dates"), ", "))) \
        .groupBy("business_id").count() \
        .join(biz_df.select("business_id", "name"), "business_id") \
        .orderBy(F.desc("count"))
    z.show(biz_rank)

# =============================================================================
# V. 5. Calculate the MoM check-in growth rate (Trending Locations)
# =============================================================================
def run_v5():
    window_mom = Window.partitionBy("business_id").orderBy("month")
    mom_growth = checkin_df.withColumn("ts", F.explode(F.split(F.col("checkin_dates"), ", "))) \
        .withColumn("month", F.date_format("ts", "yyyy-MM")) \
        .groupBy("business_id", "month").count() \
        .withColumn("prev_count", F.lag("count").over(window_mom)) \
        .withColumn("growth_rate", (F.col("count") - F.col("prev_count")) / F.col("prev_count")) \
        .join(biz_df.select("business_id", "name", "city", "categories"), "business_id") \
        .filter(F.col("categories").contains("Restaurants")) \
        .filter("growth_rate IS NOT NULL") \
        .orderBy(F.desc("growth_rate"))
    z.show(mom_growth.limit(50))

# =============================================================================
# V. 6. Analyze review seasonality by cuisine (Ice Cream vs. Soup)
# =============================================================================
def run_v6():
    seasonal_analysis = checkin_df.withColumn("ts", F.explode(F.split(F.col("checkin_dates"), ", "))) \
        .join(biz_df, "business_id") \
        .withColumn("month_num", F.month("ts")) \
        .filter(F.col("categories").rlike("Ice Cream|Soup")) \
        .groupBy("month_num", "categories").count() \
        .orderBy("month_num")
    z.show(seasonal_analysis)

# --- EXECUTION ---
# Change these calls to run the specific question you want to see
run_v1()
# run_v2()
# run_v3()
# run_v4()
# run_v5()
# run_v6()