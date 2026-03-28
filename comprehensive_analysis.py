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

