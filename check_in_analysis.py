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
