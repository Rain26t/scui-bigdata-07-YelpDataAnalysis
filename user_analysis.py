%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- INITIALIZATION for Section II ---
spark.sql("USE yelp_db")
user_df = spark.table("users")
biz_df = spark.table("business")
rev_df = spark.table("review")

# =============================================================================
# II. 1. Analyze the number of users joining each year
# =============================================================================
def run_ii1():
    users_per_year = user_df.withColumn("join_year", F.year("user_yelping_since")) \
        .groupBy("join_year").count().orderBy("join_year")
    z.show(users_per_year)

# =============================================================================
# II. 2. Identify top reviewers based on user_review_count
# =============================================================================
def run_ii2():
    top_reviewers = user_df.select(F.col("user_name").alias("name"), "user_review_count") \
        .orderBy(F.desc("user_review_count"))
    z.show(top_reviewers.limit(20))