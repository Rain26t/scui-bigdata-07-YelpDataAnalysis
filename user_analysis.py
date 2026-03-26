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

# =============================================================================
# II. 3. Identify the most popular users based on user_fans
# =============================================================================
def run_ii3():
    popular_users = user_df.select(F.col("user_name").alias("name"), "user_fans") \
        .orderBy(F.desc("user_fans"))
    z.show(popular_users.limit(20))

# =============================================================================
# II. 4. Calculate the ratio of elite users to regular users each year
# =============================================================================
def run_ii4():
    elite_ratio = user_df.withColumn("year", F.year("user_yelping_since")) \
        .withColumn("is_elite", F.when((F.col("user_elite").isNotNull()) & (F.col("user_elite") != ""), 1).otherwise(0)) \
        .groupBy("year").agg(
            F.sum("is_elite").alias("elite_count"),
            F.count("user_id").alias("total_users")
        ) \
        .withColumn("regular_count", F.col("total_users") - F.col("elite_count")) \
        .withColumn("elite_to_regular_ratio", F.col("elite_count") / F.col("regular_count")) \
        .orderBy("year")
    z.show(elite_ratio)