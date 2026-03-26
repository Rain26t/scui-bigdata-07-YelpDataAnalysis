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

# =============================================================================
# II. 5. Proportion of total users and silent users (0 reviews) each year
# =============================================================================
def run_ii5():
    silent_users = user_df.withColumn("year", F.year("user_yelping_since")) \
        .withColumn("is_silent", F.when(F.col("user_review_count") == 0, 1).otherwise(0)) \
        .groupBy("year").agg(
            F.count("user_id").alias("total_users"),
            F.sum("is_silent").alias("silent_users")
        ) \
        .withColumn("silent_proportion", F.col("silent_users") / F.col("total_users")) \
        .orderBy("year")
    z.show(silent_users)

# =============================================================================
# II. 6. Yearly stats: New users, total reviews, elite users, and fans
# =============================================================================
def run_ii6():
    yearly_stats = user_df.withColumn("year", F.year("user_yelping_since")) \
        .groupBy("year").agg(
            F.count("user_id").alias("new_users"),
            F.sum("user_review_count").alias("total_reviews"),
            F.sum(F.when((F.col("user_elite").isNotNull()) & (F.col("user_elite") != ""), 1).otherwise(0)).alias("elite_users"),
            F.sum("user_fans").alias("total_fans")
        ).orderBy("year")
    z.show(yearly_stats)

# =============================================================================
# II. 7. Early adopters: First 5 reviews for 4.5+ star businesses
# =============================================================================
def run_ii7():
    successful_biz = biz_df.filter((F.col("stars") >= 4.5) & (F.col("review_count") > 100))
    window_spec = Window.partitionBy("rev_business_id").orderBy("rev_date")
    early_reviews = rev_df.withColumn("review_rank", F.row_number().over(window_spec)) \
        .filter(F.col("review_rank") <= 5)

    tastemakers = early_reviews.join(successful_biz, early_reviews.rev_business_id == successful_biz.business_id) \
        .groupBy("rev_user_id").count().orderBy(F.desc("count"))
    z.show(tastemakers.limit(20))