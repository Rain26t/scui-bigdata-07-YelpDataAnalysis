%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- INITIALIZATION for Section III ---
spark.sql("USE yelp_db")
review_df = spark.table("review")
user_df = spark.table("users")
biz_df = spark.table("business")

# =============================================================================
# III. 1. Count the number of reviews per year
# =============================================================================
def run_iii1():
    reviews_per_year = review_df.withColumn("year", F.year("rev_date")) \
        .groupBy("year").count().orderBy("year")
    z.show(reviews_per_year)
# =============================================================================
# III. 2. Count the number of useful, funny, and cool review votes
# =============================================================================
def run_iii2():
    engagement_stats = review_df.select(
        F.sum("rev_useful").alias("Total_Useful"),
        F.sum("rev_funny").alias("Total_Funny"),
        F.sum("rev_cool").alias("Total_Cool")
    )
    z.show(engagement_stats)

    # =============================================================================
    # III. 3. Rank users by the total number of reviews each year (Top 10)
    # =============================================================================
    def run_iii3():
        yearly_user_counts = review_df.withColumn("year", F.year("rev_date")) \
            .groupBy("rev_user_id", "year").count()

        window_spec = Window.partitionBy("year").orderBy(F.desc("count"))
        ranked_users = yearly_user_counts.withColumn("rank", F.row_number().over(window_spec)) \
            .join(user_df, yearly_user_counts.rev_user_id == user_df.user_id) \
            .select("year", "user_name", "count", "rank") \
            .filter("rank <= 10").orderBy("year", "rank")
        z.show(ranked_users)

        # =============================================================================
        # III. 4: Extract the Top 20 most common words (Length > 3, No Stopwords)
        # =============================================================================
        def run_iii4():
            stops = ["the", "and", "was", "for", "that", "with", "this", "they", "have", "were"]
            top_20_words = review_df.withColumn("word", F.explode(F.split(F.lower(F.col("rev_text")), "\\s+"))) \
                .filter(~F.col("word").isin(stops)) \
                .filter(F.length("word") > 3) \
                .groupBy("word").count().orderBy(F.desc("count")).limit(20)
            z.show(top_20_words)

            # =============================================================================
            # III. 5 & 6: Top 10 words from Positive (>3 stars) vs Negative (<=3 stars)
            # =============================================================================
            def run_iii5_pos():
                top_pos_words = review_df.filter("rev_stars > 3") \
                    .withColumn("word", F.explode(F.split(F.lower(F.col("rev_text")), "\\s+"))) \
                    .filter(F.length("word") > 4) \
                    .groupBy("word").count().orderBy(F.desc("count")).limit(10)
                z.show(top_pos_words)

            def run_iii6_neg():
                top_neg_words = review_df.filter("rev_stars <= 3") \
                    .withColumn("word", F.explode(F.split(F.lower(F.col("rev_text")), "\\s+"))) \
                    .filter(F.length("word") > 4) \
                    .groupBy("word").count().orderBy(F.desc("count")).limit(10)
                z.show(top_neg_words)

            # =============================================================================
            # III. 7: Word cloud analysis (Top 100 Descriptive Words)
            # =============================================================================
            def run_iii7():
                stop_actions = ["have", "went", "came", "give", "take", "said", "asked", "told", "were", "they"]
                word_cloud_data = review_df.withColumn("word", F.explode(F.split(F.lower(F.col("rev_text")), "\\s+"))) \
                    .filter(F.length("word") > 4) \
                    .filter(~F.col("word").isin(stop_actions)) \
                    .groupBy("word").count() \
                    .orderBy(F.desc("count")).limit(100)
                z.show(word_cloud_data)