%pyspark
from pyspark.sql import functions as F

# --- INITIALIZATION for Section I ---
spark.sql("USE yelp_db")
business_df = spark.table("business")
review_df = spark.table("review")

us_states = ["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
             "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
             "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
             "VA","WA","WV","WI","WY"]

# =============================================================================
# I. 1: Identify the 20 most common merchants in the U.S.
# =============================================================================
def run_i1():
    top_merchants = business_df.filter(F.col("state").isin(us_states)) \
        .filter(F.col("name").isNotNull()) \
        .groupBy("name") \
        .agg(F.count("*").alias("location_count")) \
        .orderBy(F.desc("location_count")) \
        .limit(20)
    z.show(top_merchants)

# =============================================================================
# I. 2: Identify the top 10 cities with the most merchants in the U.S.
# =============================================================================
def run_i2():
    top_cities = business_df.filter(F.col("state").isin(us_states)) \
        .filter(F.col("city").isNotNull()) \
        .groupBy("city") \
        .agg(F.count("*").alias("merchant_count")) \
        .orderBy(F.desc("merchant_count")) \
        .limit(10)
    z.show(top_cities)

# =============================================================================
# I. 3: Identify the top 5 states with the most merchants in the U.S.
# =============================================================================
def run_i3():
    top_states = business_df.groupBy("state") \
        .agg(F.count("*").alias("merchant_count")) \
        .orderBy(F.desc("merchant_count")) \
        .limit(5)
    z.show(top_states)
# =============================================================================
# I. 4: Identify the 20 most common merchants and display their average ratings
# =============================================================================
def run_i4():
    merchants_with_stars = business_df.groupBy("name") \
        .agg(
            F.count("*").alias("number_of_locations"),
            F.round(F.avg("stars"), 2).alias("average_rating")
        ) \
        .orderBy(F.desc("number_of_locations")) \
        .limit(20)
    z.show(merchants_with_stars)

# =============================================================================
# I. 5: Count the total number of unique business categories
# =============================================================================
def run_i5():
    unique_categories = business_df.filter(F.col("categories").isNotNull()) \
        .withColumn("cat_array", F.split(F.col("categories"), ",\\s*")) \
        .withColumn("category", F.explode(F.col("cat_array"))) \
        .select(F.trim(F.col("category")).alias("category")) \
        .filter(F.col("category") != "") \
        .distinct() \
        .count()
    print(f"Total Unique Categories: {unique_categories}")
# =============================================================================
# I. 6: Identify the top 10 most frequent categories and their count
# =============================================================================
def run_i6():
    top_categories = business_df.filter(F.col("categories").isNotNull()) \
        .withColumn("category", F.explode(F.split(F.col("categories"), ",\\s*"))) \
        .select(F.trim(F.col("category")).alias("category")) \
        .filter(F.col("category") != "") \
        .groupBy("category") \
        .agg(F.count("*").alias("count")) \
        .orderBy(F.desc("count")) \
        .limit(10)
    z.show(top_categories)

# =============================================================================
# I. 7: Identify the top 20 merchants that received the most five-star reviews
# =============================================================================
def run_i7():
    five_star_leaders = review_df.filter(F.col("rev_stars") == 5) \
        .join(business_df, review_df.rev_business_id == business_df.business_id) \
        .groupBy("name") \
        .agg(F.count("*").alias("five_star_count")) \
        .orderBy(F.desc("five_star_count")) \
        .limit(20)
    z.show(five_star_leaders)


# =============================================================================
# I. 8: Count the number of restaurant types (Chinese, American, Mexican)
# =============================================================================
def run_i8():
    cuisine_counts = business_df.filter(F.col("categories").contains("Restaurants")) \
        .withColumn("type", F.when(F.col("categories").contains("Chinese"), "Chinese")
                             .when(F.col("categories").contains("American"), "American")
                             .when(F.col("categories").contains("Mexican"), "Mexican")
                             .otherwise(None)) \
        .filter(F.col("type").isNotNull()) \
        .groupBy("type") \
        .count()
    z.show(cuisine_counts)

# =============================================================================
# I. 9: Count the number of reviews for each restaurant type
# =============================================================================
def run_i9():
    review_volume = business_df.filter(F.col("categories").contains("Restaurants")) \
        .withColumn("type", F.when(F.col("categories").contains("Chinese"), "Chinese")
                             .when(F.col("categories").contains("American"), "American")
                             .when(F.col("categories").contains("Mexican"), "Mexican")
                             .otherwise(None)) \
        .filter(F.col("type").isNotNull()) \
        .groupBy("type") \
        .agg(F.sum("review_count").alias("total_reviews"))
    z.show(review_volume)
# =============================================================================
# I. 10: Analyze the rating distribution (average) for different restaurant types
# =============================================================================
def run_i10():
    rating_dist = business_df.filter(F.col("categories").contains("Restaurants")) \
        .withColumn("type", F.when(F.col("categories").contains("Chinese"), "Chinese")
                             .when(F.col("categories").contains("American"), "American")
                             .when(F.col("categories").contains("Mexican"), "Mexican")
                             .otherwise(None)) \
        .filter(F.col("type").isNotNull()) \
        .groupBy("type") \
        .agg(F.round(F.avg("stars"), 2).alias("avg_rating"))
    z.show(rating_dist)


# =============================================================================
# I. 11: Find businesses whose avg rating in last 12 months increased by >= 1 star
# =============================================================================
def run_i11():
    historical = review_df.groupBy("rev_business_id").agg(F.avg("rev_stars").alias("old_avg"))
    max_date = review_df.select(F.max("rev_date")).collect()[0][0]
    recent = review_df.filter(F.col("rev_date") >= F.add_months(F.lit(max_date), -12)) \
        .groupBy("rev_business_id").agg(F.avg("rev_stars").alias("new_avg"))
    turnarounds = recent.join(historical, "rev_business_id") \
        .filter((F.col("new_avg") - F.col("old_avg")) >= 1.0) \
        .join(business_df, recent.rev_business_id == business_df.business_id) \
        .select("name", "old_avg", "new_avg")
    z.show(turnarounds)

# =============================================================================
# I. 12: Identify top 10 pairs of distinct categories that co-occur
# =============================================================================
def run_i12():
    synergy = business_df.filter(F.col("categories").isNotNull()) \
        .withColumn("cat_list", F.split(F.col("categories"), ",\\s*")) \
        .withColumn("c1", F.explode("cat_list")) \
        .withColumn("c2", F.explode("cat_list")) \
        .filter(F.col("c1") < F.col("c2")) \
        .groupBy("c1", "c2") \
        .count() \
        .orderBy(F.desc("count")) \
        .limit(10)
    z.show(synergy)

# =============================================================================
# I. 13: Find polarizing merchants (high standard deviation in ratings)
# =============================================================================
def run_i13():
    polarizing = review_df.groupBy("rev_business_id") \
        .agg(F.count("*").alias("rev_vol"), F.stddev("rev_stars").alias("std_dev")) \
        .filter("rev_vol > 50") \
        .join(business_df, F.col("rev_business_id") == business_df.business_id) \
        .select("name", "std_dev", "rev_vol") \
        .orderBy(F.desc("std_dev")) \
        .limit(20)
    z.show(polarizing)

# --- EXECUTION ---
# Uncomment the line you want to run
run_i1()
#df





