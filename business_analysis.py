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


