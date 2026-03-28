%pyspark
from pyspark.sql import functions as F

# 1. LOAD TABLES
reviews = spark.table("yelp_db.review")
biz = spark.table("yelp_db.business").filter(F.col("state").isin("PA", "FL", "LA"))
# Based on your error, the table name is 'users'
users = spark.table("yelp_db.users")

# 2. DETECTION: GHOST USERS (Spatial Teleportation)
# Join reviews and business to find users in 2+ states on the same day
ghost_activity = reviews.join(biz, F.col("rev_business_id") == F.col("business_id")) \
    .groupBy("rev_user_id", "rev_date") \
    .agg(
        F.countDistinct("state").alias("states_teleported"),
        F.count("*").alias("reviews_sent_that_day")
    ) \
    .filter(F.col("states_teleported") > 1)

# 3. JOIN WITH USER TABLE
# Your error confirmed the column name is 'user_name' and the ID is 'user_id'
ghost_syndicate = ghost_activity.join(users, F.col("rev_user_id") == F.col("user_id")) \
    .select(
        F.col("user_name").alias("ghost_reviewer_name"),
        F.col("rev_date").alias("date_of_fraud"),
        F.col("states_teleported"),
        F.col("reviews_sent_that_day")
    ) \
    .orderBy(F.desc("states_teleported"), F.desc("reviews_sent_that_day"))

# 4. EXECUTE
print("--- THE SYNDICATE: GHOST USERS IDENTIFIED ---")
z.show(ghost_syndicate.limit(20))

#

%pyspark
from pyspark.sql import functions as F

# 1. LOAD TABLES
reviews = spark.table("yelp_db.review")
biz = spark.table("yelp_db.business").filter(F.col("state").isin("PA", "FL", "LA"))
users = spark.table("yelp_db.users")

# 2. IDENTIFY THE "SPIKE" EVENTS
# Finding days where a business got 10+ reviews with a 4.5+ average
spiky_days = reviews.groupBy("rev_business_id", "rev_date") \
    .agg(
        F.count("*").alias("review_burst_count"),
        F.avg("rev_stars").alias("avg_rating_that_day")
    ) \
    .filter((F.col("review_burst_count") >= 10) & (F.col("avg_rating_that_day") >= 4.5))

# 3. JOIN WITH BUSINESS AND USER DATA
# We include 'is_open' to see the current status of the shop
syndicate_report = reviews.join(spiky_days, ["rev_business_id", "rev_date"]) \
    .join(biz, reviews.rev_business_id == biz.business_id) \
    .join(users, reviews.rev_user_id == users.user_id) \
    .select(
        F.col("name").alias("business_target"),
        F.col("city"),
        # Create a readable status: 1 -> OPEN, 0 -> CLOSED
        F.when(F.col("is_open") == 1, "OPEN").otherwise("CLOSED").alias("current_status"),
        F.col("rev_date").alias("date_of_spike"),
        F.col("review_burst_count"),
        F.col("user_name").alias("suspicious_reviewer"),
        F.col("rev_stars").alias("stars_given")
    ) \
    .orderBy(F.desc("review_burst_count"), F.desc("date_of_spike"))

# 4. EXECUTE
print("--- REQUIREMENT III: REVIEW MANIPULATION & BUSINESS SURVIVAL ---")
z.show(syndicate_report.limit(50))