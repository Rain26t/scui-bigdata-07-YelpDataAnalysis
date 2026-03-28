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



