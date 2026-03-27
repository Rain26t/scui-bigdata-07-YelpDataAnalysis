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
