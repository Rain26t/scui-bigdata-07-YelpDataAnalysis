%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- INITIALIZATION for Section IV ---
spark.sql("USE yelp_db")
review_df = spark.table("review")
biz_df = spark.table("business")

# =============================================================================
# IV. 1: Analyze the overall distribution of star ratings (1 to 5)
# =============================================================================
def run_iv1():
    rating_dist = review_df.groupBy("rev_stars").count().orderBy("rev_stars")
    z.show(rating_dist)