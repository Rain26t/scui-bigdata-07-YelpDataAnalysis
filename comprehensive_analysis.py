%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- INITIALIZATION for Section VI ---
spark.sql("USE yelp_db")
biz_df = spark.table("business")
checkin_df = spark.table("checkin")
review_df = spark.table("review")