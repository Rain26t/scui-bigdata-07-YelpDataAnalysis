%pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- INITIALIZATION for Section IV ---
spark.sql("USE yelp_db")
review_df = spark.table("review")
biz_df = spark.table("business")