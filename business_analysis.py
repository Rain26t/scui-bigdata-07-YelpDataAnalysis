%pyspark
from pyspark.sql.functions import col, count

business = spark.table("business")

us_states = [
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA',
    'KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
    'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT',
    'VA','WA','WV','WI','WY'
]

result = business \
    .filter(col("state").isin(us_states)) \
    .filter(col("name").isNotNull()) \
    .groupBy("name") \
    .agg(count("*").alias("name_count")) \
    .orderBy(col("name_count").desc()) \
    .limit(20)

z.show(result)