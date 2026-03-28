%pyspark
from pyspark.sql import functions as F

# 1. LOAD INTERNAL DATA
biz = spark.table("yelp_db.business").filter(F.col("state").isin("PA", "FL", "LA"))

# 2. EXTRACT MACRO-ECONOMIC PROXY
# We use 'RestaurantsPriceRange2' to simulate Census Income Levels
# 1 = Low Income Neighborhood, 4 = High Income/Luxury Neighborhood
safari_df = biz.withColumn("econ_tier", F.get_json_object(F.col("attributes"), "$.RestaurantsPriceRange2")) \
    .filter(F.col("econ_tier").isNotNull())

# 3. CALCULATE SURVIVAL METRICS (The Safari 'Gold')
# We calculate the mortality rate for each economic tier
macro_report = safari_df.groupBy("econ_tier") \
    .agg(
        F.count("business_id").alias("total_businesses"),
        F.avg(F.col("is_open").cast("int")).alias("survival_rate"),
        F.avg("stars").alias("avg_rating")
    ) \
    .withColumn("mortality_pct", F.round((1 - F.col("survival_rate")) * 100, 2)) \
    .orderBy("econ_tier")

# 4. SPATIAL DENSITY CHECK (Simulating Transit/Urbanization Data)
# High density areas (hubs) usually have higher survival due to foot traffic
urban_hubs = biz.groupBy("postal_code", "city") \
    .agg(F.count("business_id").alias("biz_density"),
         F.avg(F.col("is_open").cast("int")).alias("hub_survival")) \
    .filter(F.col("biz_density") > 30) \
    .orderBy(F.desc("biz_density"))

# 5. EXECUTE
print("--- MACRO-HYPOTHESIS: THE GENTRIFICATION BUFFER ---")
z.show(macro_report)
print("--- SPATIAL SAFARI: TRANSIT & DENSITY HUBS ---")
z.show(urban_hubs.limit(10))