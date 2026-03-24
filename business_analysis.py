from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, trim


# =========================================================
# Task 1
# Identify the 20 most common merchants in the U.S.
# =========================================================
def task1_top_merchants(spark):
    us_states = [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
        "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
        "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
        "VA","WA","WV","WI","WY"
    ]

    business_df = spark.table("business")

    result = (
        business_df
        .filter(col("state").isin(us_states))
        .filter(col("name").isNotNull())
        .groupBy("name")
        .agg(count("*").alias("name_count"))
        .orderBy(col("name_count").desc())
        .limit(20)
    )

    print("\n=== Task 1: Top 20 Most Common Merchants in the U.S. ===\n")
    result.show(20, False)


# =========================================================
# Task 2
# Identify the top 10 cities with the most merchants in the U.S.
# =========================================================
def task2_top_cities(spark):
    us_states = [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
        "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
        "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
        "VA","WA","WV","WI","WY"
    ]

    business_df = spark.table("business")

    result = (
        business_df
        .filter(col("state").isin(us_states))
        .filter(col("city").isNotNull())
        .filter(trim(col("city")) != "")
        .groupBy("city")
        .agg(count("*").alias("merchant_count"))
        .orderBy(col("merchant_count").desc())
        .limit(10)
    )

    print("\n=== Task 2: Top 10 Cities with the Most Merchants in the U.S. ===\n")
    result.show(10, False)


def main():
    spark = (
        SparkSession.builder
        .appName("Yelp Business Analysis")
        .master("yarn")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Run Task 1
    task1_top_merchants(spark)

    # Run Task 2
    task2_top_cities(spark)

    spark.stop()


if __name__ == "__main__":
    main()
    main()


    # =========================================================
    # Task 3
    # Identify the top 5 states with the most merchants in the U.S.
    # =========================================================
    def task3_top_states(spark):
        us_states = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
            "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
            "VA", "WA", "WV", "WI", "WY"
        ]

        business_df = spark.table("business")

        result = (
            business_df
            .filter(col("state").isin(us_states))
            .filter(col("state").isNotNull())
            .groupBy("state")
            .agg(count("*").alias("merchant_count"))
            .orderBy(col("merchant_count").desc())
            .limit(5)
        )

        print("\n=== Task 3: Top 5 States with the Most Merchants in the U.S. ===\n")
        result.show(5, False)