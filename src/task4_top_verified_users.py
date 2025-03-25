from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# TODO: Implement the task here
top_verified = (
    posts_df.join(users_df.filter(col("Verified") == True), "UserID")
    .withColumn("TotalReach", col("Likes") + col("Retweets"))
    .groupBy("Username")
    .agg(_sum("TotalReach").alias("TotalReach"))
    .orderBy(col("TotalReach").desc())
    .limit(5)
)

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)
