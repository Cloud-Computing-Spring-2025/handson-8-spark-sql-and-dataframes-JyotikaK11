from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark Session
spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# Filter verified users
verified_users = users_df.filter(col("Verified") == True)

# Join with posts
joined_df = posts_df.join(verified_users, on="UserID", how="inner")

# Calculate Reach = Likes + Retweets
joined_df = joined_df.withColumn("Reach", col("Likes") + col("Retweets"))

# Aggregate total reach per verified user
user_reach = joined_df.groupBy("Username").agg(_sum("Reach").alias("Total_Reach"))

# Get top 5 verified users by reach
top_verified = user_reach.orderBy(col("Total_Reach").desc()).limit(5)

# Save result
top_verified.coalesce(1).write.mode("overwrite").option("header", True).csv("outputs/top_verified_users.csv")