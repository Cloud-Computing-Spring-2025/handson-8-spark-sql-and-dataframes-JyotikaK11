from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg, col, round

# Initialize Spark Session
spark = SparkSession.builder.appName("SentimentVsEngagement").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)

# Categorize sentiment
posts_df = posts_df.withColumn(
    "Sentiment",
    when(col("SentimentScore") > 0.3, "Positive")
    .when(col("SentimentScore") < -0.3, "Negative")
    .otherwise("Neutral")
)

# Group by sentiment and compute average likes and retweets
sentiment_stats = posts_df.groupBy("Sentiment").agg(
    round(avg("Likes"),1).alias("Avg_Likes"),
    round(avg("Retweets"),1).alias("Avg_Retweets")
).orderBy("Sentiment")

# Save result
sentiment_stats.coalesce(1).write.mode("overwrite").option("header", True).csv("outputs/sentiment_engagement.csv")