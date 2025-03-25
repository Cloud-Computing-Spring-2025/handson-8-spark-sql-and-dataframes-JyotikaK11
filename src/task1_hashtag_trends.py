from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, trim, lower, desc

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# Split Hashtags column into individual hashtags, explode into rows
hashtags_df = posts_df.select(
    explode(
        split(col("Hashtags"), ",")
    ).alias("Hashtag")
).withColumn("Hashtag", trim(lower(col("Hashtag"))))

# Group by hashtag and count frequency
hashtag_counts = hashtags_df.groupBy("Hashtag").count().orderBy(desc("count")).limit(10)

# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").option("header", True).csv("outputs/hashtag_trends.csv")