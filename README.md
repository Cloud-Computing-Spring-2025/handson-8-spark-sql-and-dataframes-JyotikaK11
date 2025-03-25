# Hashtag & Engagement Analysis using PySpark

In this project, we perform social media data analysis using PySpark to uncover insights around hashtag trends, user engagement patterns, sentiment-based behavior, and influential users. The dataset includes user metadata and post metrics (likes, retweets, hashtags, sentiment score, etc.).

## Objectives

* **Hashtag Trends**: Analyze frequency of hashtags across all posts to find the most popular ones.
* **Engagement by Age Group**: Calculate and compare average likes and retweets by user age categories.
* **Sentiment vs Engagement**: Explore how sentiment influences engagement rates.
* **Top Verified Users by Reach**: Identify the most impactful verified users based on their total reach (likes + retweets).

---

## Setup and Execution

### Project Structure
```
handson-8-spark-sql-and-dataframes-social-analysis-JyotikaK11/
├── input/
│   ├── posts.csv
│   └── users.csv
├── outputs/
│   ├── hashtag_trends.csv
│   ├── engagement_by_age.csv
│   ├── sentiment_engagement.csv
│   └── top_verified_users.csv
├── src/
│   ├── task1_hashtag_trends.py
│   ├── task2_engagement_by_age.py
│   ├── task3_sentiment_vs_engagement.py
│   └── task4_top_verified_users.py
└── README.md
```

### Prerequisites
- Python 3.x
- Apache Spark (with PySpark)
- Optionally: Docker & Docker Compose

### Run Tasks Locally
```bash
cd handson-8-spark-sql-and-dataframes-social-analysis-JyotikaK11/
spark-submit src/task1_hashtag_trends.py
spark-submit src/task2_engagement_by_age.py
spark-submit src/task3_sentiment_vs_engagement.py
spark-submit src/task4_top_verified_users.py
```

### Output Verification
Check the `outputs/` folder for generated result files:
```
hashtag_trends.csv
engagement_by_age.csv
sentiment_engagement.csv
top_verified_users.csv
```

---

## Task 1: Hashtag Trends
- **Split** hashtags from comma-separated strings.
- **Explode** into individual hashtag rows.
- **Group and Count** by hashtag.
- **Sort** to find top 10 most used hashtags.

### Output: `outputs/hashtag_trends.csv`
| Hashtag    | Count |
|------------|-------|
| #tech      | 25    |
| #bug       | 23    |
| #social    | 21    |
| #mood      | 20    |
| #ux        | 20    |
| #fail      | 19    |
| #ai        | 17    |
| #design    | 16    |
| #cleanui   | 15    |
| #love      | 14    |

---

## Task 2: Engagement by Age Group
- **Join** `posts.csv` and `users.csv` on `UserID`
- **Group by** AgeGroup
- **Calculate** average Likes and Retweets

### Output: `outputs/engagement_by_age.csv`
| Age Group | Avg Likes | Avg Retweets |
|-----------|-----------|--------------|
| Adult     | 78.0      | 22.6         |
| Senior    | 62.7      | 26.7         |
| Teen      | 57.9      | 25.3         |

---

## Task 3: Sentiment vs Engagement
- **Label Sentiment**:
  - Positive: SentimentScore > 0.3
  - Neutral: -0.3 ≤ SentimentScore ≤ 0.3
  - Negative: SentimentScore < -0.3
- **Group by Sentiment** and compute engagement

### Output: `outputs/sentiment_engagement.csv`
| Sentiment | Avg Likes | Avg Retweets |
|-----------|-----------|--------------|
| Negative  | 70.0      | 27.0         |
| Neutral   | 73.9      | 23.4         |
| Positive  | 59.1      | 24.6         |

---

## Task 4: Top Verified Users by Reach
- **Filter** verified users
- **Calculate Reach** = Likes + Retweets
- **Aggregate** total reach per user
- **Sort and Limit** to top 5

### Output: `outputs/top_verified_users.csv`
| Username       | Total Reach |
|----------------|-------------|
| @designer_dan  | 1274        |
| @critic99      | 1039        |

---

## Conclusion
This Spark-based analysis demonstrates how to process and analyze social engagement data efficiently. By leveraging DataFrame APIs, we performed end-to-end analysis covering popularity metrics, behavioral trends, and influencer impact in a scalable fashion. This project provides a foundation for expanding into more advanced analytics such as network influence, time-series engagement, or recommendation systems.


