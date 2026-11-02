# Distributed Big Data Analysis of YouTube Trending Videos (Apache Spark + MySQL)

An automated, end-to-end data pipeline that ingests five countries' worth of YouTube trending data, cleans and transforms it with **PySpark and Spark SQL**, stores it in **Parquet** and **MySQL**, trains a **Spark MLlib Random Forest** to predict high-view videos, and enriches the history with live data from the **YouTube Data API v3**.

Built as an individual project for the *Data Intensive Scalable Systems* module (MSc Data Analytics, National College of Ireland).

**Demo video:** https://www.youtube.com/watch?v=oNjWLDLeRZ4

## Architecture

```
Kaggle CSVs (US, GB, DE, FR, CA) ─┐
                                   ├─> PySpark + Spark SQL (clean, try_cast, de-duplicate, feature engineering)
YouTube Data API v3 (US, GB, IN) ──┘            │
                                                ├─> Parquet (columnar storage layer)
                                                ├─> Spark MLlib Random Forest (high vs low views)
                                                └─> MySQL (country_stats, category_stats, ml_results, api_trending)
                                                          │
                                                          └─> Follow-up analysis read back from MySQL -> charts
```

## What it does

| Stage | Details |
|---|---|
| Ingest | Reads 5 country CSVs into Spark (`inferSchema=False`, `multiLine=True` to survive malformed rows and titles with line breaks) |
| Clean | SQL `try_cast` for safe numeric conversion, de-duplication on `(video_id, country)`, removes null and zero-view rows -> **94,258 records** |
| Features | `like_view_ratio`, `comment_view_ratio`, `views_millions` |
| Storage | Cleaned data saved as Parquet; analysis results written to MySQL over JDBC |
| ML | Random Forest (50 trees, 80/20 split, seed 42) predicting views at or above the median -> **AUC 0.9904** |
| Live data | Pulls current trending videos for US, GB and IN via the YouTube Data API (~148 videos) |
| Analysis | Country and category comparisons, feature importance, views-vs-engagement trade-off, country efficiency score, historical-vs-live comparison |
| Automation | `run_pipeline.sh` runs the pipeline end to end; database, tables and the JDBC driver are created or downloaded automatically |

## Key findings

- Great Britain has the highest average views per trending video (about 1.29M) but the fewest trending videos.
- Music is the only category that leads on both average views and engagement (like-to-view ratio about 6.7%).
- Engagement features (likes, dislikes, comments) are the strongest predictors of high viewership.

## Tech stack

Python, Apache Spark 4 (PySpark, Spark SQL, MLlib), MySQL, Parquet, Pandas, Matplotlib, SQLAlchemy, YouTube Data API v3, Bash.

## Prerequisites

1. **Java 17** (macOS: `brew install openjdk@17`; Windows: https://adoptium.net)
2. **MySQL** installed and running (macOS: `brew install mysql && brew services start mysql`)
3. **Python 3.10+** and the packages in `requirements.txt` (the script also installs missing packages automatically)
4. A **YouTube Data API v3 key** (Google Cloud Console -> APIs & Services -> enable *YouTube Data API v3* -> create an API key)

## Setup

```bash
git clone https://github.com/Mithillessh/youtube-trending-spark-pipeline.git
cd youtube-trending-spark-pipeline
pip install -r requirements.txt

export YOUTUBE_API_KEY="your-key"
export MYSQL_PASSWORD="your-mysql-password"   # defaults to "root" if unset
```

Download the dataset from Kaggle (https://www.kaggle.com/datasets/datasnaek/youtube-new) and place these files in `~/youtube_project/data/` (the path the pipeline uses):

`USvideos.csv`, `GBvideos.csv`, `DEvideos.csv`, `FRvideos.csv`, `CAvideos.csv`

> The dataset is not included in this repository.

## Run

- **Terminal (macOS/Linux):** `bash run_pipeline.sh`
- **Jupyter:** open `youtube_pipeline.py` (jupytext percent format) and run the cells top to bottom.
- **Direct:** `python youtube_pipeline.py`

Charts are saved to `~/youtube_project/output/` and results are written to the `youtube_analytics` MySQL database.

## Limitations

- The 0.99 AUC is driven by engagement features (likes, dislikes, comment counts) that are strongly tied to view count. It shows these signals are highly informative, not that views can be predicted before a video has any engagement.
- The Kaggle dataset is historical, so the live API comparison is a snapshot, not a like-for-like time series.
- Single-machine (local) Spark; the code is written with the DataFrame API so it can scale to a cluster.

## Author

Mithillessh Vennila Sigamani: MSc Data Analytics, National College of Ireland.
