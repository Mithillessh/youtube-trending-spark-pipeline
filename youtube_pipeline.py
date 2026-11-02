# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: Python [conda env:anaconda3]
#     language: python
#     name: conda-env-anaconda3-py
# ---

# %%
# ============================================================
# YouTube Trending Video Analytics Pipeline
# Author: Mithillessh Vennila Sigamani
# Context: MSc Data Analytics, National College of Ireland (Data Intensive Scalable Systems)
#
# HOW TO RUN:
#   Option A — Jupyter: Open this file and run all cells top to bottom
#   Option B — Terminal: python youtube_pipeline.py
#
# PREREQUISITES (only 2 things needed):
#   1. Java 17 installed
#      Mac:     brew install openjdk@17
#      Windows: https://adoptium.net
#   2. MySQL installed and running
#      Mac:     brew install mysql && brew services start mysql
#      Windows: https://dev.mysql.com/downloads/mysql/
#
# EVERYTHING ELSE IS HANDLED AUTOMATICALLY:
#   - Python packages installed automatically
#   - MySQL JDBC JAR downloaded automatically
#   - MySQL database and tables created automatically
#   - All folders created automatically
#
# WORKS ON: Mac and Windows
# ============================================================
 
 
# ============================================================
# CELL 1: FULL SETUP
# This single cell handles everything needed before the pipeline runs:
#   Step 1 — Install all required Python packages automatically
#   Step 2 — Detect OS, set up Java, create folders, download JDBC JAR
#   Step 3 — Connect to MySQL and create database + tables
#   Step 4 — Start the Apache Spark session
#
# PREREQUISITES (only 2 things needed before running):
#   1. Java 17 installed
#      Mac:     brew install openjdk@17
#      Windows: https://adoptium.net
#   2. MySQL installed and running
#      Mac:     brew install mysql && brew services start mysql
#      Windows: https://dev.mysql.com/downloads/mysql/
# ============================================================
 
import subprocess
import sys
import os
import platform
import urllib.request
import pymysql
from pyspark.sql import SparkSession
 
print("=" * 55)
print(" YouTube Analytics Pipeline — Setup")
print("=" * 55)
 
# ── Step 1: Install Python packages ──────────────────────────
# Install all required packages automatically if not present.
# This ensures the pipeline runs without any manual pip installs.
print("\n[Step 1/4] Checking Python packages...")
 
REQUIRED_PACKAGES = [
    "pyspark",
    "pandas",
    "matplotlib",
    "sqlalchemy",
    "pymysql",
    "google-api-python-client",
    "numpy"
]
 
for package in REQUIRED_PACKAGES:
    try:
        import_name = "googleapiclient" if package == "google-api-python-client" else package
        __import__(import_name)
        print(f"  ✅ {package} already installed")
    except ImportError:
        print(f"  Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package, "-q"])
        print(f"  ✅ {package} installed")
 
print("All packages ready!")
 
# ── Step 2: Paths, Java, folders, JDBC JAR ───────────────────
# Detect OS, set JAVA_HOME, create project folders,
# and download the MySQL JDBC JAR automatically if missing.
print("\n[Step 2/4] Setting up environment...")
 
IS_WINDOWS = platform.system() == "Windows"
IS_MAC     = platform.system() == "Darwin"
print(f"  Running on: {platform.system()}")
 
# Project folder paths — works on both Mac and Windows
HOME         = os.path.expanduser("~")
PROJECT_DIR  = os.path.join(HOME, "youtube_project")
DATA_DIR     = os.path.join(PROJECT_DIR, "data")
OUTPUT_DIR   = os.path.join(PROJECT_DIR, "output")
JAR_DIR      = os.path.join(PROJECT_DIR, "jars")
PARQUET_PATH = os.path.join(DATA_DIR, "processed", "youtube_trending")
 
# Create all required folders automatically
os.makedirs(OUTPUT_DIR,                          exist_ok=True)
os.makedirs(JAR_DIR,                             exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "processed"), exist_ok=True)
print(f"  Project folder: {PROJECT_DIR}")
print(f"  Data folder:    {DATA_DIR}")
print(f"  Output folder:  {OUTPUT_DIR}")
 
# Detect Java installation path based on OS
if IS_MAC:
    mac_java_paths = [
        "/opt/homebrew/opt/openjdk@17",
        "/usr/local/opt/openjdk@17",
        "/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home"
    ]
    java_home = next((p for p in mac_java_paths if os.path.exists(p)), None)
elif IS_WINDOWS:
    windows_java_paths = [
        r"C:\Program Files\Eclipse Adoptium\jdk-17.0.9.9-hotspot",
        r"C:\Program Files\Java\jdk-17",
        r"C:\Program Files\Microsoft\jdk-17.0.9.9-hotspot",
        r"C:\Program Files\Eclipse Adoptium\jdk-17.0.11.9-hotspot",
    ]
    java_home = next((p for p in windows_java_paths if os.path.exists(p)), None)
else:
    java_home = os.environ.get("JAVA_HOME", "")
 
if java_home and os.path.exists(java_home):
    os.environ["JAVA_HOME"] = java_home
    os.environ["PATH"] = os.path.join(java_home, "bin") + os.pathsep + os.environ["PATH"]
    print(f"  Java found at: {java_home}")
else:
    print("  ⚠️  Java not found. Please install Java 17:")
    print("      Mac:     brew install openjdk@17")
    print("      Windows: https://adoptium.net")
 
# Download MySQL JDBC JAR automatically if not already present
# This JAR allows Spark to write results directly into MySQL
JAR_NAME = "mysql-connector-java-8.0.33.jar"
JAR_PATH = os.path.join(JAR_DIR, JAR_NAME)
JAR_URL  = "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar"
 
if not os.path.exists(JAR_PATH):
    print(f"  Downloading MySQL JDBC JAR...")
    try:
        urllib.request.urlretrieve(JAR_URL, JAR_PATH)
        size_mb = os.path.getsize(JAR_PATH) / (1024 * 1024)
        print(f"  ✅ JAR downloaded ({size_mb:.1f} MB)")
    except Exception as e:
        print(f"  ❌ JAR download failed: {e}")
        print(f"     Download manually from: {JAR_URL}")
        print(f"     Place in: {JAR_DIR}")
else:
    size_mb = os.path.getsize(JAR_PATH) / (1024 * 1024)
    print(f"  ✅ MySQL JAR found ({size_mb:.1f} MB)")
 
print("Environment ready!")
 
# ── Step 3: MySQL database and tables ────────────────────────
# Connect to MySQL and create the database and all 4 result
# tables if they do not already exist. Uses IF NOT EXISTS so
# it is safe to run multiple times without losing data.
print("\n[Step 3/4] Setting up MySQL database and tables...")
 
MYSQL_HOST     = "localhost"
MYSQL_PORT     = 3306
MYSQL_USER     = os.environ.get("MYSQL_USER", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "root")  # set MYSQL_PASSWORD in your environment
MYSQL_DB       = "youtube_analytics"
 
# Spark JDBC connection URL and properties used throughout the pipeline
MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true"
MYSQL_PROPS = {
    "user":     MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver":   "com.mysql.cj.jdbc.Driver"
}
 
try:
    # Connect to MySQL server without selecting a database first
    connection = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )
    cursor = connection.cursor()
 
    # Create database if it does not exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}")
    cursor.execute(f"USE {MYSQL_DB}")
    print(f"  ✅ Database '{MYSQL_DB}' ready")
 
    # country_stats — stores average views and engagement per country
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS country_stats (
            country        VARCHAR(10),
            avg_views      DOUBLE,
            avg_likes      DOUBLE,
            avg_like_ratio DOUBLE,
            total_videos   INT
        )
    """)
    print("  ✅ Table 'country_stats' ready")
 
    # category_stats — stores average views and engagement per category
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS category_stats (
            category_name  VARCHAR(100),
            avg_views      DOUBLE,
            avg_like_ratio DOUBLE,
            total_videos   INT
        )
    """)
    print("  ✅ Table 'category_stats' ready")
 
    # ml_results — stores Random Forest feature importance scores
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ml_results (
            feature_name     VARCHAR(100),
            importance_score DOUBLE
        )
    """)
    print("  ✅ Table 'ml_results' ready")
 
    # api_trending — stores live trending videos from YouTube API
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS api_trending (
            video_id    VARCHAR(100),
            title       TEXT,
            channel     VARCHAR(255),
            category_id VARCHAR(10),
            region      VARCHAR(10),
            views       BIGINT,
            likes       BIGINT,
            comments    BIGINT
        )
    """)
    print("  ✅ Table 'api_trending' ready")
 
    connection.commit()
    cursor.close()
    connection.close()
    print("MySQL ready!")
 
except pymysql.err.OperationalError as e:
    print(f"  ❌ Cannot connect to MySQL: {e}")
    print("     Make sure MySQL is installed and running:")
    print("     Mac:     brew services start mysql")
    print("     Windows: Start MySQL from the Windows Services panel")
    raise
 
# ── Step 4: Start Spark Session ──────────────────────────────
# Create the Apache Spark session — the entry point for all
# distributed data processing in this pipeline.
# The MySQL JAR is loaded at startup so Spark can write to MySQL.
print("\n[Step 4/4] Starting Apache Spark session...")
 
spark = SparkSession.builder \
    .appName("YouTubeProject") \
    .config("spark.jars", JAR_PATH) \
    .getOrCreate()
 
spark.sparkContext.setLogLevel("WARN")
print(f"  Spark version: {spark.version}")
print("  ✅ Spark session started")
 
print("\n" + "=" * 55)
print(" ✅ All setup complete! Pipeline is ready to run.")
print("=" * 55)
 
 

# %%
# ============================================================
# CELL 2: LOAD RAW CSV FILES INTO SPARK
# Read one CSV file per country and add a "country" column
# so we know which country each row came from after merging.
# inferSchema=False prevents type errors on messy data.
# multiLine=True handles video titles with newline characters.
# ============================================================
 
from pyspark.sql.functions import col, lit
 
countries = ["US", "GB", "DE", "FR", "CA"]
dfs = []
 
for country in countries:
    path = os.path.join(DATA_DIR, f"{country}videos.csv")
 
    if os.path.exists(path):
        # Read CSV safely — all columns come in as text first
        df = spark.read.csv(
            path,
            header=True,
            inferSchema=False,
            escape='"',
            multiLine=True
        )
        # Add country column so rows keep their origin after merging
        df = df.withColumn("country", lit(country))
        dfs.append(df)
        print(f"Loaded {country}: {df.count()} rows")
    else:
        print(f"NOT FOUND: {path}")
        print(f"  → Place {country}videos.csv in {DATA_DIR}")
 
print(f"\nCountries loaded: {len(dfs)}")
 
 

# %%
# ============================================================
# CELL 3: COMBINE AND CLEAN ALL COUNTRY DATA
# Merge all five DataFrames into one large DataFrame.
# Use SQL try_cast to safely convert text to numbers —
# returns NULL instead of crashing on bad values.
# Remove duplicates, null rows, and zero-view rows.
# Add derived feature columns for analysis and ML.
# ============================================================
 
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType
 
# Merge all five country DataFrames into one
combined = dfs[0]
for df in dfs[1:]:
    combined = combined.unionByName(df, allowMissingColumns=True)
 
# Register as a SQL table so we can use SQL queries on it
combined.createOrReplaceTempView("youtube_raw")
 
# Use SQL try_cast to safely convert text columns to numbers
# try_cast returns NULL instead of crashing if a value is not a valid number
combined = spark.sql("""
    SELECT
        video_id,
        trending_date,
        title,
        channel_title,
        category_id,
        publish_time,
        tags,
        try_cast(views         AS BIGINT) AS views,
        try_cast(likes         AS BIGINT) AS likes,
        try_cast(dislikes      AS BIGINT) AS dislikes,
        try_cast(comment_count AS BIGINT) AS comment_count,
        comments_disabled,
        ratings_disabled,
        country
    FROM youtube_raw
""")
 
# Clean data and add calculated feature columns
combined = combined \
    .dropDuplicates(["video_id", "country"]) \
    .filter(col("views").isNotNull()) \
    .filter(col("views") > 0) \
    .filter(col("likes").isNotNull()) \
    .withColumn("like_view_ratio",
        col("likes").cast(DoubleType()) / col("views").cast(DoubleType())) \
    .withColumn("comment_view_ratio",
        col("comment_count").cast(DoubleType()) / col("views").cast(DoubleType())) \
    .withColumn("views_millions",
        col("views").cast(DoubleType()) / 1000000)
 
print(f"Total rows after cleaning: {combined.count()}")
combined.printSchema()
 

# %%
# ============================================================
# CELL 4: SAVE CLEANED DATA AS PARQUET (BLOB STORAGE)
# Parquet is a columnar binary storage format — much faster
# to read back than CSV. Serves as our intermediate blob
# storage layer between ingestion and machine learning.
# ============================================================
 
combined.write.mode("overwrite").parquet(PARQUET_PATH)
print(f"Saved as Parquet to: {PARQUET_PATH}")
 
 

# %%
# ============================================================
# CELL 5: CONVERT TO PANDAS FOR VISUALISATION
# Spark DataFrames are distributed and cannot be plotted.
# We collect into a local Pandas DataFrame for Matplotlib.
# ============================================================
 
import matplotlib
matplotlib.use('Agg')  # Save to file — works without a display screen
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
 
df_pandas = combined.toPandas()
print(f"Converted to pandas: {df_pandas.shape}")
print(df_pandas.head(3))
 

# %%
# ============================================================
# CELL 6: ANALYSIS 1 — VIEWS AND ENGAGEMENT BY COUNTRY
# Group by country, calculate average views and like ratio,
# plot bar charts, write results to MySQL.
# ============================================================
 
country_stats = df_pandas.groupby('country').agg(
    avg_views      = ('views',           'mean'),
    avg_likes      = ('likes',           'mean'),
    avg_like_ratio = ('like_view_ratio', 'mean'),
    total_videos   = ('video_id',        'count')
).reset_index()
 
print(country_stats)
 
fig, axes = plt.subplots(1, 2, figsize=(12, 5))
 
# Left: average views per country in millions
axes[0].bar(country_stats['country'], country_stats['avg_views']/1e6,
            color=['#FF0000','#FF4444','#FF6666','#FF8888','#FFAAAA'])
axes[0].set_title('Average Views per Trending Video by Country (Millions)', fontsize=13)
axes[0].set_xlabel('Country')
axes[0].set_ylabel('Average Views (Millions)')
axes[0].grid(axis='y', alpha=0.3)
 
# Right: average like-to-view ratio per country
axes[1].bar(country_stats['country'], country_stats['avg_like_ratio']*100,
            color=['#333333','#555555','#777777','#999999','#BBBBBB'])
axes[1].set_title('Average Like-to-View Ratio by Country (%)', fontsize=13)
axes[1].set_xlabel('Country')
axes[1].set_ylabel('Like/View Ratio (%)')
axes[1].grid(axis='y', alpha=0.3)
 
plt.tight_layout()
chart1_path = os.path.join(OUTPUT_DIR, 'analysis1_country_stats.png')
plt.savefig(chart1_path, dpi=150, bbox_inches='tight')
plt.show()
print(f"Chart saved to: {chart1_path}")
 
# Write to MySQL
spark.createDataFrame(country_stats).write \
    .jdbc(url=MYSQL_URL, table="country_stats", mode="overwrite", properties=MYSQL_PROPS)
print("country_stats written to MySQL ✅")
 

# %%
# ============================================================
# CELL 7: ANALYSIS 2 — VIEWS AND ENGAGEMENT BY CATEGORY
# Map numeric category IDs to readable names, group by category,
# plot horizontal bar charts, write results to MySQL.
# ============================================================
 
# YouTube uses numeric category IDs — map them to readable names
category_map = {
    '1':  'Film & Animation', '2':  'Autos',           '10': 'Music',
    '15': 'Pets & Animals',   '17': 'Sports',           '19': 'Travel',
    '20': 'Gaming',           '22': 'People & Blogs',  '23': 'Comedy',
    '24': 'Entertainment',    '25': 'News & Politics',  '26': 'Howto & Style',
    '27': 'Education',        '28': 'Science & Tech',   '29': 'Nonprofits'
}
 
df_pandas['category_name'] = df_pandas['category_id'].map(category_map).fillna('Other')
 
category_stats = df_pandas.groupby('category_name').agg(
    avg_views      = ('views',           'mean'),
    avg_like_ratio = ('like_view_ratio', 'mean'),
    total_videos   = ('video_id',        'count')
).reset_index().sort_values('avg_views', ascending=True)
 
print(category_stats[['category_name','avg_views','total_videos']].to_string())
 
fig, axes = plt.subplots(1, 2, figsize=(14, 7))
 
# Left: average views per category in millions
axes[0].barh(category_stats['category_name'], category_stats['avg_views']/1e6, color='#FF0000')
axes[0].set_title('Average Views by Category (Millions)', fontsize=13)
axes[0].set_xlabel('Average Views (Millions)')
axes[0].grid(axis='x', alpha=0.3)
 
# Right: like-to-view ratio per category
category_stats2 = category_stats.sort_values('avg_like_ratio', ascending=True)
axes[1].barh(category_stats2['category_name'], category_stats2['avg_like_ratio']*100, color='#333333')
axes[1].set_title('Average Like-to-View Ratio by Category (%)', fontsize=13)
axes[1].set_xlabel('Like/View Ratio (%)')
axes[1].grid(axis='x', alpha=0.3)
 
plt.tight_layout()
chart2_path = os.path.join(OUTPUT_DIR, 'analysis2_category_stats.png')
plt.savefig(chart2_path, dpi=150, bbox_inches='tight')
plt.show()
print(f"Chart saved to: {chart2_path}")
 
# Write to MySQL
spark.createDataFrame(category_stats).write \
    .jdbc(url=MYSQL_URL, table="category_stats", mode="overwrite", properties=MYSQL_PROPS)
print("category_stats written to MySQL ✅")
 

# %%
# ============================================================
# CELL 8: ANALYSIS 3 — RANDOM FOREST ML MODEL
# Train a binary classifier to predict high vs low viewership.
# Random Forest with 50 trees trained on 80/20 split.
# Feature importance written to MySQL.
# ============================================================
 
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import when, col
 
# Reload from Parquet for ML
spark_df = spark.read.parquet(PARQUET_PATH)
 
# Find the median — videos above = high views (1), below = low views (0)
median_views = spark_df.approxQuantile("views", [0.5], 0.01)[0]
print(f"Median views (classification threshold): {median_views:,.0f}")
 
spark_df = spark_df.withColumn("high_views",
    when(col("views") >= median_views, 1).otherwise(0))
spark_df = spark_df.withColumn("category_id_num", col("category_id").cast("double"))
 
# Combine all features into a single vector column (required by Spark MLlib)
assembler = VectorAssembler(
    inputCols=["likes", "dislikes", "comment_count",
               "like_view_ratio", "comment_view_ratio", "category_id_num"],
    outputCol="features",
    handleInvalid="skip"
)
assembled = assembler.transform(spark_df).select("features", "high_views")
 
# 80% training, 20% test — seed=42 for reproducibility
train, test = assembled.randomSplit([0.8, 0.2], seed=42)
print(f"Training rows: {train.count()}, Test rows: {test.count()}")
 
# Train Random Forest with 50 trees
rf = RandomForestClassifier(labelCol="high_views", featuresCol="features",
                             numTrees=50, seed=42)
model = rf.fit(train)
predictions = model.transform(test)
 
# Evaluate using Area Under ROC Curve (AUC) — 1.0 is perfect
evaluator = BinaryClassificationEvaluator(labelCol="high_views")
auc = evaluator.evaluate(predictions)
print(f"\nModel AUC Score: {auc:.4f}")
 
# Extract feature importance scores
feature_names = ["likes", "dislikes", "comment_count",
                 "like_view_ratio", "comment_view_ratio", "category_id"]
importances = model.featureImportances.toArray()
 
# Plot feature importance (sorted lowest to highest)
fig, ax = plt.subplots(figsize=(10, 5))
sorted_idx = np.argsort(importances)
ax.barh([feature_names[i] for i in sorted_idx],
        [importances[i] for i in sorted_idx], color='#FF0000')
ax.set_title('Feature Importance — Random Forest (Predicting High View Count)', fontsize=13)
ax.set_xlabel('Importance Score')
ax.grid(axis='x', alpha=0.3)
plt.tight_layout()
chart3_path = os.path.join(OUTPUT_DIR, 'analysis3_feature_importance.png')
plt.savefig(chart3_path, dpi=150, bbox_inches='tight')
plt.show()
print(f"Chart saved to: {chart3_path}")
 
# Save feature importance to MySQL
ml_results = pd.DataFrame({
    "feature_name":     feature_names,
    "importance_score": importances
})
spark.createDataFrame(ml_results).write \
    .jdbc(url=MYSQL_URL, table="ml_results", mode="overwrite", properties=MYSQL_PROPS)
print(f"ml_results written to MySQL (AUC: {auc:.4f}) ✅")
 
 

# %%
# ============================================================
# CELL 9: FETCH LIVE TRENDING DATA FROM YOUTUBE API
# Calls YouTube Data API v3 to get currently trending videos
# for US, GB and IN. Saves to CSV and writes to MySQL.
# ============================================================
 
from googleapiclient.discovery import build
 
# YouTube Data API key is read from the environment (never commit keys).
# Set it with:  export YOUTUBE_API_KEY="your-key"   (Windows: setx YOUTUBE_API_KEY "your-key")
API_KEY = os.environ.get("YOUTUBE_API_KEY")
if not API_KEY:
    raise SystemExit("YOUTUBE_API_KEY is not set. See README.md for how to create and set a key.")
youtube  = build("youtube", "v3", developerKey=API_KEY)
 
def get_trending(region="US"):
    """
    Fetch top 50 trending videos for a given region using the YouTube API.
    Returns a list of dictionaries with video metadata and statistics.
    """
    videos  = []
    request = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        chart="mostPopular",
        regionCode=region,
        maxResults=50
    )
    response = request.execute()
    for item in response.get("items", []):
        videos.append({
            "video_id":    item["id"],
            "title":       item["snippet"]["title"],
            "channel":     item["snippet"]["channelTitle"],
            "category_id": item["snippet"]["categoryId"],
            "region":      region,
            "views":       item["statistics"].get("viewCount", 0),
            "likes":       item["statistics"].get("likeCount", 0),
            "comments":    item["statistics"].get("commentCount", 0),
        })
    return videos
 
# Fetch trending videos for US, GB and IN
all_videos = []
for region in ["US", "GB", "IN"]:
    print(f"Fetching {region}...")
    all_videos.extend(get_trending(region))
 
api_df = pd.DataFrame(all_videos)
 
# Save local CSV backup
api_csv_path = os.path.join(DATA_DIR, "api_trending.csv")
api_df.to_csv(api_csv_path, index=False)
print(f"\nCollected {len(api_df)} videos via YouTube API")
print(api_df[['title','region','views']].head(5))
 
# Write to MySQL
spark.createDataFrame(api_df).write \
    .jdbc(url=MYSQL_URL, table="api_trending", mode="overwrite", properties=MYSQL_PROPS)
print("api_trending written to MySQL ✅")
 
 

# %%
# ============================================================
# CELL 10: FOLLOW-UP ANALYSIS — READS DIRECTLY FROM MYSQL
# Reads all 4 result tables back from MySQL using Pandas.
# Produces 3 additional analyses:
#   1. Views vs Engagement tradeoff by category
#   2. Country efficiency score (views + engagement combined)
#   3. Historical (Kaggle) vs Live (YouTube API) comparison
# ============================================================
 
import sqlalchemy
 
# Connect to MySQL using SQLAlchemy + pymysql
engine = sqlalchemy.create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
)
 
# Read all four result tables from MySQL
country_stats  = pd.read_sql("SELECT * FROM country_stats",  engine)
category_stats = pd.read_sql("SELECT * FROM category_stats", engine)
ml_results     = pd.read_sql("SELECT * FROM ml_results",     engine)
api_trending   = pd.read_sql("SELECT * FROM api_trending",   engine)
 
print("✅ Data loaded from MySQL")
print(f"  country_stats:  {len(country_stats)} rows")
print(f"  category_stats: {len(category_stats)} rows")
print(f"  ml_results:     {len(ml_results)} rows")
print(f"  api_trending:   {len(api_trending)} rows")
 
# ── Follow-up 1: Views vs Engagement Tradeoff ─────────────────
# Shows high views does not always mean high engagement.
# Music is the only category that leads in both.
print("\n--- Follow-up 1: Views vs Engagement Tradeoff ---")
 
cat = category_stats[category_stats['category_name'] != 'Other'].copy()
cat = cat.sort_values('avg_views', ascending=False)
 
# Dual-axis chart: bars for views (left), line for engagement (right)
fig, ax1 = plt.subplots(figsize=(14, 6))
x = range(len(cat))
ax1.bar(x, cat['avg_views']/1e6, color='#FF0000', alpha=0.8, label='Avg Views (Millions)')
ax1.set_xlabel('Category', fontsize=12)
ax1.set_ylabel('Average Views (Millions)', fontsize=12, color='#FF0000')
ax1.tick_params(axis='y', labelcolor='#FF0000')
ax1.set_xticks(x)
ax1.set_xticklabels(cat['category_name'], rotation=45, ha='right', fontsize=9)
 
ax2 = ax1.twinx()
ax2.plot(x, cat['avg_like_ratio']*100,
         color='#333333', marker='o', linewidth=2, markersize=6, label='Like/View Ratio (%)')
ax2.set_ylabel('Like/View Ratio (%)', fontsize=12, color='#333333')
ax2.tick_params(axis='y', labelcolor='#333333')
 
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
ax1.set_title('Category Views vs Engagement: High Views ≠ High Engagement', fontsize=13)
ax1.grid(axis='y', alpha=0.3)
 
plt.tight_layout()
f1_path = os.path.join(OUTPUT_DIR, 'followup1_views_vs_engagement.png')
plt.savefig(f1_path, dpi=150, bbox_inches='tight')
plt.show()
print(f"Saved: {f1_path}")
 
top_views  = cat.nlargest(1, 'avg_views')['category_name'].values[0]
top_engage = cat.nlargest(1, 'avg_like_ratio')['category_name'].values[0]
print(f"  → Highest avg views:  {top_views}")
print(f"  → Highest engagement: {top_engage}")
 
# ── Follow-up 2: Country Efficiency Score ────────────────────
# Efficiency = 50% normalised avg views + 50% normalised engagement.
# GB scores 1.0 — most efficient market despite fewest videos.
print("\n--- Follow-up 2: Country Efficiency Analysis ---")
 
country_stats['efficiency_score'] = (
    country_stats['avg_views']      / country_stats['avg_views'].max()      * 0.5 +
    country_stats['avg_like_ratio'] / country_stats['avg_like_ratio'].max() * 0.5
)
country_sorted = country_stats.sort_values('efficiency_score', ascending=False)
 
fig, axes = plt.subplots(1, 3, figsize=(15, 5))
 
axes[0].bar(country_sorted['country'], country_sorted['total_videos']/1000,
            color=['#FF0000','#FF4444','#FF6666','#FF8888','#FFAAAA'])
axes[0].set_title('Total Trending Videos by Country (Thousands)', fontsize=11)
axes[0].set_xlabel('Country')
axes[0].set_ylabel('Videos (Thousands)')
axes[0].grid(axis='y', alpha=0.3)
 
axes[1].bar(country_sorted['country'], country_sorted['avg_views']/1e6,
            color=['#333333','#555555','#777777','#999999','#BBBBBB'])
axes[1].set_title('Average Views per Trending Video (Millions)', fontsize=11)
axes[1].set_xlabel('Country')
axes[1].set_ylabel('Avg Views (Millions)')
axes[1].grid(axis='y', alpha=0.3)
 
axes[2].bar(country_sorted['country'], country_sorted['efficiency_score'],
            color=['#FF0000','#FF4444','#FF6666','#FF8888','#FFAAAA'])
axes[2].set_title('Overall Efficiency Score\n(Views + Engagement Combined)', fontsize=11)
axes[2].set_xlabel('Country')
axes[2].set_ylabel('Efficiency Score (0-1)')
axes[2].grid(axis='y', alpha=0.3)
 
plt.suptitle('Country Efficiency: Fewer Videos, Higher Impact', fontsize=13, y=1.02)
plt.tight_layout()
f2_path = os.path.join(OUTPUT_DIR, 'followup2_country_efficiency.png')
plt.savefig(f2_path, dpi=150, bbox_inches='tight')
plt.show()
print(f"Saved: {f2_path}")
print(country_sorted[['country','total_videos','avg_views','efficiency_score']].to_string(index=False))
 
# ── Follow-up 3: Historical vs Live Trending Comparison ───────
# Compares Kaggle historical data against live YouTube API data.
# Shows which categories have grown or shrunk.
print("\n--- Follow-up 3: Historical vs Live Trending Categories ---")
 
category_map_fu = {
    '1':  'Film & Animation', '2':  'Autos',           '10': 'Music',
    '15': 'Pets & Animals',   '17': 'Sports',           '19': 'Travel',
    '20': 'Gaming',           '22': 'People & Blogs',  '23': 'Comedy',
    '24': 'Entertainment',    '25': 'News & Politics',  '26': 'Howto & Style',
    '27': 'Education',        '28': 'Science & Tech',   '29': 'Nonprofits'
}
 
api_trending['category_name'] = api_trending['category_id'].map(category_map_fu).fillna('Other')
api_trending['views'] = pd.to_numeric(api_trending['views'], errors='coerce').fillna(0)
 
live_cat = api_trending.groupby('category_name').agg(
    live_avg_views = ('views',    'mean'),
    live_count     = ('video_id', 'count')
).reset_index()
 
hist_cat = category_stats[['category_name','avg_views','total_videos']].copy()
hist_cat.columns = ['category_name','hist_avg_views','hist_count']
 
comparison = pd.merge(live_cat, hist_cat, on='category_name', how='inner')
comparison = comparison.sort_values('hist_avg_views', ascending=False).head(10)
 
fig, ax = plt.subplots(figsize=(13, 6))
x     = range(len(comparison))
width = 0.35
 
ax.bar([i - width/2 for i in x], comparison['hist_avg_views']/1e6,
       width, label='Historical Avg Views (Kaggle)', color='#FF0000', alpha=0.8)
ax.bar([i + width/2 for i in x], comparison['live_avg_views']/1e6,
       width, label='Live Avg Views (YouTube API)',  color='#333333', alpha=0.8)
 
ax.set_xlabel('Category', fontsize=12)
ax.set_ylabel('Average Views (Millions)', fontsize=12)
ax.set_title('Historical vs Live Trending: Which Categories Changed?', fontsize=13)
ax.set_xticks(x)
ax.set_xticklabels(comparison['category_name'], rotation=45, ha='right', fontsize=9)
ax.legend()
ax.grid(axis='y', alpha=0.3)
 
plt.tight_layout()
f3_path = os.path.join(OUTPUT_DIR, 'followup3_historical_vs_live.png')
plt.savefig(f3_path, dpi=150, bbox_inches='tight')
plt.show()
print(f"Saved: {f3_path}")
print(comparison[['category_name','hist_avg_views','live_avg_views']].to_string(index=False))
 
print("\n✅ All follow-up analyses complete!")
print(f"All charts saved to: {OUTPUT_DIR}")
 
 

# %%
# ============================================================
# CELL 11: FINAL VERIFICATION
# Lists all 6 output chart files to confirm everything worked.
# ============================================================
 
print("\nOutput files generated:")
for f in sorted(os.listdir(OUTPUT_DIR)):
    full_path = os.path.join(OUTPUT_DIR, f)
    size_kb   = os.path.getsize(full_path) // 1024
    print(f"  {f}  ({size_kb} KB)")
 
print("\n✅ Pipeline complete!")
print(f"All results saved to MySQL database: {MYSQL_DB}")
print(f"All charts saved to: {OUTPUT_DIR}")
 

# %%
