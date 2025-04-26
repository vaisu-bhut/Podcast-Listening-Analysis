from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import Imputer, VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from xgboost.spark import SparkXGBRegressor
import plotly.express as px
import plotly.subplots as sp

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PodcastPrediction") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.jars.packages", "ml.dmlc:xgboost4j-spark_2.12:1.7.1") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()

# Load data from mounted volume
train = spark.read.csv("/data/train.csv", header=True, inferSchema=True).repartition(8)
test = spark.read.csv("/data/test.csv", header=True, inferSchema=True).repartition(8)
original = spark.read.csv("/data/podcast_dataset.csv", header=True, inferSchema=True).repartition(8)

# Feature engineering
def feature_engineering(df):
    return (df
            .withColumn("Episode_Num", expr("substring(Episode_Title, 8, 100)"))
            .withColumn("is_weekend", when(col("Publication_Day").isin(["Saturday", "Sunday"]), 1).otherwise(0))
            .withColumn("Publication_Time", when(col("Publication_Time").isNull() | (col("Publication_Time") == ""), lit(None)).otherwise(to_timestamp("Publication_Time", 'HH:mm:ss')))
            .withColumn("publication_hour", when(col("Publication_Time").isNull(), lit(-1)).otherwise(hour(col("Publication_Time"))))
            .withColumn("host_guest_ratio", 
                       col("Host_Popularity_percentage")/col("Guest_Popularity_percentage"))
            .withColumn("length_category",
                       when(col("Episode_Length_minutes") < 15, "short")
                        .when(col("Episode_Length_minutes") < 45, "medium")
                        .otherwise("long"))
            .withColumn("ads_per_minute",
                       col("Number_of_Ads")/col("Episode_Length_minutes"))
            .withColumn("sentiment_score",
                       when(col("Episode_Sentiment") == "positive", 1)
                        .when(col("Episode_Sentiment") == "neutral", 0)
                        .otherwise(-1))
            .drop("Episode_Title"))

train = feature_engineering(train)
test = feature_engineering(test)

# Preprocessing pipeline
cat_cols = ['Podcast_Name', 'Genre', 'Publication_Day', 'length_category'] 
num_cols = ['Episode_Length_minutes', 'Host_Popularity_percentage', 
           'Guest_Popularity_percentage', 'Number_of_Ads', 'host_guest_ratio',
           'ads_per_minute', 'publication_hour', 'sentiment_score']

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
imputer = Imputer(inputCols=num_cols, outputCols=num_cols, strategy="median")
assembler = VectorAssembler(inputCols=num_cols + [f"{c}_idx" for c in cat_cols], outputCol="features")

xgb = SparkXGBRegressor(
    features_col="features",
    label_col="Listening_Time_minutes",
    num_workers=2,
    max_depth=8,
    subsample=0.8,
    learning_rate=0.05,
    n_estimators=100,
    early_stopping_rounds=None
)

pipeline = Pipeline(stages=[imputer] + indexers + [assembler, xgb])

# Train model
model = pipeline.fit(train)
predictions = model.transform(test)

# Save predictions
(predictions.select("id", "prediction")
    .withColumnRenamed("prediction", "Listening_Time_minutes")
    .coalesce(1)
    .write
    .parquet("/data/submission.parquet", mode="overwrite"))

# Export data for BI tools
train_for_viz = train.select(
    "Podcast_Name", "Genre", "Episode_Length_minutes",
    "Host_Popularity_percentage", "Guest_Popularity_percentage",
    "Publication_Day", "publication_hour", "length_category",
    "ads_per_minute", "sentiment_score", "Listening_Time_minutes"
)
train_for_viz.toPandas().to_csv("/data/viz_data.csv", index=False)

# Comprehensive visualizations
# 2.1 Key Distribution Plots
fig_dist = sp.make_subplots(rows=2, cols=2, subplot_titles=(
    "Listening Time Distribution", 
    "Episode Length vs Listening Time",
    "Popularity vs Engagement",
    "Sentiment Impact"
))
hist = px.histogram(train_for_viz.toPandas(), x="Listening_Time_minutes")
fig_dist.add_trace(hist.data[0], row=1, col=1)
scatter = px.scatter(train_for_viz.toPandas(), 
                    x="Episode_Length_minutes", 
                    y="Listening_Time_minutes",
                    trendline="lowess")
fig_dist.add_trace(scatter.data[0], row=1, col=2)
scatter2 = px.scatter(train_for_viz.toPandas(),
                     x="Host_Popularity_percentage",
                     y="Listening_Time_minutes",
                     color="Guest_Popularity_percentage")
fig_dist.add_trace(scatter2.data[0], row=2, col=1)
box = px.box(train_for_viz.toPandas(), 
            x="sentiment_score", 
            y="Listening_Time_minutes")
fig_dist.add_trace(box.data[0], row=2, col=2)
fig_dist.update_layout(height=800, showlegend=False)
fig_dist.write_html("/data/distribution.html")

# 2.2 Time Analysis Dashboard
time_analysis = train_for_viz.groupBy("Publication_Day", "publication_hour").agg(
    avg("Listening_Time_minutes").alias("avg_listening"),
    count("*").alias("episode_count")
).toPandas()
fig_time = sp.make_subplots(
    rows=1, cols=2,
    specs=[[{"type": "bar"}, {"type": "heatmap"}]],
    subplot_titles=("Episodes by Day", "Listening Heatmap")
)
bar = px.bar(time_analysis, x="Publication_Day", y="episode_count")
fig_time.add_trace(bar.data[0], row=1, col=1)
heatmap = px.density_heatmap(
    time_analysis,
    x="Publication_Day",
    y="publication_hour",
    z="avg_listening",
    histfunc="avg"
)
fig_time.add_trace(heatmap.data[0], row=1, col=2)
fig_time.update_layout(height=500)
fig_time.write_html("/data/time_analysis.html")

# 3.1 Correlation Matrix
corr_matrix = train_for_viz.toPandas()[['Listening_Time_minutes', 'Episode_Length_minutes',
    'Host_Popularity_percentage', 'Guest_Popularity_percentage', 'ads_per_minute', 'sentiment_score']].corr()
fig_corr = px.imshow(corr_matrix, labels=dict(color="Correlation"), x=corr_matrix.columns, y=corr_matrix.columns, color_continuous_scale='RdBu')
fig_corr.update_layout(title="Feature Correlation Matrix")
fig_corr.write_html("/data/correlation.html")

# 3.2 Genre Deep Dive
genre_analysis = train_for_viz.groupBy("Genre").agg(
    avg("Listening_Time_minutes").alias("avg_time"),
    avg("Host_Popularity_percentage").alias("avg_host_popularity"),
    stddev("Listening_Time_minutes").alias("time_variability")
).toPandas()
fig_genre = px.scatter(genre_analysis, x="avg_host_popularity", y="avg_time", size="time_variability", color="Genre", hover_name="Genre", title="Genre Performance Analysis")
fig_genre.write_html("/data/genre_analysis.html")

# Cleanup
spark.stop()