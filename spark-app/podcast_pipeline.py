import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import Imputer, VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from xgboost.spark import SparkXGBRegressor

def feature_engineering(df):
    return (df
        .withColumn("Episode_Num", expr("substring(Episode_Title, 8, 100)"))
        .withColumn("is_weekend", when(col("Publication_Day").isin(["Saturday", "Sunday"]), 1).otherwise(0))
        .withColumn("Publication_Time",
                    when(col("Publication_Time").isNull() | (col("Publication_Time")==""), lit(None))
                    .otherwise(to_timestamp("Publication_Time", 'HH:mm:ss')))
        .withColumn("publication_hour", when(col("Publication_Time").isNull(), lit(-1)).otherwise(hour("Publication_Time")))
        .withColumn("host_guest_ratio", col("Host_Popularity_percentage")/col("Guest_Popularity_percentage"))
        .withColumn("length_category",
                    when(col("Episode_Length_minutes")<15, "short")
                    .when(col("Episode_Length_minutes")<45, "medium")
                    .otherwise("long"))
        .withColumn("ads_per_minute", col("Number_of_Ads")/col("Episode_Length_minutes"))
        .withColumn("sentiment_score",
                    when(col("Episode_Sentiment")=="positive", 1)
                    .when(col("Episode_Sentiment")=="neutral", 0)
                    .otherwise(-1))
        .drop("Episode_Title"))

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PodcastPrediction") \
        .master(os.environ["SPARK_MASTER"]) \
        .config("spark.driver.memory","4g") \
        .config("spark.executor.memory","4g") \
        .config("spark.jars.packages","ml.dmlc:xgboost4j-spark_2.12:1.7.1") \
        .config("spark.sql.shuffle.partitions","1") \
        .getOrCreate()

    # load
    train = spark.read.csv("/data/train.csv", header=True, inferSchema=True).repartition(8)
    test  = spark.read.csv("/data/test.csv",  header=True, inferSchema=True).repartition(8)

    train = feature_engineering(train)
    test  = feature_engineering(test)

    # pipeline
    cat_cols = ['Podcast_Name','Genre','Publication_Day','length_category']
    num_cols = ['Episode_Length_minutes','Host_Popularity_percentage',
                'Guest_Popularity_percentage','Number_of_Ads',
                'host_guest_ratio','ads_per_minute','publication_hour','sentiment_score']

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
        n_estimators=100
    )

    pipeline = Pipeline(stages=[imputer] + indexers + [assembler, xgb])
    model = pipeline.fit(train)
    preds = model.transform(test)

    # save predictions
    (preds.select("id","prediction")
         .withColumnRenamed("prediction","Listening_Time_minutes")
         .coalesce(1)
         .write.parquet("/data/submission.parquet", mode="overwrite"))

    # export viz CSV
    train.select(
        "Podcast_Name","Genre","Episode_Length_minutes",
        "Host_Popularity_percentage","Guest_Popularity_percentage",
        "Publication_Day","publication_hour","length_category",
        "ads_per_minute","sentiment_score","Listening_Time_minutes"
    ).toPandas().to_csv("/data/viz_data.csv", index=False)

    spark.stop()
