import configparser
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
TESTING = bool(int(config['TEST']['TEST']))


def create_spark_session():
    """
    Function to create a spark session.

    Returns:
        The spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    return spark


def process_song_data(spark, raw_data_path, destination_path):
    """
    Function to process the song JSON data to create the songs and artists tables.

    Args:
        spark: Spark session.
        raw_data_path: Route to read the file from.
        destination_path: Route to write the data to.
    """

    song_data_path = "data/song_data/" if TESTING else f"{raw_data_path}/song_data/"
    destination_path = "data" if TESTING else destination_path

    song_data = os.path.join(song_data_path, '*/*/*/*.json')

    df = spark.read.json(song_data)

    logging.info("creating songs table")

    songs_table = df.select("song_id",
                            "title",
                            "artist_id",
                            "year",
                            "duration").dropDuplicates(["song_id"])

    songs_table.repartition("year", "artist_id").write.mode("overwrite") \
        .partitionBy("year", "artist_id").parquet(os.path.join(destination_path, "songs"))

    logging.info("creating artists table")

    artists_table = df.select("artist_id",
                              "artist_name",
                              "artist_location",
                              "artist_latitude",
                              "artist_longitude").dropDuplicates(["artist_id"])

    artists_table.repartition("artist_id").write.mode("overwrite") \
        .partitionBy("artist_id").parquet(os.path.join(destination_path, "artists"))


def process_log_data(spark, raw_data_path, destination_path):
    """
    Function to process the event logs JSON data to create the songs and artists tables.

    Args:
        spark: Spark session.
        raw_data_path: Route to read the file from.
        destination_path: Route to write the data to.
    """
    log_data_path = "data/log-data/" if TESTING else f"{raw_data_path}/log_data/"
    destination_path = "data" if TESTING else destination_path

    df = spark.read.json(log_data_path)

    df = df.filter(df.page == "NextSong")

    logging.info("creating users table")

    users_table = df.select("userId", "firstName", "lastName", "gender", "level") \
        .dropDuplicates(["userId"]) \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("firstName", "first_name") \
        .withColumnRenamed("lastName", "last_name")

    users_table.repartition("user_id").write.mode("overwrite") \
        .partitionBy("user_id").parquet(os.path.join(destination_path, "users"))

    logging.info("creating time table")

    df = df.withColumn("timestamp", (df.ts / 1000).cast("timestamp"))

    df = df.withColumn("month", F.month(df.timestamp))
    df = df.withColumn("year", F.year(df.timestamp))
    df = df.withColumn("day", F.dayofmonth(df.timestamp))
    df = df.withColumn("hour", F.hour(df.timestamp))
    df = df.withColumn("week", F.weekofyear(df.timestamp))
    df = df.withColumn("weekday", F.date_format(df.timestamp, "E"))

    time_table = df.select(["timestamp", "month", "year", "day", "hour", "week", "weekday"]) \
        .dropDuplicates(["timestamp"])

    time_table.repartition("year", "month").write.mode("overwrite") \
        .partitionBy("year", "month").parquet(os.path.join(destination_path, "time"))

    song_data = "data/song_data/"
    song_df = spark.read.option("recursiveFileLookup", "true").json(song_data)

    logging.info("creating song plays table")

    songplays_table = df.join(song_df, [df.song == song_df.title, df.artist == song_df.artist_name])

    songplays_table = songplays_table.select([
        F.monotonically_increasing_id().alias('songplay_id'),
        F.col('timestamp').alias("start_time"),
        F.year('timestamp').alias('year'),
        F.month('timestamp').alias('month'),
        F.col("userId").alias("user_id"),
        "level",
        "song_id",
        "artist_id",
        F.col('sessionId').alias("session_id"),
        "location",
        F.col('userAgent').alias("user_agent"),
    ])

    songplays_table.repartition("year", "month").write.mode("overwrite") \
        .partitionBy("year", "month").parquet(os.path.join(destination_path, "songplays"))


def main():
    spark = create_spark_session()
    input_data = config['AWS']['ORIGIN_BUCKET']
    output_data = config['AWS']['DESTINATION_BUCKET']

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
