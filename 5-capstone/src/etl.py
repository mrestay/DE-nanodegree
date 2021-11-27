import configparser
import os
from itertools import chain
import logging

import findspark
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, FloatType, StructType, StructField

import constants

config = configparser.ConfigParser()
config.read("spark.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]
TESTING = bool(int(config["TEST"]["TEST"]))

logging.basicConfig(level=logging.INFO)


def create_spark_session():
    """
    Initialize a Spark session.

    Returns:
        A Spark session.
    """
    spark = (
        SparkSession.builder.appName("US_Immigration_2016")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.0.0")
        .config("dfs.client.read.shortcircuit.skip.checksum", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", config["AWS"]["AWS_ACCESS_KEY_ID"])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", config["AWS"]["AWS_SECRET_ACCESS_KEY"])
    return spark


def replace_values(df: DataFrame, replace_dict: dict, column_name: str) -> DataFrame:
    """
    Replaces values on a spark dataframe's column with a dictionary.

    Args:
        df: A Spark DataFrame.
        replace_dict: The replacement dictionary.
        column_name: The column name to replace the values in.

    Returns:
        The Dataframe with tehe values replaced.

    """
    mapping = F.create_map([F.lit(x) for x in chain(*replace_dict.items())])
    df = df.withColumn(column_name, mapping[df[column_name]])
    return df


def format_dates(df: DataFrame, column_name: str) -> DataFrame:
    """
    Formats a SAS date. SAS dates are an integer representing the number of days since 1960-01-01.
    Args:
        df: A Spark DataFrame.
        column_name: The column name to replace the values in.

    Returns:
        The DataFrame with the dates parsed as date type.

    """
    df = df.withColumn(column_name, F.expr(f"date_add('1960-01-01', int({column_name}))"))
    return df


def process_immigration_data(spark: SparkSession, raw_data_path: str = None, destination_path: str = None):
    """
    Processes immigration data to create the fact and dimension tables.

    Args:
        spark: Spark session.
        raw_data_path: Route to read the file from.
        destination_path: Route to write the data to.
    """

    data_path = "../data/immigration_data/" if TESTING else f"{raw_data_path}/immigration_data/"
    destination_path = "../data/trusted" if TESTING else destination_path

    immigration_cols_to_keep = {
        "cicid": "immigration_id",
        "admnum": "admission_number",
        "i94port": "port_code",
        "fltno": "flight_number",
        "airline": "airline",
        "i94bir": "age",
        "i94cit": "citizenship_country",
        "i94res": "residency_country",
        "arrdate": "arrival_date",
        "depdate": "departure_date",
        "visapost": "department_state_visa_issued",
        "i94visa": "visa_code",
        "visatype": "visa_type",
        "i94mode": "transportation_mode",
        "occup": "occupation",
        "gender": "gender",
    }

    logging.info("Reading data")
    immigration_df = spark.read.parquet(data_path)
    immigration_df = immigration_df.select(
        *[
            F.col(col_name).alias(immigration_cols_to_keep.get(col_name, col_name))
            for col_name in immigration_cols_to_keep.keys()
        ]
    )
    # Fix coded columns
    logging.info("Replacing coded values in columns")
    immigration_df = replace_values(immigration_df, constants.COUNTRY_CODES, "citizenship_country")
    immigration_df = replace_values(immigration_df, constants.COUNTRY_CODES, "residency_country")
    immigration_df = replace_values(immigration_df, constants.VISA_CODES, "visa_code")
    immigration_df = replace_values(immigration_df, constants.TRANSPORTATION_MODES, "transportation_mode")

    # Format date columns
    logging.info("Formating date columns")
    immigration_df = format_dates(immigration_df, "arrival_date")
    immigration_df = format_dates(immigration_df, "departure_date")

    # Create immigration fact table
    logging.info("Creating immigration fact table")
    immigration_fact_cols = ["immigration_id", "flight_number", "port_code"]
    immigration_fact_df = immigration_df.select(immigration_fact_cols).dropDuplicates()
    immigration_fact_df = immigration_fact_df.withColumn(
        "immigration_id", immigration_fact_df.immigration_id.cast("int")
    )
    immigration_fact_df.write.mode("overwrite").parquet(os.path.join(destination_path, "immigration_fact"))

    # Create flights dimension table
    logging.info("Creating flights dimension table")
    flights_dim_cols = ["flight_number", "airline"]
    flights_dim_df = immigration_df.select(flights_dim_cols).dropDuplicates().where(F.col("flight_number").isNotNull())
    flights_dim_df.write.mode("overwrite").parquet(os.path.join(destination_path, "flights_dim"))

    # Create visitors dimension table
    logging.info("Creating visitors dimension table")
    visitors_dim_cols = ["immigration_id", "residency_country", "citizenship_country", "age", "gender", "occupation"]
    visitors_dim_df = immigration_df.select(visitors_dim_cols).dropDuplicates()
    visitors_dim_df = visitors_dim_df.withColumn("age", visitors_dim_df.age.cast("int")).withColumn(
        "immigration_id", visitors_dim_df.immigration_id.cast("int")
    )
    visitors_dim_df.write.mode("overwrite").parquet(os.path.join(destination_path, "visitors_dim"))

    # Create dates dimension table
    logging.info("Creating dates dimension table")
    dates_dim_cols = ["immigration_id", "arrival_date", "departure_date"]
    dates_dim_df = immigration_df.select(dates_dim_cols).dropDuplicates()
    dates_dim_df = (
        dates_dim_df.withColumn("immigration_id", dates_dim_df.immigration_id.cast("int"))
        .withColumn("arrival_year", F.year(dates_dim_df.arrival_date))
        .withColumn("arrival_month", F.month(dates_dim_df.arrival_date))
        .withColumn("arrival_day", F.dayofmonth(dates_dim_df.arrival_date))
        .withColumn("departure_year", F.year(dates_dim_df.departure_date))
        .withColumn("departure_month", F.month(dates_dim_df.departure_date))
        .withColumn("departure_day", F.dayofmonth(dates_dim_df.departure_date))
    )

    dates_dim_df.write.mode("overwrite").parquet(os.path.join(destination_path, "dates_dim"))

    # Create visas dimension table
    logging.info("Creating visas dimension table")
    visas_dim_cols = ["immigration_id", "visa_type", "visa_code", "department_state_visa_issued"]
    visas_dim_df = immigration_df.select(visas_dim_cols).dropDuplicates()
    visas_dim_df = visas_dim_df.withColumn("immigration_id", visas_dim_df.immigration_id.cast("int"))
    visas_dim_df.write.mode("overwrite").parquet(os.path.join(destination_path, "visas_dim"))


def process_port_codes_data(
    spark: SparkSession, raw_data_path: str = None, destination_path: str = None
) -> pd.DataFrame:
    """
    Processes the port codes data to generate the port_codes dimension table.

    Args:
        spark: Spark session.
        raw_data_path: Route to read the file from.
        destination_path: Route to write the data to.

    Returns:
        A pandas DataFrame with the port_codes data.
    """
    data_path = "../data/i94_port_codes.csv" if TESTING else f"{raw_data_path}/i94_port_codes.csv"
    destination_path = "../data/trusted" if TESTING else destination_path

    port_codes_df = pd.read_csv(data_path)
    logging.info("Creating port codes dimension")
    port_codes_dim = spark.createDataFrame(port_codes_df)
    port_codes_dim.write.mode("overwrite").parquet(os.path.join(destination_path, "port_codes_dim"))

    return port_codes_df


def process_demographics_data(
    spark: SparkSession, port_codes_df: pd.DataFrame, raw_data_path: str = None, destination_path: str = None
) -> None:
    """
    Processes the demographics data to generate the general demographics and the race demographics dimensions.

    Args:
        spark: Spark session.
        port_codes_df: A pandas DataFrame with the port_codes to generate the primary keys of the demographics tables.
        raw_data_path: Route to read the file from.
        destination_path: Route to write the data to
    """
    data_path = "../data/us-cities-demographics.csv" if TESTING else f"{raw_data_path}/us-cities-demographics.csv"
    destination_path = "../data/trusted" if TESTING else destination_path

    race_dict = {
        "American Indian and Alaska Native": "native_count",
        "Asian": "asian_count",
        "Black or African-American": "black_count",
        "Hispanic or Latino": "hispanic_count",
        "White": "white_count",
    }
    # Process general demographics data
    us_cities_df = pd.read_csv(data_path, sep=";")
    us_cities_df.columns = [col.lower().replace(" ", "_").replace("-", "_") for col in us_cities_df.columns]
    demographics_df = us_cities_df[
        [
            "state_code",
            "state",
            "city",
            "median_age",
            "male_population",
            "female_population",
            "total_population",
            "number_of_veterans",
            "foreign_born",
            "average_household_size",
        ]
    ].drop_duplicates(subset="city")
    port_codes_df["city"] = demographics_df["city"].str.lower()
    demographics_df["city"] = demographics_df["city"].str.lower()
    demographics_df = demographics_df.merge(
        port_codes_df[["city", "port_code"]], left_on="city", right_on="city", how="inner"
    )
    demographics_df["city"] = demographics_df["city"].str.title()
    demographics_df = demographics_df[
        [
            "port_code",
            "state_code",
            "state",
            "city",
            "median_age",
            "male_population",
            "female_population",
            "total_population",
            "number_of_veterans",
            "foreign_born",
            "average_household_size",
        ]
    ]

    # Create general demographics dimension table
    logging.info("Creating general demographics dimension")
    general_demographics_dim = spark.createDataFrame(demographics_df)
    general_demographics_dim = (
        general_demographics_dim.withColumn("male_population", general_demographics_dim.male_population.cast("int"))
        .withColumn("female_population", general_demographics_dim.female_population.cast("int"))
        .withColumn("total_population", general_demographics_dim.total_population.cast("int"))
        .withColumn("number_of_veterans", general_demographics_dim.number_of_veterans.cast("int"))
        .withColumn("foreign_born", general_demographics_dim.foreign_born.cast("int"))
    )
    general_demographics_dim.write.mode("overwrite").parquet(os.path.join(destination_path, "general_demog_dim"))

    # Process race demographics data
    race_demographics_df = (
        us_cities_df[["city", "state", "state_code", "race", "count"]]
        .pivot_table("count", ["city", "state", "state_code"], "race")
        .reset_index()
    )

    race_demographics_df = race_demographics_df.drop_duplicates(subset="city")
    race_demographics_df.rename(columns=race_dict, inplace=True)
    race_demographics_df["city"] = race_demographics_df["city"].str.lower()
    race_demographics_df = race_demographics_df.merge(
        port_codes_df[["city", "port_code"]], left_on="city", right_on="city", how="inner"
    )
    race_demographics_df["city"] = race_demographics_df["city"].str.title()
    race_demographics_df = race_demographics_df[
        [
            "port_code",
            "state_code",
            "state",
            "city",
            "native_count",
            "asian_count",
            "black_count",
            "hispanic_count",
            "white_count",
        ]
    ]

    # Create race demographics dimension table
    logging.info("Creating race demographics dimension")
    race_demographics_dim = spark.createDataFrame(race_demographics_df)
    race_demographics_dim = (
        race_demographics_dim.withColumn("native_count", race_demographics_dim.native_count.cast("int"))
        .withColumn("asian_count", race_demographics_dim.asian_count.cast("int"))
        .withColumn("black_count", race_demographics_dim.black_count.cast("int"))
        .withColumn("hispanic_count", race_demographics_dim.hispanic_count.cast("int"))
        .withColumn("white_count", race_demographics_dim.white_count.cast("int"))
    )
    race_demographics_dim.write.mode("overwrite").parquet(os.path.join(destination_path, "race_demog_dim"))


def process_airports_data(
    spark: SparkSession, port_codes_df: pd.DataFrame, raw_data_path: str = None, destination_path: str = None
) -> None:
    """
    Processes the airports data to generate the airports dimension.

    Args:
        spark: Spark session.
        port_codes_df: A pandas DataFrame with the port_codes to generate the primary keys of the demographics tables.
        raw_data_path: Route to read the file from.
        destination_path: Route to write the data to
    """
    data_path = "../data/airport-codes_csv.csv" if TESTING else f"{raw_data_path}/airport-codes_csv.csv"
    destination_path = "../data/trusted" if TESTING else destination_path

    airport_cast_types = {"elevation_ft": "float64", "x_coordinate": "float64", "y_coordinate": "float64"}
    # Process airpots data
    airport_df = pd.read_csv(data_path)
    airport_df = airport_df[(airport_df["iso_country"] == "US")]
    airport_df = airport_df[(airport_df["type"] != "closed")]
    airport_df = airport_df[airport_df["municipality"].notnull()]
    airport_df["state"] = airport_df["iso_region"].str.split("-").str[1]
    airport_df[["x_coordinate", "y_coordinate"]] = airport_df.coordinates.str.split(",", expand=True)
    airport_df = airport_df[
        [
            "local_code",
            "iata_code",
            "type",
            "name",
            "state",
            "municipality",
            "elevation_ft",
            "x_coordinate",
            "y_coordinate",
        ]
    ]
    port_codes_df["city"] = port_codes_df["city"].str.lower()
    airport_df["municipality"] = airport_df["municipality"].str.lower()
    airport_df = airport_df.merge(
        port_codes_df[["city", "port_code"]], left_on="municipality", right_on="city", how="inner"
    )
    airport_df["city"] = airport_df["city"].str.title()

    airport_df = airport_df.astype(airport_cast_types)
    airport_df = airport_df[
        [
            "port_code",
            "local_code",
            "iata_code",
            "type",
            "name",
            "state",
            "city",
            "elevation_ft",
            "x_coordinate",
            "y_coordinate",
        ]
    ]

    # Create airports dimension table
    logging.info("Creating airports dimension table")
    schema = StructType(
        [
            StructField("port_code", StringType(), True),
            StructField("local_code", StringType(), True),
            StructField("iata_code", StringType(), True),
            StructField("type", StringType(), True),
            StructField("name", StringType(), True),
            StructField("state", StringType(), True),
            StructField("city", StringType(), True),
            StructField("elevation_ft", FloatType(), True),
            StructField("x_coordinate", FloatType(), True),
            StructField("y_coordinte", FloatType(), True),
        ]
    )
    airports_dim = spark.createDataFrame(airport_df, schema=schema)
    airports_dim = airports_dim.withColumn("elevation_ft", airports_dim.elevation_ft.cast("int"))

    airports_dim.write.mode("overwrite").parquet(os.path.join(destination_path, "airports_dim"))


def main() -> None:
    """
    Runs the ETL process to generate the tables into the datalake.
    """
    input_data_path = config["AWS"]["ORIGIN_BUCKET"]
    output_data_path = config["AWS"]["DESTINATION_BUCKET"]

    spark = create_spark_session()
    process_immigration_data(spark)
    port_codes_df = process_port_codes_data(spark, raw_data_path=input_data_path, destination_path=output_data_path)

    process_demographics_data(spark, port_codes_df, raw_data_path=input_data_path, destination_path=output_data_path)

    process_airports_data(spark, port_codes_df, raw_data_path=input_data_path, destination_path=output_data_path)


if __name__ == "__main__":
    findspark.init()
    main()
