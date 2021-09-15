# Data lakes with Spark

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data
warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as
a directory with JSON metadata on the songs in their app.

- ```LOG_DATA```: s3a://udacity-dend/log_data
- ```SONG_DATA```: s3a://udacity-dend/song_data

The main goal of the project is to build an ETL pipeline that extracts Sparkify data from S3, processes them using
Spark, and loads the data back into S3 as a set of dimensional tables. This will allow the analytics team to continue
finding insights in what songs their users are listening to.

## Data Sources

There are two JSON sources to retrieve the data from.

**song_data**: JSON files containing song metadata. A sample entry in the JSON file is:

```JSON
{
  "num_songs": 1,
  "artist_id": "ARJIE2Y1187B994AB7",
  "artist_latitude": null,
  "artist_longitude": null,
  "artist_location": "",
  "artist_name": "Line Renaud",
  "song_id": "SOUPIRU12A6D4FA1E1",
  "title": "Der Kleine Dompfaff",
  "duration": 152.92036,
  "year": 0
}

```

**log_data**: JSON files containing song metadata. A sample entry in the JSON file is:

```JSON
{
  "artist": "Sydney Youngblood",
  "auth": "Logged In",
  "firstName": "Jacob",
  "gender": "M",
  "itemInSession": 53,
  "lastName": "Klein",
  "length": 238.07955,
  "level": "paid",
  "location": "Tampa-St. Petersburg-Clearwater, FL",
  "method": "PUT",
  "page": "NextSong",
  "registration": 1.540558e+12,
  "sessionId": 954,
  "song": "Ain't No Sunshine",
  "status": 200,
  "ts": 1543449657796,
  "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4)",
  "userId": 73
}
```

## Data Schema

### Fact Table

- **songplays** (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

### Dimension Tables

- **users**: (user_id, first_name, last_name, gender, leve)
- **songs**: (song_id, title, artist_id, year, duration)
- **artists**: ( artist_id, name, location, lattitude, longitud)
- **time**: (start_time, hour, day, week, month, year, weekday)

## Prerequisites

1. For local development

    - Spark Spark 2.4.7
    - AWS S3 bucket with read and write credentials
    - Python 3.6+
    - Pyspark

2. For deployment to AWS EMR

    - AWS EMR cluster (5.20.0+) with the following specifications:
    - Spark Spark 2.4.7
    - Valid key pair to set up SSH connections with the cluster
    - Valid security group that allows SSH access on port 22
    - AWS S3 bucket with proper access permissinos from AWS EMR

## Running the ETL

1. Create the file `dl.cfg` in the project root with the following information:

```yaml
[ TEST ]
TEST=0

[ AWS ]
AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
DESTINATION_BUCKET=s3a://<your-bucket>/
ORIGIN_BUCKET=s3a://udacity-dend/

[ EMR ]
SPARK_HOME=/usr/lib/spark
PYSPARK_PYTHON=/usr/lib/spark/python
PY4J=/usr/lib/spark/python/lib/py4j-0.10.7-src.zip
SPARK_JARS=org.apache.hadoop:hadoop-aws:3.2.0
```

**Note** that the `TESTING` variable is used to read and write local sample of the data in `<project-root>/data` if set
to `1` or read and write to the corresponding s3 buckets.

3. Run `etl.py` to extract data, transform with Spark and load it to your local filesystem or S3. In the main directory,
   run the following line in the terminal

    ```bash
    python etl.py
    ```