# Data Modeling with PostgreSQL

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new
music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.

Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity
on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like to create a Postgres database with tables designed to optimize queries on song play analysis. The objective
is to create a database schema and ETL pipeline for this analysis.

## Data Sources

There are two JSON sources to retrieve the data from.

**song_data**: JSON files containing song metadata. An sample entry in the JSON file is:

```JSON
{
  "artist_id": "AR8IEZO1187B99055E",
  "artist_latitude": NaN,
  "artist_location":,
  "artist_longitude": NaN,
  "artist_name": "Marc Shaiman",
  "duration": 149.86404,
  "num_songs": 1,
  "song_id": "SOINLJW12A8C13314C",
  "title": "City Slickers",
  "year": 2008
}
```

**log_data**: JSON files containing song metadata. An sample entry in the JSON file is:

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

## Database Design

Using the song and log datasets, a star schema was imlemented optimized for queries on song play analysis. This includes
the following tables.

### Fact tables

**songplays** records in log data associated with song plays

* **songplay_id** (int) Primary Key: ID for each songplay
* **start_time** (timestamp): Start time of songplay session
* **user_id** (int): User's ID
* **level** (varchar): User's membership level
* **song_id** (varchar): Song's ID
* **artist_id** (varchar): Artist's ID
* **session_id** (int): ID of user's current session
* **location** (varchar): User's location
* **user_agent** (varchar): User's software agent

### Dimension Tables

**users** user data from the app

* **user_id** (int): ID of user
* **first_name** (varchar): User's first name
* **last_name** (varchar): User's last name
* **gender** (varchar): User's gender
* **level** (varchar): User membership level

**songs** - songs data

* **song_id** (varchar): Song's ID
* **title** (varchar): Song's title
* **artist_id** (varchar): Artist's ID
* **year** (int): Year of song release
* **duration** (float): Duration of song

**artists** - artists data

* **artist_id** (varchar): Artist's ID
* **name** (varchar) : Artist's name
* **location** (varchar): Artist's location
* **latitude** (float): Latitude of artist's location
* **longitude** (float): Longitude of artist's location

**time** - timestamps of records in songplay logs

* **start_time** (timestamp): Starting timestamp for songplay event
* **hour** (int): The hour of the songplay's start
* **day** (int): The day of the songplay's start
* **week** (int): The week of the songplay's start
* **month** (int): The month of the songplay's start
* **year** (int): The year of the songplay's start
* **weekday** (int): The day of the week of the songplay's start

## ETL

1. Run `create_tables.py` to create the database and tables. In the main directory, run the following line in the
   terminal

    ```bash
    python create_tables.py
    ```

2. Run `etl.py` to insert data into the tables.In the main directory, run the following line in the terminal

    ```bash
    python etl.py
    ```

3. Run `test.ipynb` to confirm the creation of your tables with the correct columns. Make sure to click "Restart kernel"
   to close the connection to the database after running this notebook.

## Example queries

1. **Songplays table**

   ```PostgreSQL
   SELECT * FROM songplays Limit 1
   ```

| user\_id | level | song\_id | artist\_id | session\_id | location | user\_agent |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 69 | free | NULL | NULL | 455 | Philadelphia-Camden-Wilmington, PA-NJ-DE-MD | Mozilla/5.0 \(Macintosh; Intel Mac OS X 10\_9\_4\) AppleWebKit/537.36 \(KHTML, like Gecko\) Chrome/36.0.1985.125 Safari/537.36 |

2. **Songs table**

   ```PostgreSQL
   SELECT *
   FROM songs
   Limit 1
   ```

| song\_id | title | artist\_id | year | duration |
| :--- | :--- | :--- | :--- | :--- |
| SONHOTT12A8C13493C | Something Girls | AR7G5I41187FB4CE6C | 1982 | 233.40363 |

3. **Artists table**

```PostgreSQL
   SELECT *
   FROM artists
   Limit 1
```

| artist\_id | name | location | latitude | longitude |
| :--- | :--- | :--- | :--- | :--- |
| ARMJAGH1187FB546F3 | The Box Tops | Memphis, TN | -90.04892 | 35.14968 |

## Author

* **Mateo Restrepo** - [@mrestay](https://github.com/mrestay)

## Acknowledgments

* Hat tip to anyone whose code was used!