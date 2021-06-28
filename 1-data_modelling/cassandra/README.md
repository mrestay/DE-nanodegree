# Data Modeling with Cassandra

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new
music streaming app. The analysis team is particularly interested in understanding what songs users are listening to.
Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV
files on user activity on the app.

They'd like to create an Apache Cassandra analytics database which they can query on song play data to answer the
business questions.

## Data Sources

The analytics team has several `csv` files with the data on a local directory. The data looks as follows:

```csv
"artist","firstName","gender","itemInSession","lastName","length","level","location","sessionId","song","userId"
"Harmonia","Ryan","M","0","Smith","655.77751","free","San Jose-Sunnyvale-Santa Clara, CA","583","Sehr kosmisch","26"
```

## Database Design

For this porject the analytics team wants to answer the following 3 questions:

1. Find the artist, song title and song's length in the music app history that was heard during `sessionId = 338`, and
   `itemInSession = 4`
2. Find name of artist, song (sorted by itemInSession) and user (first and last name) for `userid = 10`
   , `sessionid = 182`
3. Find every username (first and last) in my music app history who listened to the song 'All Hands Against His Own'

To answer this questions three tables were created in the database:

**sessions** table to answer question 1

* **sessionId** (int) Primary Key: ID for each session
* **itemInSession** (int) Primary Key: Item id in each session
* **artist** (text): Artist name
* **song** (text): Song name
* **length** (float): Song's length

**user_sessions** table to answer question 2

* **userId** (int) Primary Key: ID for each user
* **sessionId** (int) Primary Ke: ID for each session
* **itemInSession** (int) Clustering key: Item id in each session
* **artist** (text): Artist name
* **song** (text): Song name
* **firstName** (text): User's first name
* **lastName** (text): User's last name

**songs** table to answer question 2

* **song** (text) Primary Key: ID for each songplay
* **userId** (int) Primary Key: Start time of songplay session
* **firstName** (text): User's first name
* **lastName** (text): User's last name

## Queries

Query to answer question 1:

```SQL
SELECT artist, song, length
FROM sessions
WHERE sessionId = 338
  AND itemInSession = 4
```

Query to answer question 2:

```SQL
SELECT artist, song, firstName, lastName
FROM user_sessions
WHERE userId = 10
  and sessionId = 182
```

Query to answer question 3:

```SQL
SELECT firstName, LastName
FROM songs
WHERE song = 'All Hands Against His Own' 
ALLOW FILTERING
```

## Author

* **Mateo Restrepo** - [@mrestay](https://github.com/mrestay)

## Acknowledgments

* Hat tip to anyone whose code was used!