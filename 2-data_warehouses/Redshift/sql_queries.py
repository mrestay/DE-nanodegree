import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS app_user CASCADE"
song_table_drop = "DROP TABLE IF EXISTS song CASCADE"
artist_table_drop = "DROP TABLE  IF EXISTS artist CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist_name     VARCHAR,
    auth            VARCHAR,
    first_name      VARCHAR,
    gender          VARCHAR,
    item_in_session INTEGER,
    last_name       VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    VARCHAR,
    session_id      INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    user_agent      VARCHAR,
    user_id         INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs        INTEGER,
    artist_id        VARCHAR,
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         FLOAT,
    year             INTEGER
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id   VARCHAR DISTKEY PRIMARY KEY,
    artist_id VARCHAR NOT NULL,
    title     VARCHAR,
    year      INTEGER,
    duration  FLOAT SORTKEY
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id    INTEGER DISTKEY PRIMARY KEY,
    first_name VARCHAR,
    last_name  VARCHAR,
    gender     VARCHAR,
    level      VARCHAR
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR DISTKEY PRIMARY KEY,
    name      VARCHAR SORTKEY,
    location  VARCHAR,
    latitude  FLOAT,
    longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time TIMESTAMP DISTKEY SORTKEY PRIMARY KEY,
    hour       INTEGER NOT NULL,
    day        INTEGER NOT NULL,
    week       INTEGER NOT NULL,
    month      INTEGER NOT NULL,
    year       INTEGER NOT NULL,
    weekday    INTEGER NOT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id BIGINT IDENTITY (0,1) PRIMARY KEY,
    start_time  TIMESTAMP REFERENCES time SORTKEY NOT NULL,
    user_id     INTEGER REFERENCES users DISTKEY,
    song_id     VARCHAR REFERENCES songs,
    artist_id   VARCHAR REFERENCES artists        NOT NULL,
    session_id  INTEGER                           NOT NULL,
    level       VARCHAR,
    location    VARCHAR,
    user_agent  VARCHAR
)
""")

# STAGING TABLES


staging_events_copy = ("""
COPY staging_events from {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
COMPUPDATE OFF
JSON {}
TIMEFORMAT as 'epochmillisecs'
BLANKSASNULL
TRIMBLANKS
TRUNCATECOLUMNS
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
COPY staging_songs from {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
COMPUPDATE OFF
JSON 'auto'
BLANKSASNULL
TRIMBLANKS
TRUNCATECOLUMNS
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time,
                       user_id,
                       song_id,
                       artist_id,
                       session_id,
                       level,
                       location,
                       user_agent)
SELECT DISTINCT(ts)          as start_time,
               se.user_id    as user_id,
               ss.song_id    as song_id,
               ss.artist_id  as artist_id,
               se.session_id as session_id,
               se.level      as level,
               se.location   as location,
               se.user_agent as user_agent
FROM staging_events se
         JOIN staging_songs ss
              ON ss.artist_name = se.artist_name
                  AND ss.title = se.song
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id,
                   first_name,
                   last_name,
                   gender,
                   level)
SELECT DISTINCT(user_id)  AS user_id,
               first_name AS first_name,
               last_name  AS last_name,
               gender     AS gender,
               level      AS level
FROM staging_events se
WHERE se.user_id IS NOT NULL
  AND page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id,
                   artist_id,
                   title,
                   year,
                   duration)
SELECT DISTINCT(song_id) AS song_id,
               artist_id AS artist_id,
               title     AS title,
               duration  AS duration,
               year      AS year
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,
                     name,
                     location,
                     latitude,
                     longitude)
SELECT DISTINCT(artist_id)      AS artist_id,
               artist_name      AS name,
               artist_location  AS location,
               artist_latitude  AS latitude,
               artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time,
                  hour,
                  day,
                  week,
                  month,
                  year,
                  weekday)
SELECT DISTINCT(ts)                   AS start_time,
               extract(HOUR from ts)  AS hour,
               extract(DAY from ts)   AS day,
               extract(week from ts)  AS week,
               extract(MONTH from ts) AS month,
               extract(YEAR from ts)  AS year,
               extract(DOW from ts)   AS weekday
FROM staging_events
WHERE ts IS NOT NULL
  AND page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create,
                        song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
