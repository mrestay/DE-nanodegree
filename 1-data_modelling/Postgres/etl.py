import glob
import os
from io import StringIO

import pandas as pd
import psycopg2

from sql_queries import song_select


def process_song_file(cur, filepath: str):
    """
    Reads the song_data JSON files located in the filepath. The function will perform data transformations
    and insert the data into the songs and artists tables

    Args:
        cur (cursor): Database connection cursor
        filepath (str): Path of the json files

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]]

    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_longitude", "artist_latitude"]]

    # stream the data using 'to_csv' and StringIO(); then use sql's 'copy_from' function
    output = StringIO()
    # ignore the index
    artist_data.to_csv(output, sep='\t', header=False, index=False)
    # jump to start of stream
    output.seek(0)

    cur.copy_from(output, 'tmp_artists', null="")
    cur.execute("""
    INSERT INTO artists
    SELECT *
    FROM tmp_artists
    ON CONFLICT (artist_id) DO NOTHING
    """)

    # stream the data using 'to_csv' and StringIO(); then use sql's 'copy_from' function
    output = StringIO()
    # ignore the index
    song_data.to_csv(output, sep='\t', header=False, index=False)
    # jump to start of stream
    output.seek(0)

    cur.copy_from(output, 'tmp_songs', null="")
    cur.execute("""
    INSERT INTO songs
    SELECT *
    FROM tmp_songs
    ON CONFLICT (song_id) DO NOTHING
    """)


def process_log_file(cur, filepath: str):
    """
    Reads the log_data JSON files located in the filepath. The function will perform data transformations
    and insert the data into the time, users and songplays tables

    Args:
        cur (cursor): Database connection cursor
        filepath (str): Path of the json files

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit="ms")

    # insert time data records
    time_data = [[ts, ts.hour, ts.day, ts.week, ts.month, ts.year, ts.day_name()] for ts in t]

    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame.from_records(time_data, columns=column_labels)

    # stream the data using 'to_csv' and StringIO(); then use sql's 'copy_from' function
    output = StringIO()
    # ignore the index
    time_df.to_csv(output, sep='\t', header=False, index=False)
    # jump to start of stream
    output.seek(0)

    cur.copy_from(output, 'tmp_time', null="")
    cur.execute("""
        INSERT INTO time
        SELECT *
        FROM tmp_time
        ON CONFLICT (start_time) DO NOTHING
        """)

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    output = StringIO()
    # ignore the index
    user_df.to_csv(output, sep='\t', header=False, index=False)
    # jump to start of stream
    output.seek(0)

    cur.copy_from(output, 'tmp_users', null="")
    cur.execute("""
            INSERT INTO users
            SELECT *
            FROM tmp_users
            ON CONFLICT (user_id) DO NOTHING
            """)

    songplay_list = []
    # insert songplay records
    for index, row in df.iterrows():

        # get song_id and artist_id from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index,
                         pd.to_datetime(row.ts, unit='ms'),
                         int(row.userId),
                         row.level,
                         songid,
                         artistid,
                         row.sessionId,
                         row.location,
                         row.userAgent.strip('"\'')
                         )

        songplay_list.append(songplay_data)

    songplay_df = pd.DataFrame(songplay_list)
    output = StringIO()
    # ignore the index
    songplay_df.to_csv(output, sep='\t', header=False, index=False)
    # jump to start of stream
    output.seek(0)

    cur.copy_from(output, 'tmp_songplays', null="")
    cur.execute("""
                INSERT INTO songplays
                SELECT *
                FROM tmp_songplays
                ON CONFLICT (songplay_id) DO NOTHING
                """)


def process_data(cur, conn, filepath: str, func: callable):
    """
    Read all JSON files and call either process_song_file function or process_log_file function depending on whether the
    JSON file is song_data file or log_data file
    Args:
        cur (cursor): Database connection cursor
        conn (connection): Database connection
        filepath (str): Path of the json files
        func (callable): Either process_song_file or process_log_file method

    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def create_temporal_tables(cur):
    """
    Create temporal tables for bulk copy into the database
    Args:
        cur (cursor): Database connection cursor

    Returns:
        None
    """

    cur.execute("""
    CREATE TEMP TABLE tmp_songs
    AS
    SELECT * 
    FROM songs
    WITH NO DATA
    """)

    cur.execute("""
    CREATE TEMP TABLE tmp_artists
    AS
    SELECT * 
    FROM artists
    WITH NO DATA
    """)

    cur.execute("""
    CREATE TEMP TABLE tmp_time
    AS
    SELECT * 
    FROM time
    WITH NO DATA
    """)

    cur.execute("""
    CREATE TEMP TABLE tmp_users
    AS
    SELECT * 
    FROM users
    WITH NO DATA
    """)

    cur.execute("""
    CREATE TEMP TABLE tmp_songplays
    AS
    SELECT * 
    FROM songplays
    WITH NO DATA
    """)


def drop_temporal_tables(cur):
    """
    Drop temporal tables from database schema
        Args:
            cur (cursor): Database connection cursor

        Returns:
            None
    """
    cur.execute("DROP TABLE IF EXISTS tmp_users")
    cur.execute("DROP TABLE IF EXISTS tmp_artists")
    cur.execute("DROP TABLE IF EXISTS tmp_songplays")
    cur.execute("DROP TABLE IF EXISTS tmp_songs")
    cur.execute("DROP TABLE IF EXISTS tmp_users")


def main():
    """
    Main method of the ETL pipeline. Generates the database connection and executes the transform and load methods to
    copy the data to the database

    Returns:
        None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    create_temporal_tables(cur)

    print("temporal tables created")

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    drop_temporal_tables(cur)

    print("dropped temporal tables")

    conn.close()


if __name__ == "__main__":
    main()
