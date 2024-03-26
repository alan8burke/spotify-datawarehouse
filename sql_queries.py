import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
        CREATE TABLE IF NOT EXISTS staging_events (
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender CHAR,
            itemInSession INT,
            lastName VARCHAR,
            length NUMERIC,
            level VARCHAR,
            location VARCHAR,
            method VARCHAR,
            page VARCHAR,
            registration VARCHAR,
            sessionId BIGINT,
            song VARCHAR,
            status INT,
            ts TIMESTAMP,
            userAgent VARCHAR,
            userId BIGINT
        )
"""

staging_songs_table_create = """
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs INT,
            artist_id VARCHAR,
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location VARCHAR,
            artist_name VARCHAR,
            song_id VARCHAR,
            title VARCHAR,
            duration NUMERIC,
            year INT
        )
"""

songplay_table_create = """
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id BIGINT IDENTITY (0,1) PRIMARY KEY,
            start_time TIMESTAMP NOT NULL REFERENCES time,
            user_id BIGINT NOT NULL REFERENCES users,
            level VARCHAR (4) NOT NULL,
            song_id VARCHAR REFERENCES songs,
            artist_id VARCHAR REFERENCES artists,
            session_id INT,
            location VARCHAR,
            user_agent VARCHAR);
"""

user_table_create = """
        CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY,
                                          first_name VARCHAR NOT NULL,
                                          last_name VARCHAR NOT NULL,
                                          gender CHAR NOT NULL,
                                          level VARCHAR (4) NOT NULL);
"""

song_table_create = """
        CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY,
                                          title VARCHAR NOT NULL,
                                          artist_id VARCHAR,
                                          year INT,
                                          duration NUMERIC NOT NULL);
"""

artist_table_create = """
        CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY,
                                            name VARCHAR NOT NULL,
                                            location VARCHAR,
                                            latitude FLOAT,
                                            longitude FLOAT);
"""

time_table_create = """
        CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY,
                                         hour INT,
                                         day INT,
                                         week INT,
                                         month INT,
                                         year INT,
                                         weekday INT);
"""

# STAGING TABLES

staging_events_copy = (
    """
        COPY staging_events FROM {}
        credentials 'aws_iam_role={}'
        JSON {} compupdate off
        region 'us-west-2'
        timeformat as 'epochmillisecs';
"""
).format(
    config.get("S3", "LOG_DATA"),
    config.get("IAM_ROLE", "ARN"),
    config.get("S3", "LOG_JSONPATH"),
)

staging_songs_copy = (
    """
        COPY staging_songs FROM {}
        credentials 'aws_iam_role={}'
        JSON 'auto' truncatecolumns compupdate off
        region 'us-west-2';
"""
).format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = """
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id,
                               session_id, location, user_agent)
        SELECT e.ts AS start_time,
               e.userId AS user_id,
               e.level,
               s.song_id AS song_id,
               s.artist_id AS artist_id,
               e.sessionId AS session_id,
               e.location,
               e.userAgent AS user_agent
        FROM staging_events e
        JOIN staging_songs s ON e.artist = s.artist_name
        AND e.length = s.duration
        AND e.song = s.title
"""

user_table_insert = """
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT userId AS user_id,
               firstName AS first_name,
               lastName AS last_name,
               gender,
               level
        FROM staging_events
        WHERE page = 'NextSong'
        AND user_id IS NOT null
        AND ts = (SELECT max(ts) FROM staging_events WHERE user_id = userId);
"""

song_table_insert = """
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id,
               title,
               artist_id,
               year,
               duration
        FROM staging_songs
"""

artist_table_insert = """
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id,
               artist_name AS name,
               artist_location AS location,
               artist_latitude AS latitude,
               artist_longitude AS longitude
        FROM staging_songs
"""

time_table_insert = """
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT ts AS start_time,
               EXTRACT(hour FROM start_time) AS hour,
               EXTRACT(day FROM start_time) AS day,
               EXTRACT(week FROM start_time) AS week,
               EXTRACT(month FROM start_time) AS month,
               EXTRACT(year FROM start_time) AS year,
               EXTRACT(DOW FROM start_time) AS weekday
        FROM staging_events
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
