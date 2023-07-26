import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
     artist VARCHAR,
     auth VARCHAR,
     firstName VARCHAR,
     gender CHAR(1),
     itemInSession INTEGER,
     lastName VARCHAR,
     length FLOAT,
     level VARCHAR,
     location VARCHAR,
     method VARCHAR,
     page VARCHAR,
     registration FLOAT,
     sessionId INTEGER, 
     song VARCHAR,
     status INTEGER,
     ts BIGINT,
     userAgent VARCHAR,
     userId INTEGER
);                              
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id INTEGER IDENTITY (1, 1) SORTKEY PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INTEGER REFERENCES users(user_id) DISTKEY,
    level VARCHAR,
    song_id VARCHAR REFERENCES songs(song_id),
    artist_id VARCHAR REFERENCES artists(artist_id),
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR
) DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(   
    song_id VARCHAR SORTKEY PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration FLOAT
) DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(   
    artist_id VARCHAR SORTKEY PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
) DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(   
    start_time TIMESTAMP SORTKEY PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
) DISTSTYLE ALL;
""")

# STAGING TABLES

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


staging_events_copy = ("""
    COPY staging_events
    FROM {}
    IAM_ROLE {}
    FORMAT AS JSON {}
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    IAM_ROLE {}
    FORMAT AS JSON 'auto'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
        se.userId                   AS user_id,
        se.level                    AS level,
        ss.song_id                  AS song_id,
        ss.artist_id                AS artist_id,
        se.sessionId                AS session_id,
        se.location                 AS location,
        se.userAgent                AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss ON (se.song = ss.title AND se.artist = ss.artist_name)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id,
        first_name,
        last_name,
        gender,
        level)
    SELECT DISTINCT se.userId        AS user_id,
        se.firstName                 AS first_name,
        se.lastName                  AS last_name,
        se.gender                    AS gender,
        se.level                     AS level
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id,
        title,
        artist_id,
        year,
        duration)
    SELECT  DISTINCT ss.song_id          AS song_id,
            ss.title                     AS title,
            ss.artist_id                 AS artist_id,
            ss.year                      AS year,
            ss.duration                  AS duration
    FROM staging_songs AS ss;
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude)
    SELECT DISTINCT ss.artist_id                AS artist_id,
            ss.artist_name                      AS name,
            ss.artist_location                  AS location,
            ss.artist_latitude                  AS latitude,
            ss.artist_longitude                 AS longitude
    FROM staging_songs AS ss;
""")

time_table_insert = ("""
    INSERT INTO time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
           EXTRACT(hour FROM start_time)    AS hour,
           EXTRACT(day FROM start_time)     AS day,
           EXTRACT(week FROM start_time)    AS week,
           EXTRACT(month FROM start_time)   AS month,
           EXTRACT(year FROM start_time)    AS year,
           EXTRACT(week FROM start_time)    AS weekday
   FROM staging_events AS se
   WHERE se.page = 'NextSong';
""")


# GET NUMBER OF ROWS IN EACH TABLE
get_count_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_count_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_count_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

get_count_users = ("""
    SELECT COUNT(*) FROM users
""")

get_count_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_count_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_count_time = ("""
    SELECT COUNT(*) FROM time
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, songplay_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
select_rows_count_queries= [get_count_staging_events, get_count_staging_songs, get_count_songplays, get_count_users, get_count_songs, get_count_artists, get_count_time]