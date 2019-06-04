import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS public.staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS public.staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS public.songplays;"
user_table_drop = "DROP TABLE IF EXISTS public.users;"
song_table_drop = "DROP TABLE IF EXISTS public.songs;"
artist_table_drop = "DROP TABLE IF EXISTS public.artists;"
time_table_drop = "DROP TABLE IF EXISTS public.time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist VARCHAR
    ,auth VARCHAR
    ,first_name VARCHAR
    ,gender VARCHAR
    ,item_in_session INT
    ,last_name VARCHAR
    ,length FLOAT
    ,level VARCHAR
    ,location VARCHAR
    ,method VARCHAR
    ,page VARCHAR
    ,registration BIGINT
    ,session_id INT
    ,song VARCHAR
    ,status INT
    ,ts BIGINT
    ,user_agent VARCHAR
    ,user_id VARCHAR
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INT
    ,artist_id VARCHAR
    ,artist_latitude FLOAT
    ,artist_longitude FLOAT
    ,artist_location VARCHAR
    ,artist_name VARCHAR
    ,song_id VARCHAR
    ,title VARCHAR
    ,duration FLOAT
    ,year INT
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id BIGINT IDENTITY(0, 1) NOT NULL PRIMARY KEY
    ,start_time TIMESTAMP NOT NULL
    ,user_id VARCHAR NOT NULL
    ,level VARCHAR
    ,song_id VARCHAR NOT NULL
    ,artist_id VARCHAR NOT NULL
    ,session_id INT
    ,lcation VARCHAR
    ,user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id VARCHAR NOT NULL PRIMARY KEY
    ,first_name VARCHAR
    ,last_name VARCHAR
    ,gender VARCHAR
    ,level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR NOT NULL PRIMARY KEY
    ,title VARCHAR
    ,artist_id VARCHAR NOT NULL
    ,year INT
    ,duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR NOT NULL PRIMARY KEY
    ,name VARCHAR
    ,location VARCHAR
    ,latitude FLOAT
    ,longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY
    ,hour INT
    ,day INT
    ,week INT
    ,month INT
    ,year INT
    ,weekday INT
);
""")


# STAGING TABLES

staging_events_copy = ("""
COPY public.staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON {}
REGION 'us-west-2';
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH')
)   

staging_songs_copy = ("""
COPY public.staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN')
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time
    ,user_id
    ,level
    ,song_id
    ,artist_id
    ,session_id
    ,lcation
    ,user_agent
)
SELECT DISTINCT
    TIMESTAMP 'epoch' + e.ts * INTERVAL '1 second' AS start_time
    ,e.user_id AS user_id
    ,e.level AS level
    ,s.song_id AS song_id
    ,a.artist_id AS artist_id
    ,e.session_id AS session_id
    ,e.location AS location
    ,e.user_agent AS user_agent
FROM
    public.staging_events e
    JOIN public.songs s ON (e.song = s.title)
    JOIN public.artists a ON (e.artist = a.name)
WHERE
    e.page = 'NextSong'
;
""")

user_table_insert = ("""
INSERT INTO users (
    user_id
    ,first_name
    ,last_name
    ,gender
    ,level
)
SELECT DISTINCT
    user_id
    ,first_name
    ,last_name
    ,gender
    ,level
FROM
    public.staging_events
WHERE
    page = 'NextSong'
;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id
    ,title
    ,artist_id
    ,year
    ,duration
)
SELECT DISTINCT
    song_id
    ,title
    ,artist_id
    ,year
    ,duration
FROM
	public.staging_songs
;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id
    ,name
    ,location
    ,latitude
    ,longitude
)
SELECT DISTINCT
    artist_id
    ,artist_name AS name
    ,artist_location AS location
    ,artist_latitude AS latitude
    ,artist_longitude AS logitude
FROM
    public.staging_songs
;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time
    ,hour
    ,day
    ,week
    ,month
    ,year
    ,weekday
)
SELECT
	start_time AS start_time
    ,EXTRACT(hour FROM start_time) AS hour
    ,EXTRACT(day FROM start_time) AS day
    ,EXTRACT(week FROM start_time) AS week
    ,EXTRACT(month FROM start_time) AS month
    ,EXTRACT(year FROM start_time) AS year
    ,EXTRACT(weekday FROM start_time) AS weekday
FROM
	public.songplays
;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]
