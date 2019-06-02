import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist VARCHAR
    ,auth VARCHAR
    ,first_name VARCHAR
    ,gender VARCHAR
    ,item_in_session VARCHAR
    ,last_name VARCHAR
    ,length VARCHAR
    ,level VARCHAR
    ,location VARCHAR
    ,method VARCHAR
    ,page VARCHAR
    ,registration VARCHAR
    ,session_id VARCHAR
    ,song VARCHAR
    ,status VARCHAR
    ,ts VARCHAR
    ,user_agent VARCHAR
    ,user_id VARCHAR
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs VARCHAR
    ,artist_id VARCHAR
    ,artist_latitude VARCHAR
    ,artist_longitude VARCHAR
    ,artist_location VARCHAR
    ,artist_name VARCHAR
    ,song_id VARCHAR
    ,title VARCHAR
    ,duration VARCHAR
    ,year VARCHAR
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id BIGINT IDENTITY(0, 1) NOT NULL PRIMARY KEY
    ,start_time BIGINT NOT NULL
    ,user_id VARCHAR NOT NULL
    ,level VARCHAR NOT NULL
    ,song_id VARCHAR NOT NULL
    ,artist_id VARCHAR NOT NULL
    ,session_id VARCHAR NOT NULL
    ,lcation VARCHAR
    ,user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id VARCHAR NOT NULL PRIMARY KEY
    ,first_name VARCHAR NOT NULL
    ,last_name VARCHAR NOT NULL
    ,gender VARCHAR
    ,level VARCHAR NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR NOT NULL PRIMARY KEY
    ,title VARCHAR NOT NULL
    ,artist_id VARCHAR NOT NULL
    ,year INT
    ,duration FLOAT NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR NOT NULL PRIMARY KEY
    ,name VARCHAR NOT NULL
    ,location VARCHAR
    ,latitude FLOAT
    ,longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time BIGINT NOT NULL PRIMARY KEY
    ,hour INT NOT NULL
    ,day INT NOT NULL
    ,week INT NOT NULL
    ,month INT NOT NULL
    ,year INT NOT NULL
    ,weekday INT NOT NULL
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
    CAST(e.ts AS BIGINT) AS start_time
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
    e.ts IS NOT NULL
    AND e.user_id IS NOT NULL
    AND e.level IS NOT NULL
    AND s.song_id IS NOT NULL
    AND a.artist_id IS NOT NULL
    AND e.session_id IS NOT NULL
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
	user_id IS NOT NULL
    AND first_name IS NOT NULL
    AND last_name IS NOT NULL
    AND level IS NOT NULL
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
    ,CAST(year AS INT)
    ,CAST(duration AS FLOAT)
FROM
	public.staging_songs
WHERE
	song_id IS NOT NULL
    AND title IS NOT NULL
    AND artist_id IS NOT NULL
    AND duration IS NOT NULL
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
    ,CAST(artist_latitude AS FLOAT) AS latitude
    ,CAST(artist_longitude AS FLOAT) AS logitude
FROM
    public.staging_songs
WHERE
	artist_id IS NOT NULL
    AND artist_name IS NOT NULL
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
	t.unixtime AS start_time
    ,EXTRACT(hour FROM t.timestamp) AS hour
    ,EXTRACT(day FROM t.timestamp) AS day
    ,EXTRACT(week FROM t.timestamp) AS week
    ,EXTRACT(month FROM t.timestamp) AS month
    ,EXTRACT(year FROM t.timestamp) AS year
    ,EXTRACT(weekday FROM t.timestamp) AS weekday
FROM (
	SELECT DISTINCT
    	CAST(ts AS BIGINT) AS unixtime
		,TIMESTAMP 'epoch' + CAST(ts AS BIGINT) * INTERVAL '1 second' AS timestamp
	FROM
		public.staging_events
  	WHERE
  		ts IS NOT NULL
) t
;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
