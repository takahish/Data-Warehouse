# Data Warehouse on AWS

## Required libraries

- configparser
- psycopg2

## Motivation

I learn Data Modeling on AWS by using JSON metadata that represents the songs and JSON files that represents user activity.

## Project

Created tables must be stored into Redshift. In addtition for analyzing user activity, Fact and Dimension tables must be created from songs metadata and user activity logs.

## Files

- create_tables.py: create tables on AWS
- elt.py: define the ETL process
- sql_queries.py: define the SQL queries

## Data

### Songs metadata

The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### User activity logs

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

And below is an example of what a single activity log in 2018-11-13-events.json, looks like.

```
{"artist":null,"auth":"Logged In","firstName":"Kevin","gender":"M","itemInSession":0,"lastName":"Arellano","length":null,"level":"free","location":"Harrisburg-Carlisle, PA","method":"GET","page":"Home","registration":1540006905796.0,"sessionId":514,"song":null,"status":200,"ts":1542069417796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.125 Safari\/537.36\"","userId":"66"}
```

## ETL Processes

The summary of ETL processes is below. For more details, see etl.ipynb, etl.py and sql_queries.py.

### #0: staging table

- Copy song JSON files on S3 to staging_songs table on Redshift.
- Copy log JSON files on S3 to staging_events table on Redshift.
    - Column names in JSON files are different from names of staging_events, so JSONPaths is needed.
      https://docs.aws.amazon.com/ja_jp/redshift/latest/dg/copy-usage_notes-copy-from-json.html

### #1: songs table

- Select columns for song ID, title, artist ID, year, and duration from staging_songs.
- Execute an insert query to songs table in Redshift.

### #2: artists table

- Select columns for artist ID, name, location, latitude, and longitude from staging_songs.
- Execute an insert query to artists table in Redshift.

### #3: time table

- Extract the timestamp, hour, day, week of year, month, year, and weekday from staging_events.
- Execute an insert query to time table in Redshift.

### #4: users table

- Select columns for user ID, first name, last name, gender and level from staging_events.
- Execute an insert query to songs table in Redshift.

### #4: songsplays table

- Select the timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent from staging_events.
  - Log files don't include song ID and artist ID, so get these ID by executing select query to songs and artists tables.
- Execute an insert query to songs table in Redshift.

## Usage

Create tables and execute ETL.

```
$ python create_tables.py
$ python etl.py
```

## Result tables

### Fact Table

#### songplays - records in log data associated with song plays.

|schemaname|tablename|column|type|encoding|distkey|sortkey|notnull|
|-----|-----|-----|-----|-----|-----|-----|-----|
|public|songplays|songplay_id|bigint|lzo|FALSE|0|TRUE|
|public|songplays|start_time|bigint|lzo|FALSE|0|TRUE|
|public|songplays|user_id|character varying(256)|lzo|FALSE|0|TRUE|
|public|songplays|level|character varying(256)|lzo|FALSE|0|TRUE|
|public|songplays|song_id|character varying(256)|lzo|FALSE|0|TRUE|
|public|songplays|artist_id|character varying(256)|lzo|FALSE|0|TRUE|
|public|songplays|session_id|character varying(256)|lzo|FALSE|0|TRUE|
|public|songplays|lcation|character varying(256)|lzo|FALSE|0|FALSE|
|public|songplays|user_agent|character varying(256)|lzo|FALSE|0|FALSE|

<a href="assets/sample_songplays.csv">samples</a>

### Dimension Tables

#### users - users in the app

|schemaname|tablename|column|type|encoding|distkey|sortkey|notnull|
|-----|-----|-----|-----|-----|-----|-----|-----|
|public|users|user_id|character varying(256)|lzo|FALSE|0|TRUE|
|public|users|first_name|character varying(256)|lzo|FALSE|0|TRUE|
|public|users|last_name|character varying(256)|lzo|FALSE|0|TRUE|
|public|users|gender|character varying(256)|lzo|FALSE|0|FALSE|
|public|users|level|character varying(256)|lzo|FALSE|0|TRUE|

<a href="assets/sample_users.csv">samples</a>

#### songs - songs in music database

|schemaname|tablename|column|type|encoding|distkey|sortkey|notnull|
|-----|-----|-----|-----|-----|-----|-----|-----|
|public|songs|song_id|character varying(256)|lzo|FALSE|0|TRUE|
|public|songs|title|character varying(256)|lzo|FALSE|0|TRUE|
|public|songs|artist_id|character varying(256)|lzo|FALSE|0|TRUE|
|public|songs|year|integer|lzo|FALSE|0|FALSE|
|public|songs|duration|double precision|none|FALSE|0|TRUE|

<a href="assets/sample_songs.csv">samples</a>

#### artists - artists in music database

|schemaname|tablename|column|type|encoding|distkey|sortkey|notnull|
|-----|-----|-----|-----|-----|-----|-----|-----|
|public|artists|artist_id|character varying(256)|lzo|FALSE|0|TRUE|
|public|artists|name|character varying(256)|lzo|FALSE|0|TRUE|
|public|artists|location|character varying(256)|lzo|FALSE|0|FALSE|
|public|artists|latitude|double precision|none|FALSE|0|FALSE|
|public|artists|longitude|double precision|none|FALSE|0|FALSE|

<a href="assets/sample_artists.csv">samples</a>

#### time - timestamps of records in songplays broken down into specific units

|schemaname|tablename|column|type|encoding|distkey|sortkey|notnull|
|-----|-----|-----|-----|-----|-----|-----|-----|
|public|time|start_time|bigint|lzo|FALSE|0|TRUE|
|public|time|hour|integer|lzo|FALSE|0|TRUE|
|public|time|day|integer|lzo|FALSE|0|TRUE|
|public|time|week|integer|lzo|FALSE|0|TRUE|
|public|time|month|integer|lzo|FALSE|0|TRUE|
|public|time|year|integer|lzo|FALSE|0|TRUE|
|public|time|weekday|integer|lzo|FALSE|0|TRUE|

<a href="assets/sample_time.csv">samples</a>

## Acknowledgements

I wish to thank Udacity for advice and review.
