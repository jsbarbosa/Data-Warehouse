import configparser

# Load configuration and store relevant variables
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE__ARN: str = config.get('IAM_ROLE', 'ARN')
S3__LOG_DATA: str = config.get('S3', 'LOG_DATA')
S3__LOG_JSON_PATH: str = config.get('S3', 'LOG_JSONPATH')
S3__SONG_DATA: str = config.get('S3', 'SONG_DATA')
S3__SONGS_JSONPATH: str = config.get('S3', 'SONGS_JSONPATH')

"""
SQL STATEMENTS
"""
# Set required table names
STAGING_EVENTS_TABLE: str = 'staging_events'
STAGING_SONGS_TABLE: str = 'staging_songs'
SONGPLAY_TABLE: str = 'songplays'
USER_TABLE: str = 'users'
SONG_TABLE: str = 'songs'
ARTIST_TABLE: str = 'artists'
TIME_TABLE: str = 'times'

# Drop tables
DROP_TABLE_FORMAT: str = 'DROP TABLE IF EXISTS {table};'

# Create table statements
STAGING_EVENTS_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {STAGING_EVENTS_TABLE} (
    event_id INT IDENTITY(0,1) NOT NULL,
    artist VARCHAR NULL,
    auth VARCHAR NULL,
    firstName VARCHAR NULL,
    gender VARCHAR NULL,
    itemInSession VARCHAR NULL,
    lastName VARCHAR NULL,
    length VARCHAR NULL,
    level VARCHAR NULL,
    location VARCHAR NULL,
    method VARCHAR NULL,
    page VARCHAR NULL,
    registration VARCHAR NULL,
    sessionId INTEGER NOT NULL DISTKEY SORTKEY,
    song VARCHAR NULL,
    status INTEGER NULL,
    ts BIGINT NOT NULL,
    userAgent VARCHAR NULL,
    userId INTEGER NULL
);
"""

STAGING_SONGS_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {STAGING_SONGS_TABLE}(
    song_id VARCHAR NOT NULL,
    num_songs INTEGER NULL,
    artist_id VARCHAR NOT NULL DISTKEY SORTKEY,
    artist_latitude VARCHAR  NULL,
    artist_longitude VARCHAR NULL,
    artist_location VARCHAR NULL,
    artist_name VARCHAR NULL,
    title VARCHAR(500) NULL,
    duration FLOAT NULL,
    year INTEGER NULL
);
"""

SONGPLAY_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {SONGPLAY_TABLE}(
    songplay_id  INTEGER IDENTITY(0,1) SORTKEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
);
"""

USER_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {USER_TABLE}(
    user_id INT NOT NULL SORTKEY, 
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR, 
    level VARCHAR
) diststyle all;
"""

SONG_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {SONG_TABLE}(
    song_id VARCHAR NOT NULL SORTKEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration FLOAT
) diststyle all;
"""

ARTIST_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {ARTIST_TABLE}(
    artist_id VARCHAR NOT NULL SORTKEY,
    name VARCHAR, 
    location VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT
) diststyle all;
"""

TIME_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {TIME_TABLE}(
    start_time TIMESTAMP NOT NULL SORTKEY, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT
) diststyle all;
"""


# STAGING TABLES
STAGING_EVENTS_COPY_STATEMENT: str = f"""
    COPY {STAGING_EVENTS_TABLE} FROM {S3__LOG_DATA}
    credentials 'aws_iam_role={IAM_ROLE__ARN}'
    format as json {S3__LOG_JSON_PATH}
    STATUPDATE ON
    region 'us-west-2';
"""

STAGING_SONGS_COPY_STATEMENT: str = f"""
    COPY {STAGING_SONGS_TABLE} FROM {S3__SONG_DATA}
    credentials 'aws_iam_role={IAM_ROLE__ARN}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
"""

# FINAL TABLES

"""
Fact table records in event data associated with song plays i.e.
records with page `NextSong`
"""
SONGPLAY_TABLE_INSERT: str = f"""
INSERT INTO {SONGPLAY_TABLE} (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT 
    TIMESTAMP 'epoch' + {STAGING_EVENTS_TABLE}.ts / 1000 * INTERVAL '1 second' AS start_time,
    {STAGING_EVENTS_TABLE}.user_id,
    {STAGING_EVENTS_TABLE}.level,
    {STAGING_SONGS_TABLE}.song_id,
    {STAGING_SONGS_TABLE}.artist_id,
    {STAGING_EVENTS_TABLE}.session_id,
    {STAGING_EVENTS_TABLE}.location,
    {STAGING_EVENTS_TABLE}.user_agent
FROM 
    {STAGING_EVENTS_TABLE}
        JOIN
    {STAGING_SONGS_TABLE} 
        ON {STAGING_SONGS_TABLE}.artist_name = {STAGING_EVENTS_TABLE}].artist
WHERE
    {STAGING_EVENTS_TABLE}.page = 'NextSong'
"""

USER_TABLE_INSERT: str = f"""
INSERT INTO {USER_TABLE}
SELECT 
    DISTINCT {STAGING_EVENTS_TABLE}.userId AS user_id, # only one entry per user
    {STAGING_EVENTS_TABLE}.firstName AS first_name,
    {STAGING_EVENTS_TABLE}.lastName AS last_name,
    {STAGING_EVENTS_TABLE}.gender,
    {STAGING_EVENTS_TABLE}.level
FROM 
    {STAGING_EVENTS_TABLE}
WHERE 
    {STAGING_EVENTS_TABLE}.page = 'NextSong';
"""

SONG_TABLE_INSERT: str = f"""
INSERT INTO {SONG_TABLE}
SELECT 
    DISTINCT {STAGING_SONGS_TABLE}.song_id,
    {STAGING_SONGS_TABLE}.title,
    {STAGING_SONGS_TABLE}.artist_id,
    {STAGING_SONGS_TABLE}.year,
    {STAGING_SONGS_TABLE}.duration
FROM
    {STAGING_SONGS_TABLE};
"""

ARTIST_TABLE_INSERT: str = f"""
INSERT INTO {ARTIST_TABLE}
SELECT
    {STAGING_SONGS_TABLE}.artist_id, 
    {STAGING_SONGS_TABLE}.artist_name, 
    {STAGING_SONGS_TABLE}.artist_location, 
    {STAGING_SONGS_TABLE}.artist_latitude, 
    {STAGING_SONGS_TABLE}.artist_longitude
FROM
    {STAGING_SONGS_TABLE};
"""

TIME_TABLE_INSERT: str = f"""
INSERT INTO {TIME_TABLE}
SELECT 
    DISTINCT TIMESTAMP 'epoch' + {STAGING_EVENTS_TABLE}.ts / 1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(week FROM start_time) AS weekday
FROM 
    {STAGING_EVENTS_TABLE}
WHERE
    {STAGING_EVENTS_TABLE}.page = 'NextSong';
"""


# QUERY GROUPING BY ACTION
DROP_TABLES: list = [
    STAGING_EVENTS_TABLE,
    STAGING_SONGS_TABLE,
    SONGPLAY_TABLE,
    SONG_TABLE,
    ARTIST_TABLE,
    TIME_TABLE,
]

CREATE_TABLE_QUERIES: list = [
    STAGING_EVENTS_TABLE_CREATE,
    STAGING_SONGS_TABLE_CREATE,
    SONGPLAY_TABLE_CREATE,
    USER_TABLE_CREATE,
    SONG_TABLE_CREATE,
    ARTIST_TABLE_CREATE,
    TIME_TABLE_CREATE
]

COPY_TABLE_QUERIES: list = [
    STAGING_EVENTS_COPY_STATEMENT,
    STAGING_SONGS_COPY_STATEMENT
]

INSERT_TABLE_QUERIES: list = [
    SONGPLAY_TABLE_INSERT,
    USER_TABLE_INSERT,
    SONG_TABLE_INSERT,
    ARTIST_TABLE_INSERT,
    TIME_TABLE_INSERT
]
