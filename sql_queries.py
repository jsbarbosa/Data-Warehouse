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
CREATE TABLE {STAGING_EVENTS_TABLE} (
  d_datekey            integer       not null sortkey,
  d_date               varchar(19)   not null,
  d_dayofweek	      varchar(10)   not null,
  d_month      	    varchar(10)   not null,
  d_year               integer       not null,
  d_yearmonthnum       integer  	 not null,
  d_yearmonth          varchar(8)	not null,
  d_daynuminweek       integer       not null,
  d_daynuminmonth      integer       not null,
  d_daynuminyear       integer       not null,
  d_monthnuminyear     integer       not null,
  d_weeknuminyear      integer       not null,
  d_sellingseason      varchar(13)    not null,
  d_lastdayinweekfl    varchar(1)    not null,
  d_lastdayinmonthfl   varchar(1)    not null,
  d_holidayfl          varchar(1)    not null,
  d_weekdayfl          varchar(1)    not null)
diststyle all;
"""

STAGING_SONGS_TABLE_CREATE: str = ""

SONGPLAY_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {SONGPLAY_TABLE}(
    songplay_id SERIAL,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR,
    PRIMARY KEY (songplay_id)
);
"""

USER_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {USER_TABLE}(
    user_id INT NOT NULL, 
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR, 
    level VARCHAR,
    PRIMARY KEY (user_id)
);
"""

SONG_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {SONG_TABLE}(
    song_id VARCHAR NOT NULL,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration FLOAT,
    PRIMARY KEY (song_id)
);
"""

ARTIST_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {ARTIST_TABLE}(
    artist_id VARCHAR NOT NULL,
    name VARCHAR, 
    location VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT,
    PRIMARY KEY (artist_id)
);
"""

TIME_TABLE_CREATE: str = f"""
CREATE TABLE IF NOT EXISTS {TIME_TABLE}(
    start_time TIMESTAMP NOT NULL, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT,
    PRIMARY KEY (start_time)
);
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
SONGPLAY_TABLE_INSERT: str = f"""
"""

USER_TABLE_INSERT: str = f"""
"""

SONG_TABLE_INSERT: str = f"""
"""

ARTIST_TABLE_INSERT: str = f"""
"""

TIME_TABLE_INSERT: str = f"""
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
