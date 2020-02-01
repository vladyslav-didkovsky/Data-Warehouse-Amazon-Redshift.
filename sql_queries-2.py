import configparser
import os

from data_warehouse.config import CONFIG
from data_warehouse.read import read_sql_query

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
         artist TEXT,
           auth TEXT,
      firstName TEXT,
         gender TEXT,
  iteminsession INTEGER,
       lastname TEXT,
         length FLOAT,
          level TEXT,
       location TEXT,
         method TEXT,
           page TEXT,
   registration FLOAT,
      sessionid INTEGER,
           song TEXT,
         status INTEGER,
             ts BIGINT,
      useragent TEXT,
         userid FLOAT
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
           song_id TEXT PRIMARY KEY,
             title VARCHAR(1024),
          duration FLOAT,
              year FLOAT,
         num_songs FLOAT,
         artist_id TEXT,
       artist_name VARCHAR(1024),
   artist_latitude FLOAT,
  artist_longitude FLOAT,
   artist_location VARCHAR(1024)
);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
  songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
   start_time BIGINT NOT NULL REFERENCES time(start_time) sortkey,
      user_id INTEGER NOT NULL REFERENCES users(user_id),
        level VARCHAR NOT NULL,
      song_id VARCHAR NOT NULL REFERENCES songs(song_id) distkey,
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
   session_id INTEGER NOT NULL,
     location VARCHAR NOT NULL,
   user_agent VARCHAR NOT NULL
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
     user_id INTEGER NOT NULL PRIMARY KEY,
  first_name VARCHAR NOT NULL,
   last_name VARCHAR NOT NULL,
      gender VARCHAR NOT NULL,
       level VARCHAR NOT NULL
)
DISTSTYLE all;
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
      title VARCHAR(1024) NOT NULL,
  artist_id VARCHAR NOT NULL REFERENCES artists(artist_id) sortkey distkey,
       year INTEGER,
   duration FLOAT
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
  artist_id VARCHAR PRIMARY KEY,
       name VARCHAR(1024) NOT NULL,
   location VARCHAR(1024),
  lattitude FLOAT,
  longitude FLOAT
)
DISTSTYLE all;
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
  start_time BIGINT PRIMARY KEY,
        hour INTEGER,
         day INTEGER,
        week INTEGER,
       month INTEGER,
        year INTEGER,
     weekday INTEGER
)
DISTSTYLE all;
"""

# STAGING TABLES
staging_events_copy = """
COPY staging_events
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON '{}';
""".format(CONFIG["S3"]["LOG_DATA"], CONFIG["IAM_ROLE"]["ARN"], CONFIG["S3"]["LOG_JSONPATH"])
staging_songs_copy = """
COPY staging_songs
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""".format(CONFIG["S3"]["SONG_DATA"], CONFIG["IAM_ROLE"]["ARN"])

# FINAL TABLES
songplay_table_insert = """
insert into songplays (start_time,
                       user_id,
                       level,
                       song_id,
                       artist_id,
                       session_id,
                       location,
                       user_agent)
select staging_events.ts as start_time,
       staging_events.userid::INTEGER as user_id,
       staging_events.level,
       staging_songs.song_id,
       staging_songs.artist_id,
       staging_events.sessionid as session_id,
       staging_events.location,
       staging_events.useragent as user_agent
  from staging_events
  left join staging_songs
    on staging_events.song = staging_songs.title
   and staging_events.artist = staging_songs.artist_name
  left outer join songplays
    on staging_events.userid = songplays.user_id
   and staging_events.ts = songplays.start_time
 where staging_events.page = 'NextSong'
   and staging_events.userid is not Null
   and staging_events.level is not Null
   and staging_songs.song_id is not Null
   and staging_songs.artist_id is not Null
   and staging_events.sessionid is not Null
   and staging_events.location is not Null
   and staging_events.useragent is not Null
   and songplays.songplay_id is Null
 order by start_time, user_id
;
"""

user_table_insert = """
insert into users
select user_id::INTEGER,
       first_name,
       last_name,
       gender,
       level
  from (select userid as user_id,
               firstname as first_name,
               lastname as last_name,
               gender,
               level
          from staging_events
         where user_id is not Null) as temp
 group by user_id, first_name, last_name, gender, level
 order by user_id;
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
  FROM staging_songs;
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
  FROM staging_songs;
"""

time_table_insert = """
insert into time
select start_time,
       date_part(hour, date_time) as hour,
       date_part(day, date_time) as day,
       date_part(week, date_time) as week,
       date_part(month, date_time) as month,
       date_part(year, date_time) as year,
       date_part(weekday, date_time) as weekday
  from (select ts as start_time,
               '1970-01-01'::date + ts/1000 * interval '1 second' as date_time
          from staging_events
         group by ts) as temp
 order by start_time;
"""


# QUERY LISTS
create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, staging_events_table_create, staging_songs_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]