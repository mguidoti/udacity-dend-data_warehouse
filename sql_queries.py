import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                                 CREATE TABLE IF NOT EXISTS staging_events (
                                                                             event_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY DISTKEY,
                                                                             artist VARCHAR,
                                                                             auth VARCHAR,
                                                                             firstName VARCHAR,
                                                                             gender VARCHAR,
                                                                             itemInSession INTEGER, 
                                                                             lastName VARCHAR,
                                                                             length FLOAT, 
                                                                             level VARCHAR,
                                                                             location VARCHAR,
                                                                             method VARCHAR,
                                                                             page VARCHAR,
                                                                             registration BIGINT,
                                                                             sessionId INTEGER,
                                                                             song VARCHAR,
                                                                             status INTEGER,
                                                                             ts TIMESTAMP,
                                                                             userAgent VARCHAR,
                                                                             userId INTEGER);
""")

staging_songs_table_create = ("""
                                 CREATE TABLE IF NOT EXISTS staging_songs (
                                                                            num_songs INTEGER NOT NULL PRIMARY KEY DISTKEY,
                                                                            artist_id VARCHAR NOT NULL,
                                                                            artist_latitude DECIMAL,
                                                                            artist_longitude DECIMAL,
                                                                            artist_location VARCHAR,
                                                                            artist_name VARCHAR,
                                                                            song_id VARCHAR,
                                                                            title VARCHAR,
                                                                            duration DECIMAL,
                                                                            year INTEGER);
""")

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplays (
                                                                   songplay_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY DISTKEY,
                                                                   start_time TIMESTAMP NOT NULL,
                                                                   user_id VARCHAR,
                                                                   level VARCHAR,
                                                                   song_id VARCHAR,
                                                                   artist_id VARCHAR,
                                                                   session_id VARCHAR,
                                                                   location VARCHAR,
                                                                   user_agent VARCHAR);
                         """)

user_table_create = ("""
                        CREATE TABLE IF NOT EXISTS users (
                                                           user_id INTEGER NOT NULL PRIMARY KEY DISTKEY,
                                                           first_name VARCHAR,
                                                           last_name VARCHAR,
                                                           gender VARCHAR,
                                                           level VARCHAR);
                     """)

song_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songs (
                                                           song_id VARCHAR NOT NULL PRIMARY KEY DISTKEY,
                                                           title VARCHAR,
                                                           artist_id VARCHAR,
                                                           year INTEGER,
                                                           duration DECIMAL);
                     """)

artist_table_create = ("""
                          CREATE TABLE IF NOT EXISTS artists (
                                                               artist_id VARCHAR NOT NULL PRIMARY KEY DISTKEY,
                                                               name VARCHAR,
                                                               location VARCHAR,
                                                               latitude DECIMAL,
                                                               longitude DECIMAL);
                       """)

time_table_create = ("""
                        CREATE TABLE IF NOT EXISTS time (
                                                          start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY DISTKEY,
                                                          hour INTEGER,
                                                          day INTEGER,
                                                          week INTEGER,
                                                          month INTEGER,
                                                          year INTEGER,
                                                          weekday INTEGER);
                     """)

# STAGING TABLES

staging_events_copy = ("""
                          COPY staging_events FROM {}
                          CREDENTIALS 'aws_iam_role={}'
                          TIMEFORMAT as 'epochmillisecs'
                          FORMAT AS JSON {}
                          region 'us-west-2'
                          BLANKSASNULL EMPTYASNULL;
                       """).format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSON_PATH'))

staging_songs_copy = ("""
                         COPY staging_songs FROM {}
                         CREDENTIALS 'aws_iam_role={}'
                         FORMAT AS JSON 'auto'
                         region 'us-west-2'
                         BLANKSASNULL EMPTYASNULL;
                      """).format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
                            INSERT INTO songplays (
                                                    start_time,
                                                    user_id,
                                                    level,
                                                    song_id,
                                                    artist_id,
                                                    session_id,
                                                    location,
                                                    user_agent)
                                 SELECT DISTINCT events.ts,
                                                 events.userId,
                                                 events.level,
                                                 songs.song_id,
                                                 songs.artist_id,
                                                 events.sessionId,
                                                 events.location,
                                                 events.userAgent
                                 FROM staging_events AS events
                                 INNER JOIN staging_songs AS songs
                                     ON events.artist = songs.artist_name
                                         AND events.song = songs.title
                                     WHERE events.page = 'NextSong';
""")

user_table_insert = ("""
                        INSERT INTO users (
                                            user_id,
                                            first_name,
                                            last_name,
                                            gender,
                                            level)
                            SELECT DISTINCT events.userId,
                                            events.firstName,
                                            events.lastName,
                                            events.gender,
                                            events.level
                            FROM staging_events AS events
                            WHERE events.userId IS NOT NULL;
                                            
""")

song_table_insert = ("""
                        INSERT INTO songs (
                                           song_id,
                                           title,
                                           artist_id,
                                           year,
                                           duration)
                            SELECT DISTINCT songs.song_id,
                                            songs.title,
                                            songs.artist_id,
                                            songs.year,
                                            songs.duration
                            FROM staging_songs AS songs
                            WHERE songs.song_id IS NOT NULL;
""")

artist_table_insert = ("""
                          INSERT INTO artists (
                                               artist_id,
                                               name,
                                               location,
                                               latitude,
                                               longitude)
                          SELECT DISTINCT songs.artist_id,
                                          songs.artist_name,
                                          songs.artist_location,
                                          songs.artist_latitude,
                                          songs.artist_longitude
                          FROM staging_songs AS songs
                          WHERE songs.artist_id IS NOT NULL;
""")

time_table_insert = ("""
                        INSERT INTO time (
                                          start_time,
                                          hour,
                                          day,
                                          week,
                                          month,
                                          year,
                                          weekday)
                        SELECT DISTINCT events.ts,
                                        EXTRACT(hour FROM events.ts),
                                        EXTRACT(day FROM events.ts),
                                        EXTRACT(week FROM events.ts),
                                        EXTRACT(month FROM events.ts),
                                        EXTRACT(year FROM events.ts),
                                        EXTRACT(dayofweek FROM events.ts)
                        FROM staging_events AS events
                        WHERE events.page = 'NextSong';
                                        
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]