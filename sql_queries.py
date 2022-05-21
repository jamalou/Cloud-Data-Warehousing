import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS events_staging (artist VARCHAR, 
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
                                                                            registration NUMERIC, 
                                                                            sessionId INT, 
                                                                            song VARCHAR DISTKEY, 
                                                                            status INT, 
                                                                            ts BIGINT, 
                                                                            userAgent VARCHAR, 
                                                                            userId VARCHAR
                                                                            );
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS songs_staging  (num_songs INT,
                                                                            artist_id VARCHAR,
                                                                            artist_latitude DOUBLE PRECISION,
                                                                            artist_longitude DOUBLE PRECISION,
                                                                            artist_location VARCHAR,
                                                                            artist_name VARCHAR,
                                                                            song_id VARCHAR,
                                                                            title VARCHAR DISTKEY,
                                                                            duration NUMERIC,
                                                                            year INT
                                                                            );
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id INT IDENTITY(0,1) PRIMARY KEY,
                                                                  start_time TIMESTAMP NOT NULL,
                                                                  user_id INT NOT NULL,
                                                                  level VARCHAR,
                                                                  song_id VARCHAR,
                                                                  artist_id VARCHAR,
                                                                  session_id INT,
                                                                  location VARCHAR,
                                                                  user_agent VARCHAR)
                                                                  
                                                                  DISTSTYLE AUTO;
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id INT NOT NULL PRIMARY KEY,
                                                          first_name VARCHAR,
                                                          last_name VARCHAR,
                                                          gender CHAR,
                                                          level VARCHAR)
                                                          
                                                          DISTSTYLE AUTO;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR NOT NULL PRIMARY KEY DISTKEY,
                                                          title VARCHAR NOT NULL,
                                                          artist_id VARCHAR,
                                                          year INT,
                                                          duration NUMERIC NOT NULL
                                                          );
                                                        
""") #this table will be a direct projection from staging songs (all its columns are in the staging_songs_table)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR NOT NULL PRIMARY KEY,
                                                              name VARCHAR NOT NULL,
                                                              location VARCHAR,
                                                              latitude DOUBLE PRECISION,
                                                              longitude DOUBLE PRECISION
                                                              )
                                                              
                                                              DISTSTYLE AUTO;
""") #this table will be a direct projection from staging songs (all its columns are in the staging_songs_table)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY, 
                                                         hour INT, 
                                                         day INT, 
                                                         week INT, 
                                                         month INT,
                                                         year INT,
                                                         weekday INT
                                                         )
                                                         
                                                         DISTSTYLE AUTO;
""")

# STAGING TABLES

staging_events_copy = ("""copy events_staging
    from {}
    iam_role {}
    json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy songs_staging
    from {}
    iam_role {}
    json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT
                timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
                CASE WHEN userId ~ '^[0-9]+$' THEN CAST(userId AS INT) ELSE NULL END AS user_id,
                level,
                song_id,
                artist_id,
                sessionId AS session_id,
                location,
                userAgent as user_agent
        FROM events_staging e LEFT JOIN songs_staging s
            ON e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration
        WHERE page = 'NextSong' AND user_id IS NOT NULL
        ;
""")

user_table_insert = ("""INSERT INTO users
        SELECT DISTINCT
                CASE WHEN userId ~ '^[0-9]+$' THEN CAST(userId AS INT) ELSE NULL END AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender,
                level
        FROM events_staging
        WHERE user_id IS NOT NULL
        ;                                                                         
""")

song_table_insert = ("""INSERT INTO songs
        SELECT DISTINCT
                song_id,
                title,
                artist_id,
                year,
                duration
        FROM songs_staging
        ;
""")

artist_table_insert = ("""INSERT INTO artists
        SELECT DISTINCT
                artist_id,
                artist_name AS name,
                artist_location AS location,
                artist_latitude AS latitude,
                artist_longitude AS longitude
        FROM songs_staging
        ;
""")

time_table_insert = ("""INSERT INTO time
        SELECT DISTINCT
                timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
                DATE_PART(hour, start_time) AS hour,
                DATE_PART(day, start_time) AS day,
                DATE_PART(week, start_time) AS week,
                DATE_PART(month, start_time) AS month,
                DATE_PART(year, start_time) AS year,
                DATE_PART(dayofweek, start_time) AS weekday
        FROM events_staging
        ;
""")

# QUERY LISTS

# I changed the lists into dicts {table_name: query} to be able to print outputs when running the scripts 
create_table_queries = {
    'events_staging': staging_events_table_create,
    'songs_staging': staging_songs_table_create,
    'songplays': songplay_table_create,
    'users': user_table_create,
    'songs': song_table_create,
    'artist_table_create': artist_table_create,
    'time_table_create': time_table_create
}

drop_table_queries = {
    'events_staging': staging_events_table_drop,
    'songs_staging': staging_songs_table_drop,
    'songplays': songplay_table_drop,
    'users': user_table_drop,
    'songs': song_table_drop,
    'artist_table_create': artist_table_drop,
    'time_table_create': time_table_drop
}

copy_table_queries = {
    'events_staging': staging_events_copy,
    'songs_staging': staging_songs_copy,
}

insert_table_queries = {
    'songplays': songplay_table_insert,
    'users': user_table_insert,
    'songs': song_table_insert,
    'artist_table_create': artist_table_insert,
    'time_table_create': time_table_insert
}
