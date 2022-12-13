import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS public.staging_events (
    artist varchar(256),
    auth varchar(256),
    firstname varchar(256),
    gender varchar(10),
    iteminsession int4,
    lastname varchar(256),
    length float,
    "level" varchar(10),
    location varchar(256),
    "method" varchar(256),
    page varchar(256),
    registration numeric(18,0),
    sessionid int4,
    song varchar(256),
    status int4,
    ts int8,
    useragent varchar(256),
    userid int4
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS public.staging_songs (
    num_songs int4,
    artist_id varchar(256),
    artist_name varchar(256),
    artist_latitude numeric(18,5),
    artist_longitude numeric(18,5),
    artist_location varchar(256),
    song_id varchar(256),
    title varchar(256),
    duration float,
    "year" int4
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS public.songplays (
    songplay_id BIGINT IDENTITY(1, 1),
    start_time timestamp NOT NULL,
    userid int4 NOT NULL,
    "level" varchar(10),
    songid varchar(256),
    artistid varchar(256),
    sessionid int4,
    location varchar(256),
    user_agent varchar(256),
    CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS public.users (
    userid int4 NOT NULL,
    first_name varchar(256),
    last_name varchar(256),
    gender varchar(10),
    "level" varchar(10),
    CONSTRAINT users_pkey PRIMARY KEY (userid)
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS public.songs (
    songid varchar(256) NOT NULL,
    title varchar(256),
    artistid varchar(256),
    "year" int4,
    duration numeric,
    CONSTRAINT songs_pkey PRIMARY KEY (songid)
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS public.artists (
    artistid varchar(256) NOT NULL,
    name varchar(256),
    location varchar(256),
    lattitude numeric(18,5),
    longitude numeric(18,5)
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY, 
    hour int,
    day int, 
    week int, 
    month int,
    year int,
    weekday int
);
""")

# STAGING TABLES
staging_events_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {}
    REGION {};
""").format(
    'staging_events',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['CLUSTER']['REGION']
)

staging_songs_copy = ("""
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto'
    REGION {};
""").format(
    'staging_songs',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['REGION']
)


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time , userid , level , songid , 
                            artistid , sessionid , location , user_agent) 
                            SELECT
                            TIMESTAMP 'epoch' + (events.ts/1000 * INTERVAL '1 second') as start_time,
                            events.userid, 
                            events.level, 
                            songs.song_id, 
                            songs.artist_id, 
                            events.sessionid, 
                            events.location, 
                            events.useragent
                        FROM staging_events events
                        LEFT JOIN staging_songs songs
                            ON events.song = songs.title
                            AND events.artist = songs.artist_name
                            AND events.length = songs.duration
                        WHERE events.page = 'NextSong'
                """)
                
user_table_insert = ("""INSERT INTO users (userid, first_name, last_name, gender, level ) 
                        SELECT DISTINCT userid,
                        firstName,
                        lastName,
                        gender,
                        level 
                        FROM staging_events 
                        where page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO songs (songid, title, artistid, year, duration) 
                        SELECT DISTINCT song_id, 
                        title, 
                        artist_id, 
                        year, 
                        duration
                        FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists (artistid, name, location, lattitude, longitude) 
                        SELECT DISTINCT artist_id,
                        artist_name,
                        artist_location, 
                        artist_latitude, 
                        artist_longitude  
                        FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                        SELECT DISTINCT start_time, 
                        extract(hour from start_time), 
                        extract(day from start_time), 
                        extract(week from start_time), 
                        extract(month from start_time),                             
                        extract(year from start_time), 
                        extract(dayofweek from start_time) 
                        FROM songplays 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
