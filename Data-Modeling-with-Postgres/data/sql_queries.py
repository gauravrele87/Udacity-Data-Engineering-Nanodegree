# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id SERIAL PRIMARY KEY,
                                start_time TIMESTAMP, 
                                user_id INT, 
                                level VARCHAR, 
                                song_id VARCHAR(64), 
                                artist_id VARCHAR(64), 
                                session_id INT, 
                                location TEXT, 
                                user_agent TEXT
                                )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                                user_id INT NOT NULL PRIMARY KEY, 
                                first_name VARCHAR(128), 
                                last_name VARCHAR(128), 
                                gender VARCHAR(5), 
                                level VARCHAR(5)
                                )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                                song_id varchar NOT NULL PRIMARY KEY, 
                                title TEXT, 
                                artist_id VARCHAR(64), 
                                year INT, 
                                duration FLOAT(4)
                                )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                                artist_id varchar(64) NOT NULL PRIMARY KEY, 
                                name VARCHAR(255), 
                                location VARCHAR(255), 
                                lattitude FLOAT, 
                                longitude FLOAT
                                )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                                start_time TIMESTAMP primary key, 
                                hour INT, 
                                day INT, 
                                week INT, 
                                month INT, 
                                year INT, 
                                weekday TEXT
                                )
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT into songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                            VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)  ON CONFLICT (songplay_id) DO NOTHING
""")

user_table_insert = ("""INSERT into users VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
""")

song_table_insert = ("""INSERT into songs VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""INSERT into artists VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""INSERT into time VALUES (%s, %s, %s, %s, %s, %s, %s) on CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""SELECT song_id, artist_id FROM (SELECT artists.artist_id AS artist_id, songs.song_id as song_id, artists.name AS name, songs.title as title, songs.duration as duration FROM songs INNER JOIN artists ON songs.artist_id = artists.artist_id) as songs_played WHERE songs_played.title LIKE %s AND songs_played.name LIKE %s
""")

# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]