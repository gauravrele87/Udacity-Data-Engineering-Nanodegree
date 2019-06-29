import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description:
        This function can be used to read the file in the filepath (data/song_data)
        to get the information for the tables songs, artists

        The required variables in the tables are
        Song Table: unique song id (song_id) which is the primary key, the title (title) of the song, 
                    the artist id (artist_id) for the artist, 
                    year when the song was produced (year) and duration of the song (duration).

        Artist Table: unique artist id (artist_id) which is the primary key, name of the artist (artist_name), 
                      location (location), latitude of the artist location (artist_latitude), longitude of 
                      artists location (artist_longitude).
    
    Arguments:
        cur: the cursor object. 
        filepath: songs data file path. 

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)                    

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)

def insert_time_data(cur, df):
    """
    Insert all the data in the time table
    
    Params:
        cur: cursor object
        df: The dataframe to be used to create the data for the time table
        
    Returns:
        None
    """
    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit= 'ms')
    
    # insert time data records
    time_data = [t.values, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['TimeStamp', 'Hour', 'Day', 'Week', 'Month', 'Year', 'Weekday']
    time_df = pd.DataFrame({lab: data for lab,data in zip(column_labels, time_data)})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        
def insert_user_data(cur, df):
    """
    Insert all the data in the "users" table
    
    Params:
        cur: cursor object
        df: The dataframe to be used to create the data for the "users" table
        
    Returns:
        None
    """
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        
def insert_songplays_data(cur, df):
    """
    Insert all the data in the "songplays" table
    
    Params:
        cur: cursor object
        df: The dataframe to be used to create the data for the "songplays" table
        
    Returns:
        None
    """
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        #print(song_select, (row.song, row.artist))
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)

def process_log_file(cur, filepath):
    """
    Description: 
        This function can be used to read the file in the filepath (data/log_data)
        to get the user and time info and used to populate the users and time dim tables.
                 
    The required variables for the users table and time dim tables are
    Users Table: Unique user id (user_id), first name of the user (first_name), last name (last_name), gender (gender)
                 and subscrption level (level)
                 
    Time dimenension table: time('TimeStamp', 'Hour', 'Day', 'Week', 'Month', 'Year', 'Weekday')
    
    NOT SURE WHICH ONE is BETTER

    Arguments:
        cur: the cursor object. 
        filepath: log data file path. 

    Returns:
        None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']


    # Call the insert_time_data function to insert the data into the "time" table
    insert_time_data(cur, df)
    
    # Call the insert_user_data function to insert the data into the "users" table
    insert_user_data(cur, df)
    
    # Call the insert_songplay_data function to insert the data into the "songplays" table
    insert_user_data(cur, df)


def process_data(cur, conn, filepath, func):
    """
    Description:
    This function processes all the files given in the file paths by creating a list of file paths to be 
    processed and then looping over each file and processing them using the function provided which would 
    be either 'process_song_file' function or 'process_log_file' function 
    
    Params:
        cur: the cursor object
        conn: connection to the database
        filepath: the overall directory to get the data from
        func: the function to use for processing data
        
    Returns
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Main funciton that connects to the database and calls the process_data function
    
    Params:
        None
        
    Returns:
        None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()