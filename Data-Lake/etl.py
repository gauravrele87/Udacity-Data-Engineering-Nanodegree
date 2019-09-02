import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function can be used to read the files stored in Amazon S3 (/song_data)
    to get the information for the tables songs, artists
    The required variables in the tables are
    Song Table: unique song id (song_id) which is the primary key, the title (title) of the song, 
                the artist id (artist_id) for the artist, 
                year when the song was produced (year) and duration of the song (duration).
    Artist Table: unique artist id (artist_id) which is the primary key, name of the artist (artist_name), 
                  location (location), latitude of the artist location (artist_latitude), longitude of 
                  artists location (artist_longitude).
    
    Arguments:
        spark: the spark session you want to use. 
        input_data: Amazon S3 path for the raw data
        output_data: Amazon S3 path for storing the processed data
    Returns:
        None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    print('Reading in song data')
    df = spark.read.json(song_data)
    
    print(df)

    # extract columns to create songs table
    songs_table = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    
    # write songs table to parquet files partitioned by year and artist
    print('Writing in song table')
    songs_table.write.parquet(output_data + "/songs_table")

    # extract columns to create artists table
    print('Extracting artists table')
    artists_table = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    
    # write artists table to parquet files
    print('Writing in artists table')
    artists_table.write.parquet(output_data+'/artists_table')


def process_log_data(spark, input_data, output_data):
    """
    This function is used to read the files stored in Amazon S3 (data/log_data)
    to get the user and time info and used to populate the users and time dim tables.
                 
    The required variables for the users table and time dim tables are
    Users Table: Unique user id (user_id), first name of the user (first_name), last name (last_name), gender (gender)
                 and subscrption level (level)
                 
    Time dimenension table: time('TimeStamp', 'Hour', 'Day', 'Week', 'Month', 'Year', 'Weekday')
    
    Arguments:
        spark: the spark session you want to use. 
        input_data: Amazon S3 path for the raw data
        output_data: Amazon S3 path for storing the processed data
    Returns:
        None
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/'

    # read log data file
    print('Reading in log data')
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # extract columns for users table   
    print('Extracting artists table data')
    artists_table = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    
    # write users table to parquet files
    print('Writing in users table')
    artists_table.write.parquet(output_data+'/users_table')

    # create timestamp column from original timestamp column
    print('Creating time table')
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    df = df.withColumn('hour', F.hour(df.datetime))
    df = df.withColumn('day', F.dayofmonth(df.datetime))
    df = df.withColumn('week', F.weekofyear(df.datetime))
    df = df.withColumn('month', F.month(df.datetime))
    df = df.withColumn('year', F.year(df.datetime))
    df = df.withColumn('weekday', F.dayofweek(df.datetime))
    
    # extract columns to create time table
    time_table = df[['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data+'/timetable')

    # read in song data to use for songplays table
    print('Creating songplays table')
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.artist_name == df.artist)
    songplays_table = songplays_table.withColumn("songplay_id",F.monotonically_increasing_id())
    songplays_table = songplays_table[['songplay_id', 'start_time', 'userId', 'level', 'song_id', 
                                      'artist_id', 'sessionId', 'location', 'userAgent','month','year']]

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data + '/songplays_table')



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://randombucketgaurav/udacity-data/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
