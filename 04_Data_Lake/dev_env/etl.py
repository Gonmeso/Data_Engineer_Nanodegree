import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_KEY']

logs_bucket = config['S3']['LOG_DATA']
songs_bucket = config['S3']['SONG_DATA']
output_bucket = config['S3']['OUTPUT_DATA']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    # Read songs data
    print('------ Reading song data ------')
    song_data = spark.read.json(f'{input_data}/A/A/*/*.json')  # Subset for faster experimentation
    
    # Create temporal table
    print('------ Creating temporal table "tmp_songs" ------')
    song_data.createOrReplaceTempView('tmp_songs')

    # extract columns to create songs table
    print('------ Extracting songs table columns ------')
    songs_table = spark.sql(
    '''
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM tmp_songs
    WHERE song_id IS NOT NULL
    ''')
    
    # write songs table to parquet files partitioned by year and artist
    print('------ Writing songs tables parquets ------')
    songs_path = f'{output_data}/songs/'
    songs_table.write.partitionBy('year', 'artist_id').parquet(
            songs_path,
            mode='overwrite'
        )

    # extract columns to create artists table
    print('------ Extracting artists table columns ------')
    artists_table = spark.sql(
    '''
    SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location,
                    artist_latitude AS latitude, artist_longitude AS longitude
    FROM tmp_songs
    WHERE artist_id IS NOT NULL
    ''')
    
    # write artists table to parquet files
    print('------ Writing artists tables parquets ------')
    artists_path = f'{output_data}/artists/'
    artists_table.write.parquet(
            artists_path,
            mode='overwrite'
        )


def process_log_data(spark, input_data, output_data):

    # Read logs files
    print('------ Reading logs data ------')
    events = spark.read.json(f'{logs_bucket}/*/*/*.json')
    
    # Cast ts to timestamp
    print('------ Casting to timestamp ------')
    events = events.withColumn("start_time",
        (col("ts")/1000).cast("timestamp"))

    # Create temporal table
    print('------ Creating temporal table "tmp_events" ------')
    events.createOrReplaceTempView('tmp_events')

    # extract columns for users table    
    print('------ Extracting artists table columns ------')
    users_table = spark.sql('''
    SELECT DISTINCT userId as user_id, firstName as first_name, lastName as last_name, gender, level
    FROM tmp_events
    WHERE userId IS NOT NULL
    ''')
    
    # write users table to parquet files
    print('------ Writing users tables parquets ------')
    users_path = f'{output_data}/users/'
    users_table.write.parquet(
            users_path,
            mode='overwrite'
        )
    
    # extract columns to create time table
    print('------ Extracting time table columns ------')
    time_table = spark.sql(
    '''
    SELECT DISTINCT start_time,
                    EXTRACT(hour from start_time) as hour,
                    EXTRACT(day from start_time) as day,
                    EXTRACT(week from start_time) as week,
                    EXTRACT(month from start_time) as month,
                    EXTRACT(year from start_time) as year,
                    DAYOFWEEK(start_time) AS weekday
    FROM tmp_events
    WHERE ts IS NOT NULL
    ''')
    
    # write time table to parquet files partitioned by year and month
    print('------ Writing time tables parquets ------')
    time_path = f'{output_data}/time/'
    time_table.write.partitionBy('year', 'month').parquet(
            time_path,
            mode='overwrite'
        )

    # extract columns from joined song and log datasets to create songplays table 
    print('------ Extracting songplays table columns ------')
    songplays_table = spark.sql(
    '''
    SELECT e.start_time, e.userId AS user_id, e.level, e.sessionId AS session_id, e.location,
           e.userAgent AS user_agent, s.song_id, s.artist_id
    FROM tmp_events e
    JOIN tmp_songs s ON e.artist = s.artist_name
    AND e.song = s.title
    WHERE e.page = 'NextSong'
    ''')

    # write songplays table to parquet files partitioned by year and month
    print('------ Writing songplays tables parquets ------')
    songplays_path = f'{output_data}/songplays/'
    songplays_table.write.partitionBy('year', 'month').parquet(
            songplays_path,
            mode='overwrite'
        )


def main():
    print('Creating Spark Session')
    spark = create_spark_session()
    
    process_song_data(spark, songs_bucket, output_bucket)    
    process_log_data(spark, logs_bucket, output_bucket)
    
    print('Process finished successfully')


if __name__ == "__main__":
    main()