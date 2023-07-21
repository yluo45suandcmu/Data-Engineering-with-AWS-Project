# Project: Data Warehouse with AWS

## Project Summary

Sparkify, a fast-growing music startup, has decided to migrate its processes and data to the cloud. Their data, currently residing on S3, includes a collection of JSON logs capturing user activities on their app, as well as JSON metadata on the songs available.

The aim of this project is to build an ETL pipeline that will extract Sparkify's data from S3, stage them in Redshift, and then transform the data into a set of dimensional tables. This will enable Sparkify's analytics team to gain insights into user song-play behaviors more efficiently.

## Schema for Song Play Analysis
Using the song and event datasets, a star schema optimized for queries on song play analysis is created. This includes the following tables.

### Fact Table
songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
- users - users in the app
  user_id, first_name, last_name, gender, level
- songs - songs in music database
  song_id, title, artist_id, year, duration
- artists - artists in music database
  artist_id, name, location, latitude, longitude
- time - timestamps of records in songplays broken down into specific units
  start_time, hour, day, week, month, year, weekday

## File Descriptions

- create_tables.py: This script connects to the database, drops any existing tables, and creates the tables needed for this project.
- etl.py: This is the ETL pipeline, which extracts data from files in S3, stages the data in Redshift, and then transforms the data into a set of dimensional tables.
- sql_queries.py: This script defines the SQL statements, which will be imported into create_tables.py and etl.py.
- dwh.cfg: Configuration file that contains necessary credentials for AWS and database -connection information. Do not share this file publicly.
- README.md: This markdown file provides a summary of the project, how to run the Python scripts, and an explanation of the files in the repository.

## How to Run Python Scripts

1. To run this project you will need to fill the following information, and save it as *dwh.cfg* in the project root folder.

```
[CLUSTER]
HOST=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=5439

[IAM_ROLE]
ARN=''

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
KEY=
SECRET=

[DWH]
DWH_CLUSTER_TYPE       = multi-node
DWH_NUM_NODES          = 4
DWH_NODE_TYPE          = dc2.large
DWH_CLUSTER_IDENTIFIER = 
DWH_DB                 = 
DWH_DB_USER            = 
DWH_DB_PASSWORD        = 
DWH_PORT               = 5439
DWH_IAM_ROLE_NAME      = 
```
2. Run the following command to install the dependencies 

    `$pip install psycopg2`
    
3. Run the *create_tables.py* script to set up the database and tables

    `$ python create_tables.py`

4. Run the *etl.py* script to extract data from the S3 buckets and load them into the newly created tables:

    `$ python etl.py`

5. Run the *test.py* script to validate that the data has been inserted into the tables properly 

    `$ python test.py`
