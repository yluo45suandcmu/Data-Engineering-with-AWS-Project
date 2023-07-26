# Project: Apache Airflow Data Pipelines

## Project Summary

Sparkify, a fast-growing music startup, has decided to migrate its processes and data to the cloud. Their data, currently residing on S3, includes a collection of JSON logs capturing user activities on their app, as well as JSON metadata on the songs available.

This is a project based on Apache Airflow that demonstrates the concepts of building custom operators to perform tasks like staging data, populating a data warehouse and executing data checks.

## Project structure
Here's the directory structure for this project:

```
.
├── README.md
├── dags
│   └── your_dag_file.py
├── plugins
│   ├── helpers
│   │   ├── __init__.py
│   │   └── sql_queries.py
│   └── operators
│       ├── __init__.py
│       ├── data_quality.py
│       ├── load_dimension.py
│       ├── load_fact.py
│       └── stage_redshift.py

```

## Prerequisites
- You need to create an IAM User in your AWS account. Check this https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html.
- Setup and configure AWS Redshift Serverless. Follow this https://docs.aws.amazon.com/redshift/latest/dg/welcome.html.
- Setup Airflow connections with AWS and AWS Redshift with docker. Follow this https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html. 

## Project Instructions
You'll be provided with two datasets stored in the following S3 buckets:
- Log data: 's3://udacity-dend/log_data'
- Song data: 's3://udacity-dend/song_data'
Copy the data from the Udacity S3 buckets to your own S3 bucket using the AWS CLI. Update the S3 path information in the Airflow data pipeline configuration.

Implement the following four operators:
- 'StageToRedshiftOperator': Loads JSON formatted files from S3 to Redshift tables.
- 'LoadFactOperator': Takes an SQL query and a target database table as input, and executes the query to load the fact table.
- 'LoadDimensionOperator': Takes an SQL query and a target database table as input, and executes the query to load the dimension table.
- 'DataQualityOperator': Runs SQL query checks on the data loaded in the tables and raises an exception if the check fails.

## Running the Project
After setting up, you can run the project by triggering the data pipeline from the Airflow UI.

![Working DAG with correct task dependencies](example-dag.png)