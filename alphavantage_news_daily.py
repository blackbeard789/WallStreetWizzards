from google.cloud import storage
import psycopg2
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import configparser



os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/ubuntu/airflow/key/wallstreetwizzards-5e5842786425.json'

storage_client = storage.Client()



config_path = '/home/ubuntu/airflow/key/config.ini'
config = configparser.ConfigParser()
config.read(config_path)

user = config.get('database', 'user')
password = config.get('database', 'password')
host = config.get('database', 'host')
port = config.get('database', 'port')
database = config.get('database', 'database')
key = config.get('alphavantage', 'key')
bucket_name = config.get('bucket', 'bucket_name')


# Function to generate date strings
def generate_date_strings():
    # Assuming you want the current date and the previous day
    current_date = datetime.now().strftime("%Y%m%d")
    time_from = datetime.now().strftime("%Y%m%dT0000")
    time_to = datetime.now().strftime("%Y%m%dT2359")
    return current_date, time_from, time_to




def upload_json_to_bucket(blob_name, json_content, bucket_name, folder_path=''):
    '''
    Upload JSON content to a bucket within a specific folder
    : blob_name (str) - object name
    : json_content (dict or str) - JSON content in dictionary or string format
    : bucket_name (str)
    : folder_path (str) - folder path within the bucket (optional)
    '''
    if isinstance(json_content, dict):
        # Convert dictionary to a JSON string
        json_str = json.dumps(json_content, indent=4)
    elif isinstance(json_content, str):
        # Assume it's already a JSON string
        json_str = json_content
    else:
        raise ValueError("Invalid JSON content type. Must be a dictionary or a string.")

    blob_path = f'{folder_path}/{blob_name}' if folder_path else blob_name

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json_str, content_type='application/json')  # Specify content type as JSON
    return blob


# Function to execute the workflow
def requesttobucket_news_daily():
    # Get date strings
    date, time_from, time_to = generate_date_strings()

    

   
    # Construct the URL with the current hour range
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&apikey={key}&time_from={time_from}&time_to={time_to}&limit=1000'

    r = requests.get(url)
    data = r.json()

    upload_json_to_bucket(f'news_{date}.json', data, bucket_name, folder_path='news')
    
   
    


# Download JSON file from the bucket
def download_json_from_bucket(blob_name, bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Download blob content as a byte string
    blob_content = blob.download_as_string()
    
    # Decode byte string to regular string (assuming it contains JSON)
    json_str = blob_content.decode('utf-8')
    
    # Parse the string as JSON
    json_data = json.loads(json_str)
    
    return json_data

def connect_to_cloudsql():
    connection = psycopg2.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )
    return connection



def create_table_if_not_exists(connection):
    try:
        cursor = connection.cursor()

        create_table_query = '''
        CREATE SCHEMA IF NOT EXISTS alphavantage;
        CREATE TABLE IF NOT EXISTS alphavantage.news (
            ticker TEXT,
            time_published TEXT,
            title TEXT,
            source TEXT,
            relevance_score NUMERIC,
            sentiment_score NUMERIC,
            sentiment_label TEXT
        )
        '''
        cursor.execute(create_table_query)
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while creating table in PostgreSQL:", error)


# Function to clear data from a specific table
def clear_table(connection, table_name):
    try:
        cursor = connection.cursor()

        clear_table_query = f'TRUNCATE TABLE {table_name};'  # Use TRUNCATE or DELETE as per your need
        cursor.execute(clear_table_query)
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print(f"Error while clearing table {table_name} in PostgreSQL:", error)
    finally:
        if connection:
            cursor.close()




# Insert data into the table
def insert_data_into_postgresql(connection, data):
    cursor = connection.cursor()
    insert_query = '''
    INSERT INTO alphavantage.news (ticker, time_published, title, source, relevance_score, sentiment_score, sentiment_label)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''
    try:
        for item in data:
            cursor.execute(insert_query, (
                item['ticker'],
                item['time_published'],
                item['title'],
                item['source'],
                item['relevance_score'],
                item['sentiment_score'],
                item['sentiment_label']
            ))
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data into PostgreSQL:", error)
    finally:
        if connection:
            cursor.close()
        



# Function to execute the workflow
def buckettosql_news_daily():

    current_date = datetime.now().strftime("%Y%m%d")
    # Download JSON file from the bucket to a local file
    downloaded_json = download_json_from_bucket(f'news/news_{current_date}.json', f'{bucket_name}')
    # print(type(downloaded_json))  # Use the downloaded JSON data as needed
    


    # Extract and structure data for database insertion
    data_for_database = []
    
    for feed_item in downloaded_json.get('feed', []):
      title = feed_item.get('title', '')
      source = feed_item.get('source', '')
      time_published = feed_item.get('time_published', "")
      parsed_date = datetime.strptime(time_published, '%Y%m%dT%H%M%S')
      formatted_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
      ticker_sentiments = feed_item.get('ticker_sentiment', [])

      for ticker_sentiment in ticker_sentiments:
          if ticker_sentiment.get('ticker') == "BRK-A":
            ticker = "BRK-B"
          elif ticker_sentiment.get('ticker') == "BF-A":
            ticker = "BF-B"
          else:
            ticker = ticker_sentiment.get('ticker', '')
          ticker_data = {
              'ticker': ticker_sentiment.get('ticker', ''),
              'time_published': formatted_date,
              'title': title,
              'source': source,
              'relevance_score': ticker_sentiment.get('relevance_score', 0.0),
              'sentiment_score': ticker_sentiment.get('ticker_sentiment_score', 0.0),
              'sentiment_label': ticker_sentiment.get('ticker_sentiment_label', '')
          }
          data_for_database.append(ticker_data)

    # # Connect to Cloud SQL PostgreSQL
    connection = connect_to_cloudsql()


    # Create table if it doesn't exist
    create_table_if_not_exists(connection)

    # Clear data from the 'alphavantage.news' table
    clear_table(connection, 'alphavantage.news')
    
    # # Insert JSON data into PostgreSQL
    insert_data_into_postgresql(connection, data_for_database)

    
    # # # Close the database connection
    connection.close()


    
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
    'retry_delay': 300,
}

# Define the DAG
dag = DAG('alphavantage_news_daily',
          default_args=default_args,
          description='Download CSV and extract column',
          schedule_interval='@hourly')  # Adjust the schedule interval as needed

# Define a PythonOperator to execute the workflow
requesttobucket_news_daily_task = PythonOperator(
    task_id='requesttobucket_news_daily_task',
    python_callable=requesttobucket_news_daily,
    dag=dag,
)

# Define a PythonOperator to execute the workflow
buckettosql_news_daily_task = PythonOperator(
    task_id='buckettosql_news_daily_task',
    python_callable=buckettosql_news_daily,
    dag=dag,
)

requesttobucket_news_daily_task >> buckettosql_news_daily_task  # Set task dependencies as needed