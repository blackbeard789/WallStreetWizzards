from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import os
from google.cloud import storage
import json
from datetime import datetime, timedelta
import psycopg2
import configparser



os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/ubuntu/airflow/key/wallstreetwizzards-5e5842786425.json'


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
    past_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    time_from = (datetime.now() - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    time_to = (datetime.now() - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=0)
    return past_date, time_from, time_to



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
def requesttobucket_news_hist():
    # Get date strings
    date, time_from, time_to = generate_date_strings()


    # List to store the data from each API call
    all_data = []

    # Loop through each hour from time_from until time_to is reached
    while time_from <= time_to:
        current_hour_from = time_from.strftime("%Y%m%dT%H%M")  # Format for the API call start time
        current_hour_to = (time_from + timedelta(minutes=59)).strftime("%Y%m%dT%H%M")  # Format for the API call end time

        # Construct the URL with the current hour range
        url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&apikey={key}&time_from={current_hour_from}&time_to={current_hour_to}&limit=1000'

        r = requests.get(url)
        data = r.json()

        # Append data to the list
        all_data.append(data)

        # Move to the next hour
        time_from += timedelta(hours=1)



    # Combine all data into a single JSON file
    combined_data = {
        "date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        "data": all_data
    }


    upload_json_to_bucket(f'news_{date}.json', combined_data, bucket_name, folder_path='news')
    



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
        CREATE TABLE IF NOT EXISTS alphavantage.hist_news (
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
    INSERT INTO alphavantage.hist_news (ticker, time_published, title, source, relevance_score, sentiment_score, sentiment_label)
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
def buckettosql_news_hist():

    past_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    # Download JSON file from the bucket to a local file
    downloaded_json = download_json_from_bucket(f'news/news_{past_date}.json', f'{bucket_name}')
  

    # Extract and structure data for database insertion
    data_for_database = []
    for entry in downloaded_json['data']:
        for feed_item in entry.get('feed', []):
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
                    'ticker': ticker,
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
    #clear_table(connection, 'alphavantage.hist_news')
    
    # Insert JSON data into PostgreSQL
    insert_data_into_postgresql(connection, data_for_database)
    
    # # # Close the database connection
    connection.close()

    
 
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=2),
    'retries': 1,
    'retry_delay': 300,
}

# Define the DAG
dag = DAG('alphavantage_news_hist',
          default_args=default_args,
          description='Download CSV and extract column',
          schedule_interval= '00 8 * * *')  # Adjust the schedule interval as needed

# Define a PythonOperator to execute the workflow
requesttobucket_news_hist_task = PythonOperator(
    task_id='requesttobucket_news_hist',
    python_callable=requesttobucket_news_hist,
    dag=dag,
)


# Define a PythonOperator to execute the workflow
buckettosql_news_his_task = PythonOperator(
    task_id='buckettosql_news_hist',
    python_callable=buckettosql_news_hist,
    dag=dag,
)

requesttobucket_news_hist_task >> buckettosql_news_his_task  # Set task dependencies as needed