try:
    import os
    import configparser
    import psycopg2
    import datetime
    import pandas as pd
    import yfinance as yf
    from datetime import timedelta, datetime
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from google.cloud import storage
    from google.cloud.storage.blob import Blob
    from io import StringIO, BytesIO

except Exception as e:
    print(f"Error {e}")


def get_components():
    """Get the index components from the S&P 500 from Wikipedia"""

    # Get current S&P 500 components from Wikipedia
    components = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]

    # Replace points with hyphen because of API-Call
    components['Symbol'] = components['Symbol'].str.replace('.', '-')
    n = len(components)

    if n >= 500:
        print("Successfully got the components of the S&P 500 from Wikipedia!")
        symbols = components["Symbol"].tolist()
        return symbols

    else:
        print(f"There is an error while loading index components of the S&P 500! The length is {n} instead of >= 500.")
        return None


def get_yesterday_price(symbol_list):
    """Get the price from closing (yesterday) for S&P 500 components"""

    # Initialize an empty list to store data
    data_list = []

    # Get the dates
    t = datetime.now()
    t_m1 = t - timedelta(days=1)
    today = t.strftime('%Y-%m-%d')
    yesterday = t_m1.strftime('%Y-%m-%d')

    # Loop through each symbol in the list and get stock price from yesterday
    for symbol in symbol_list:
        print(symbol)
        stock_data = yf.download(symbol, start=yesterday,
                                 end=today)

        # Check if data is available for the specified date
        if not stock_data.empty:
            # Extract relevant information and append to the data list
            data_list.append([symbol,
                              yesterday,  # t-1 because values refere to t-1
                              stock_data['Open'].values[0],
                              stock_data['High'].values[0],
                              stock_data['Low'].values[0],
                              stock_data['Close'].values[0],
                              stock_data['Volume'].values[0]])
        else:
            print(f"No data available for {symbol} on {yesterday}")

    # Create a DataFrame from the collected data
    columns = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Price', 'Volume']
    result_df = pd.DataFrame(data_list, columns=columns)

    result_df.to_csv(index=False)

    # Create a StringIO object and write CSV data to it
    csv_buffer = StringIO()
    result_df.to_csv(csv_buffer, index=False)

    # Reset the buffer position to the beginning
    csv_buffer.seek(0)

    # Load to Google Cloud
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ubuntu/airflow/keys/wallstreetwizzards-9e6fdbabdbfb.json"
        storage_client = storage.Client()
        bucket_name = "wallstreetwizzards_bucket"
        blob_name = f"prices/prices - {yesterday}"
        bucket = storage_client.get_bucket(bucket_name)
        blob = Blob(blob_name, bucket)
        blob.upload_from_file(csv_buffer, content_type='text/csv')
        return True

    except Exception as e:
        print(e)
        return False


def datalake_to_datawarehouse():
    # Credentials
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ubuntu/airflow/keys/wallstreetwizzards-9e6fdbabdbfb.json"
    storage_client = storage.Client()
    t = datetime.now()
    t_m1 = t - timedelta(days=1)
    yesterday = t_m1.strftime('%Y-%m-%d')
    blob_name = f"prices/prices - {yesterday}"
    bucket_name = "wallstreetwizzards_bucket"

    # Read csv from bucket
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        content = blob.download_as_bytes()
        df = pd.read_csv(BytesIO(content))

    except Exception as e:
        print(e)
        return None

    # Establish Postgre connection
    try:
        config_path = '/home/ubuntu/airflow/keys/config.ini'
        config = configparser.ConfigParser()
        config.read(config_path)

        database_config = {
            'host': config.get('database', 'host'),
            'database': config.get('database', 'database'),
            'user': config.get('database', 'user'),
            'password': config.get('database', 'password')
        }
        print("database_config: ", database_config)

        # Establish a connection to the database
        connection = psycopg2.connect(**database_config)
        print("Connected to the database.")

    except Exception as e:
        print(e)
        return None

    # Create schema if not exists
    try:
        cursor = connection.cursor()
        create_schema_query = "CREATE SCHEMA IF NOT EXISTS yfinance;"
        cursor.execute(create_schema_query)
        connection.commit()
        cursor.close()

    except Exception as e:
        print(e)
        connection.close()
        return None

    # Create table if not exists
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS yfinance.hist_prices (         
            symbol VARCHAR(6),
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            price FLOAT,
            volume FLOAT,
            PRIMARY KEY (symbol, date)
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()

    except Exception as e:
        print(e)
        connection.close()  # Close the connection on error
        return None

    # Insert data
    try:
        print(df)
        print("DF?")
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO yfinance.hist_prices (symbol, date, open, high, low, price, volume) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, date) DO UPDATE 
            SET open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                price = EXCLUDED.price,
                volume = EXCLUDED.volume;
        """
        values = [tuple(row) for row in df.values]
        cursor.executemany(insert_query, values)
        connection.commit()

    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()

# Specify default args of the dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Set the start date to the desired date and time (5:00 AM)
start_date = datetime(2023, 11, 26, 5, 0, 0)

# Instantiate the DAG
dag = DAG(
    'yfinance_prices_yesterday',
    default_args=default_args,
    description='DAG for prices from yesterday (closing)',
    schedule_interval="@daily",  # Set your desired interval
    start_date=start_date,
)

# Task 1: Get components
task_get_components = PythonOperator(
    task_id='get_components',
    python_callable=get_components,
    dag=dag,
)

# Task 2: Fetch Prices from API and load into DL
task_get_yesterday_price = PythonOperator(
    task_id='get_yesterday_price',
    python_callable=get_yesterday_price,
    op_args=[task_get_components.output],  # Pass the output of task_generate_list as input
    provide_context=True,  # Set to True to provide context (including the task instance) to the callable
    dag=dag,
)

# Task 3: Get csv from DL and load into DW
task_datalake_to_datawarehouse = PythonOperator(
    task_id='datalake_to_datawarehouse',
    python_callable=datalake_to_datawarehouse,
    dag=dag,
)

# Set the task dependencies
task_get_components >> task_get_yesterday_price >> task_datalake_to_datawarehouse