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
    """Get the index components from the S&P 500 from Wikipedia and load them into Data Lake"""

    # Get the current date
    current_date = datetime.now()
    t = current_date.strftime("%Y-%m-%d")

    # Get current S&P 500 components from Wikipedia
    components = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]

    # Replace points with hyphen because of API-Call format requirements
    components['Symbol'] = components['Symbol'].str.replace('.', '-')
    n = len(components)

    if n >= 500:
        print("Successfully got the components of the S&P 500 from Wikipedia!")
        symbols = components["Symbol"].tolist()

        # Load S&P 500 components to Google Cloud
        components.to_csv(index=False)

        # Create a StringIO object and write CSV data to it
        csv_buffer = StringIO()
        components.to_csv(csv_buffer, index=False)

        # Reset the buffer position to the beginning
        csv_buffer.seek(0)

        # Load to Google Cloud Data Lake
        try:
            os.environ[
                "GOOGLE_APPLICATION_CREDENTIALS"] = "/home/ubuntu/airflow/keys/wallstreetwizzards-9e6fdbabdbfb.json"
            storage_client = storage.Client()
            bucket_name = "wallstreetwizzards_bucket"
            blob_name = f"components/components - {t}"
            bucket = storage_client.get_bucket(bucket_name)
            blob = Blob(blob_name, bucket)
            blob.upload_from_file(csv_buffer, content_type='text/csv')
            return symbols

        except Exception as e:
            print(e)

    else:
        print(f"There is an error while loading index components of the S&P 500! The length is {n} instead of >= 500.")
        return None


def get_current_price(symbol_list):
    """Get the real-time price for S&P 500 components"""

    # Initialize an empty list to store data
    data_list = []

    # Get the current date
    current_date = datetime.now()
    t = current_date.strftime("%Y-%m-%d")

    # Loop through each symbol in the list and get current stock price
    for symbol in symbol_list:
        print(symbol)
        ticker_yahoo = yf.Ticker(symbol)
        data = ticker_yahoo.history()

        try:
            # Get the current price
            last_quote = data['Close'].iloc[-1]

        except IndexError:
            print(f"Error getting price for {symbol}. Setting price to 'n/a'.")
            last_quote = None

        # Append data to the list
        data_list.append({
            'Date': t,
            'Symbol': symbol,
            'Last Price': last_quote
        })

    # Create a DataFrame from the list of data
    result_df = pd.DataFrame(data_list)
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
        blob_name = f"prices/prices - {t}"
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
    current_date = datetime.now()
    t = current_date.strftime("%Y-%m-%d")
    blob_name = f"prices/prices - {t}"
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

    # Establish connection to Postgres Database
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
        clear_table_query = 'TRUNCATE TABLE yfinance.prices;'
        cursor.execute(clear_table_query)

        create_table_query = """
        CREATE TABLE IF NOT EXISTS yfinance.prices (
            -- Define your table columns here based on your DataFrame columns
            symbol VARCHAR(6),
            date DATE,
            price FLOAT,
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
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO yfinance.prices (date, symbol, price) VALUES (%s, %s, %s)
            ON CONFLICT (symbol, date) DO UPDATE SET price = EXCLUDED.price;
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
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

start_date = datetime(2023, 11, 26, 0, 0, 0)

# Instantiate the DAG
dag = DAG(
    'yfinance_prices_current',
    default_args=default_args,
    description='DAG for current prices (intraday)',
    schedule="@hourly",
    start_date=start_date
)

# Task 1: Get components
task_get_components = PythonOperator(
    task_id='get_components',
    python_callable=get_components,
    dag=dag,
)

# Task 2: Fetch prices from API and load into DL
task_get_current_price = PythonOperator(
    task_id='get_current_price',
    python_callable=get_current_price,
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

# Task dependencies
task_get_components >> task_get_current_price >> task_datalake_to_datawarehouse