import configparser
import requests
import pandas as pd
import os
from google.cloud import storage
import json
from datetime import datetime, timedelta
import time
import csv
from io import StringIO
import psycopg2




os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'wallstreetwizzards-5e5842786425.json'


config_path = 'config.ini'
config = configparser.ConfigParser()
config.read(config_path)

user = config.get('database', 'user')
password = config.get('database', 'password')
host = config.get('database', 'host')
port = config.get('database', 'port')
database = config.get('database', 'database')
key = config.get('alphavantage', 'key')
bucket_name = config.get('bucket', 'bucket_name')


# Download JSON file from the bucket
def download_csv_from_bucket(blob_name, bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Download blob content as a byte string
    blob_content = blob.download_as_string()
    
    # Decode byte string to regular string (assuming it contains CSV)
    csv_str = blob_content.decode('utf-8')
    
    # Create a StringIO object to work with CSV module
    csv_io = StringIO(csv_str)
    
    # Use CSV module to read the string as CSV
    csv_reader = csv.reader(csv_io)
    
    # Convert CSV reader object to a list of rows
    csv_data = list(csv_reader)
    
    return csv_data

def extract_column_as_list(csv_data, column_index):
    column_list = []
    for row_index, row in enumerate(csv_data):
        if row_index != 0:  # Skip the first row (index 0)
            column_list.append(row[column_index])
    return column_list

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




def generate_date_strings():
    # Assuming you want the current date and the previous day
    current_date = datetime.now().strftime("%Y%m%d")

    return current_date


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
        CREATE TABLE IF NOT EXISTS alphavantage.metrics (
            symbol TEXT,
            asset_type TEXT,
            name TEXT,
            cik TEXT,
            exchange TEXT,
            currency TEXT,
            country TEXT,
            sector TEXT,
            industry TEXT,
            address TEXT,
            fiscal_year_end TEXT,
            latest_quarter DATE,
            market_capitalization NUMERIC,
            ebitda NUMERIC,
            pe_ratio NUMERIC,
            peg_ratio NUMERIC,
            book_value NUMERIC,
            dividend_per_share NUMERIC,
            dividend_yield NUMERIC,
            eps NUMERIC,
            revenue_per_share_ttm NUMERIC,
            profit_margin NUMERIC,
            operating_margin_ttm NUMERIC,
            return_on_assets_ttm NUMERIC,
            return_on_equity_ttm NUMERIC,
            revenue_ttm NUMERIC,
            gross_profit_ttm NUMERIC,
            diluted_eps_ttm NUMERIC,
            quarterly_earnings_growth_yoy NUMERIC,
            quarterly_revenue_growth_yoy NUMERIC,
            analyst_target_price NUMERIC,
            trailing_pe NUMERIC,
            forward_pe NUMERIC,
            price_to_sales_ratio_ttm NUMERIC,
            price_to_book_ratio NUMERIC,
            ev_to_revenue NUMERIC,
            ev_to_ebitda NUMERIC,
            beta NUMERIC,
            fifty_two_week_high NUMERIC,
            fifty_two_week_low NUMERIC,
            fifty_day_moving_average NUMERIC,
            two_hundred_day_moving_average NUMERIC,
            shares_outstanding NUMERIC,
            dividend_date DATE,
            ex_dividend_date DATE
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
    INSERT INTO alphavantage.metrics (
        symbol,
        asset_type, 
        name, 
        cik, 
        exchange, 
        currency, 
        country, 
        sector, 
        industry, 
        address,
        fiscal_year_end, 
        latest_quarter, 
        market_capitalization, 
        ebitda, 
        pe_ratio, 
        peg_ratio, 
        book_value,
        dividend_per_share, 
        dividend_yield, 
        eps, 
        revenue_per_share_ttm, 
        profit_margin, 
        operating_margin_ttm,
        return_on_assets_ttm, 
        return_on_equity_ttm, 
        revenue_ttm, 
        gross_profit_ttm, 
        diluted_eps_ttm,
        quarterly_earnings_growth_yoy, 
        quarterly_revenue_growth_yoy, 
        analyst_target_price, 
        trailing_pe,
        forward_pe, 
        price_to_sales_ratio_ttm, 
        price_to_book_ratio, 
        ev_to_revenue, 
        ev_to_ebitda, 
        beta,
        fifty_two_week_high, 
        fifty_two_week_low, 
        fifty_day_moving_average, 
        two_hundred_day_moving_average,
        shares_outstanding, 
        dividend_date, 
        ex_dividend_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    try:
        for item in data:
            for key, value in item.items():
                if value == 'None':
                    item[key] = None
                elif value == '-':
                    item[key] = None
            cursor.execute(insert_query, (
                item['symbol'],
                item['asset_type'],
                item['name'],
                item['cik'],
                item['exchange'],
                item['currency'],
                item['country'],
                item['sector'],
                item['industry'],
                item['address'],
                item['fiscal_year_end'],
                item['latest_quarter'],
                item['market_capitalization'],
                item['ebitda'], 
                item['pe_ratio'], 
                item['peg_ratio'], 
                item['book_value'], 
                item['dividend_per_share'],
                item['dividend_yield'], 
                item['eps'], 
                item['revenue_per_share_ttm'], 
                item['profit_margin'],
                item['operating_margin_ttm'], 
                item['return_on_assets_ttm'], 
                item['return_on_equity_ttm'],
                item['revenue_ttm'], 
                item['gross_profit_ttm'], 
                item['diluted_eps_ttm'],
                item['quarterly_earnings_growth_yoy'], 
                item['quarterly_revenue_growth_yoy'],
                item['analyst_target_price'], 
                item['trailing_pe'], 
                item['forward_pe'], 
                item['price_to_sales_ratio_ttm'],
                item['price_to_book_ratio'], 
                item['ev_to_revenue'], 
                item['ev_to_ebitda'], 
                item['beta'],
                item['fifty_two_week_high'], 
                item['fifty_two_week_low'], 
                item['fifty_day_moving_average'],
                item['two_hundred_day_moving_average'], 
                item['shares_outstanding'],
                item['dividend_date'], 
                item['ex_dividend_date']
            ))
        connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data into PostgreSQL:", error)
    finally:
        if connection:
            cursor.close()
        

def print_data_from_table(connection):
    cursor = connection.cursor()
    select_query = "SELECT * from alphavantage.metrics" 
    
    cursor.execute(select_query)
    rows = cursor.fetchall()
    
    for row in rows:
        print(row)

    cursor.close()


def main():

    #Get date strings

    current_date = generate_date_strings()
    current_datetime = datetime.now()

    # Format the datetime object to the desired string format
    component_datetime = current_datetime.strftime("%Y-%m-%d")

    csv_data = download_csv_from_bucket(f'components/components - {component_datetime}', f'{bucket_name}')

    # Assuming you want to extract the first column (index 0)
    desired_column_index = 0

    # Extract the desired column as a list
    symbols = extract_column_as_list(csv_data, desired_column_index)

    all_data = []

    for symbol in symbols:
        url = f'https://www.alphavantage.co/query?function=OVERVIEW&apikey={key}&symbol={symbol}'

        r = requests.get(url)
        data = r.json()
        all_data.append(data)
        print(data)
        time.sleep(1)

    combined_data = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "data": all_data
    }
   
    upload_json_to_bucket(f'metrics_{current_date}.json', combined_data, bucket_name, folder_path='metrics')


    # Download JSON file from the bucket to a local file
    downloaded_json = download_json_from_bucket(f'metrics/metrics_{current_date}.json', f'{bucket_name}')

    # Extract and structure data for database insertion
    data_for_database = []

    for entry in downloaded_json['data']:
    
        latest_quarter_str = entry.get('LatestQuarter')
        dividend_date_str = entry.get('DividendDate')
        ex_dividend_date_str = entry.get('ExDividendDate')
        print(latest_quarter_str)
        
        # Convert date strings to dates if they are not 'None'
        latest_quarter = datetime.strptime(latest_quarter_str, '%Y-%m-%d').date() if latest_quarter_str and latest_quarter_str != 'None' else None
        dividend_date = datetime.strptime(dividend_date_str, '%Y-%m-%d').date() if dividend_date_str and dividend_date_str != 'None' else None
        ex_dividend_date = datetime.strptime(ex_dividend_date_str, '%Y-%m-%d').date() if ex_dividend_date_str and ex_dividend_date_str != 'None' else None


        ticker_data = {
            'symbol': entry.get('Symbol', ''),
            'asset_type': entry.get('AssetType', ''),
            'name': entry.get('Name', ''),
            'cik': entry.get('CIK', ''),
            'exchange': entry.get('Exchange', ''),
            'currency': entry.get('Currency', ''),
            'country': entry.get('Country', ''),
            'sector': entry.get('Sector', ''),
            'industry': entry.get('Industry', ''),
            'address': entry.get('Address', ''),
            'fiscal_year_end': entry.get('FiscalYearEnd', ''),
            'latest_quarter': latest_quarter,
            'market_capitalization': entry.get('MarketCapitalization', 0.0),
            'ebitda': entry.get('EBITDA', 0.0),
            'pe_ratio': entry.get('PERatio', 0.0),
            'peg_ratio': entry.get('PEGRatio', 0.0),
            'book_value': entry.get('BookValue', 0.0),
            'dividend_per_share': entry.get('DividendPerShare', 0.0),
            'dividend_yield': entry.get('DividendYield', 0.0),
            'eps': entry.get('EPS', 0.0),
            'revenue_per_share_ttm': entry.get('RevenuePerShareTTM', 0.0),
            'profit_margin': entry.get('ProfitMargin', 0.0),
            'operating_margin_ttm': entry.get('OperatingMarginTTM', 0.0),
            'return_on_assets_ttm': entry.get('ReturnOnAssetsTTM', 0.0),
            'return_on_equity_ttm': entry.get('ReturnOnEquityTTM', 0.0),
            'revenue_ttm': entry.get('RevenueTTM', 0.0),
            'gross_profit_ttm': entry.get('GrossProfitTTM', 0.0),
            'diluted_eps_ttm': entry.get('DilutedEPSTTM', 0.0),
            'quarterly_earnings_growth_yoy': entry.get('QuarterlyEarningsGrowthYOY', 0.0),
            'quarterly_revenue_growth_yoy': entry.get('QuarterlyRevenueGrowthYOY', 0.0),
            'analyst_target_price': entry.get('AnalystTargetPrice', 0.0),
            'trailing_pe': entry.get('TrailingPE', 0.0),
            'forward_pe': entry.get('ForwardPE', 0.0),
            'price_to_sales_ratio_ttm': entry.get('PriceToSalesRatioTTM', 0.0),
            'price_to_book_ratio': entry.get('PriceToBookRatio', 0.0),
            'ev_to_revenue': entry.get('EVToRevenue', 0.0),
            'ev_to_ebitda': entry.get('EVToEBITDA', 0.0),
            'beta': entry.get('Beta', 0.0),
            'fifty_two_week_high': entry.get('52WeekHigh', 0.0),
            'fifty_two_week_low': entry.get('52WeekLow', 0.0),
            'fifty_day_moving_average': entry.get('50DayMovingAverage', 0.0),
            'two_hundred_day_moving_average': entry.get('200DayMovingAverage', 0.0),
            'shares_outstanding': entry.get('SharesOutstanding', 0.0),
            'dividend_date': dividend_date,
            'ex_dividend_date': ex_dividend_date,
                }
        data_for_database.append(ticker_data)

    # Connect to Cloud SQL PostgreSQL
    connection = connect_to_cloudsql()


    # Create table if it doesn't exist
    create_table_if_not_exists(connection)

    # Clear data from the 'alphavantage.news' table
    clear_table(connection, 'alphavantage.metrics')
    
    # # Insert JSON data into PostgreSQL
    insert_data_into_postgresql(connection, data_for_database)

    
    # # Close the database connection
    connection.close()

if __name__ == "__main__":
    main()