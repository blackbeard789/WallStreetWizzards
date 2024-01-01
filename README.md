# WallStreetWizzards

## Overview
The Financial Market Data Processing Project aims to harness the vast amount of available financial data to empower more informed decision-making in the financial industry. This project focuses on integrating diverse sources of stock market data into a robust data infrastructure, employing data warehousing and data lake technologies for efficient management and analysis.

## Goals
### Objectives
The overall goal is to be able to make smart use of the information overload in the financial industry through sophisticated practices for data-based decision-making. The project's purpose therefore is to create a robust data infrastructure by combining data warehousing and data lake technologies, enabling efficient management of vast amounts of stock market data. The objective is further to establish a unified platform capable of storing, processing, and analysing diverse stock market data sources. For this purpose, the three research questions mentioned in chapter 1.5 were formulated, which should help to solve the problem.

### Addressees
The addressees of the results of the work are professionals from the financial sector. On the one hand, the results of the work should shorten decision-making processes in time. On the other hand, the results of the work should also make decisions broader and, above all, more data-driven. The addressees will be shown the versatility of data science tools in combination with public APIs. The disruptive potential of data engineering and data science will be demonstrated in comparison to traditional, often very expensive financial information service providers.

## Data Sources (APIs)
An API can be described as a set of rules and protocols that allows different software applications to communicate with each other. Since the project description specifies working with APIs and the authors want to improve their skills in this area, three APIs are selected to answer the research questions. The first two sources below are dynamic APIs (API returns in every query different data). Additionally, one static API is also used in this work.

### Yahoo Finance (dynamic)
Yahoo Finance is a popular source of stock market data available for retail traders. Price data of many financial instruments can be accessed publicly and free of charge without login. However, clients may be blocked if too many requests are made. In the course of our project work, we have been able to confirm that several thousand enquiries can be made within a short space of time without any problems. Using the {yfinance} package, which was developed by the Python community, it is possible to get the data from Yahoo Finance via API.
Link: https://pypi.org/project/yfinance/
https://github.com/ranaroussi/yfinance

### Alpha Vantage (dynamic)
This API returns live and historical market news as well as sentiment data from a large (and still growing) selection of premier news outlets around the world, covering stocks, cryptocurrencies, forex, and a wide range of topics such as fiscal policy, mergers and acquisitions, IPOs, etc. Additionally, financial data of companies (e.g. dividend yield, EPS, P/E-Ratio) can also be received via API. For this project, we were able to request a free "premium" API key for academic purposes from Alpha Vantage. This means that the number of requests per day is not limited to 90, but an unlimited number of requests can be made.
Link: https://www.alphavantage.co/documentation/#intelligence

### Wikipedia (static)
In order to specify the API requests for Yahoo Finance and Alpha Vantage, the symbols (standardized unique identifier, e.g. “TSLA” for Tesla Inc. stock) for all S&P 500 companies are needed as a function argument for the requests. Therefore, a reference table with all S&P 500 symbols was scraped with the pandas function pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"). This more or less static data (changes in index components occur only a few times a year) is further periodically loaded and updated into the data lake. This ensures that the current index components are always loaded and up to date.
Link: https://en.wikipedia.org/wiki/List_of_S%26P_500_companies


## Data architecture
The project schema and methodology is visualized in the following figure
![image](https://github.com/blackbeard789/WallStreetWizzards/assets/153673259/046425ca-f8c0-4c5d-aa24-83f4590abf81)

## Installation
### Packages
To run this project, you'll need to have the following Python packages installed:

    configparser
    requests
    pandas
    google-cloud-storage
    psycopg2
    yfinance

You can install these packages via pip. Run the following commands in your terminal or command prompt:
```bash
pip install configparser
pip install requests
pip install pandas
pip install google-cloud-storage
pip install psycopg2-binary
pip install yfinance
```


### Apache Airflow
For this project Apache Airflow was used. This have to be set up in the first place, to run the scripts. The documentation can be found here:
https://airflow.apache.org/

#### DAGs
The DAGs have to be inserted in the DAG folder of the Apache Airflow installation in python. The following files are the needed DAGs:
- alphavantage_news_daily.py
- alphavantage_news_hist.py
- yfinance_prices_current.py
- yfinance_prices_yesterday.py


### Metrics
The alphavantage_metrics.py is setup for a onetime execution to insert the data into a sql database. The script can be run in python localy with the right packages installed. 

### Credentials
For the credentials a config.ini file is needed, where the credentials are stored. Additionaly for the google cloud bucket, a json file is needed. This file can be created in the google cloud itself and has to be downloaded

### config.ini file
The file has to be formated in the following form, so it can be correctly ingested:
```
[postgresql]
user = test
password = test
host = test
port = test
database = newsdata
```
## Results
Our goal was to build a user-friendly dashboard in Tableau that enables traders, analysts, and investors to explore data, perform technical and sentiment analysis and get insights. Because we were given a lecture on data visualisation with Tableau in class and learned about the Visual Information Seeking Mantra (VISM 2.0), we took this as our basic framework for the dashboard. Our dashboard is structured according to the overview-first principle, where a rough overview of the big picture should be given first. Secondly, a user should be able to filter and zoom before the desired data can finally be displayed. In concrete terms, this means that a user (e.g. trader, financial analyst) can first open our “Prices and Metrics” Dashboard where a stock is pre-selected (overview). The user can then set the time period as required (zoom) and select another share (filter). Via hover, the peer data of the corresponding sector for the selected stock is also displayed (relate / multi view). For instance, if the user hovers over the Tesla stock, the average metrics for the manufacturing sector is highlighted (Dividend %, EPS, P/E-Ratio, P/B-Ratio). If the user goes over to the “Metrics” Dashboard, he will find specific information on the market sentiment of this stock (details). The public tableau link is provided here: 
https://public.tableau.com/app/profile/claudio.furrer5016/viz/WallStreetWizzard1/Metrics

