# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = "Banks.db"
table_name = "Largest_banks"
csv_path = "./Largest_banks_data.csv"


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            # Get bank name (from link if present, otherwise plain text)
            if col[1].find('a') is not None:
                bank_name = col[1].a.get_text(strip=True)
            else:
                bank_name = col[1].get_text(strip=True)

            mc_usd_raw = col[2].get_text(strip=True)

            # Skip rows where mc_usd_raw is not a number
            try:
                # Try to convert to float after removing commas
                _ = float(mc_usd_raw.replace(',', ''))
            except ValueError:
                continue  # Skip this row

            data_dict = {"Name": bank_name,
                         "MC_USD_Billion": mc_usd_raw}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df, exchange_rate_path):
    # Clean USD values (remove commas and convert to float)
    df["MC_USD_Billion"] = df["MC_USD_Billion"].replace(',', '', regex=True).astype(float)

    # Read exchange rate CSV into dict
    rates_df = pd.read_csv(exchange_rate_path)
    exchange_rate = dict(zip(rates_df['Currency'], rates_df['Rate']))

    # Add new columns using list comprehensions
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    return df
    


def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


log_progress('Preliminaries complete. Initiating ETL process')

# Extraction
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

# Transformation
df = transform(df, exchange_rate_path)
print(df)
log_progress('Data transformation complete. Initiating loading process')

# Debug info - print number of rows & sample data
print(f"Number of rows extracted: {len(df)}")
print(df[['Name', 'MC_USD_Billion', 'MC_EUR_Billion']])

# Print 5th bank EUR market cap safely
if len(df) > 4:
    print("MC_EUR_Billion of 5th largest bank:", df['MC_EUR_Billion'][4])
else:
    print("Less than 5 banks found - cannot print 5th bank's EUR value.")

# Load to CSV
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

# Load to DB
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

# Use a query guaranteed to return results - show top 5 by GBP market cap
query_statement = f"SELECT Name, MC_GBP_Billion FROM {table_name} ORDER BY MC_GBP_Billion DESC LIMIT 5"
run_query(query_statement, sql_connection)

run_query("SELECT * FROM Largest_banks", sql_connection)

# 2. Print average market cap in GBP
run_query("SELECT AVG(MC_GBP_Billion) AS Avg_Market_Cap_GBP FROM Largest_banks", sql_connection)

# 3. Print only names of top 5 banks
run_query("SELECT Name FROM Largest_banks LIMIT 5", sql_connection)
log_progress('Process Complete.')
sql_connection.close()
