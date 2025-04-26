# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
    
    print(timestamp + ' : ' + message + '\n') 


def extract(url, table_attribs):
    # Step 1: Request and parse the page
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    # Step 2: Prepare a list to collect the rows
    records = []
    # Step 3: Find all tbody elements
    tables = data.find_all('tbody')
    # Step 4: Select the correct table (maybe tables[1], but trying tables[0] first)
    rows = tables[0].find_all('tr')
    # Step 5: Loop through the rows
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[1].find('a') is not None and col[2].text.strip() != '—':
                # Extract Bank name
                bank_name = col[1].get_text(strip=True)
                
                # Extract and clean Market Cap
                market_cap_text = col[2].text.strip()
                market_cap_clean = market_cap_text.replace('$', '').replace(',', '').replace('\n', '')
                market_cap_float = float(market_cap_clean)

                # Append the record (as a tuple)
                records.append((bank_name, market_cap_float))
    # Step 6: Create DataFrame at once
    df = pd.DataFrame(records, columns=table_attribs)
    
    return df



def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['Market_Cap_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['Market_Cap_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['Market_Cap_Billion']]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    cursor = sql_connection.cursor()
    print(f"\nRunning Query:\n{query_statement}\n")  # Print the query
    cursor.execute(query_statement)
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    print("\nQuery execution complete.\n")

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "/home/mugheer/Data_Engineering/projects/project2/exchange_rate.csv"
output_path = "/home/mugheer/Data_Engineering/projects/project2/Largest_banks_data.csv"
table_attribs = ["Name", "Market_Cap_Billion"]
db_name = "Banks.db"
table_name = "Largest_banks"
log_progress('Preliminaries complete. Initiating ETL process')

log_progress('Extracting data from website')
df_banks = extract(url, table_attribs)

log_progress('Extracting data from csv and transforming')
df_exchange = transform(df_banks,csv_path)
pd.set_option('display.max_columns', None)
log_progress("Loading dataframe into csv file")
load_to_csv(df_exchange,output_path)
log_progress("Loading sql connection")
sql_connection = sqlite3.connect('Banks.db')
log_progress("Loading dataframe into Database")
load_to_db(df_exchange,sql_connection,table_name)
query1 = "SELECT * FROM Largest_banks"
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query3 = "SELECT Name FROM Largest_banks LIMIT 5"
log_progress("running queries")
run_query(query1,sql_connection)

run_query(query2,sql_connection)

run_query(query3,sql_connection)
log_progress("process completed")
log_progress("server connection closed")
sql_connection.close()

