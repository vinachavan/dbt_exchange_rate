'''
@Auther - Vinayak Chavan
@Date - 18-Jan-2024
@Description - This will fetch exchange data for Czech Republic and store it into database
'''
import pandas as pd
import os
import unicodedata
from datetime import datetime, timedelta
import psycopg2
import sqlalchemy
import time
import configparser

# Reading configuration file
config = configparser.ConfigParser()
config.read('dbt_config')
HOSTNAME = os.environ['DBT_HOST']
USERNAME = os.environ['DBT_USER']
PASSWORD = os.environ['DBT_PASS']
DATABASE = config['dev']['database']
PORT = config['dev']['port']
URL = config['dev']['api_url']


# This class is responsible to fetch the data from API i.e.
# (https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/kurzy-devizoveho-trhu/denni_kurz.txt?date=16.01.2024)
# and store it into the database.
# 
class ExchangeRateDatesCZ:
    def __init__(self, date=None):
        # Initial values to be set
        self.table_name = config['dev']['table_name']
        self.date = date
    
    # To get the exchange rate data from API based on date
    # Retuning response from API in padas DataFrame object
    def get_exchange_rate(self, date):
        # Reading the csv file into dataframe of pandas library 
        df=pd.DataFrame()
        try:
            df = pd.read_csv(f"{URL}{date}", skiprows = 1, delimiter='|')
        except Exception as e:
            print ('Caught exception while calling API:', e) 
            raise e
        return df

    # As we can see there are only exchange rate data 
    # available for weekdays, so pulling only weekdays date 
    def weekdays_from_3months(self):
        # Choose start and end date for getting exchange
        # data information 
        end_date = datetime.now()
        if self.date == None:
            end_date = datetime.now() - timedelta(days=1)
        else:
            end_date = datetime.strptime(self.date, "%d.%m.%Y")
        # Pulling 3 months back date
        start_date = end_date - timedelta(days=90)
        weekdays_dates = []
        current_date = start_date
        # Fetching all the dates from last 3 months and 
        # putting into list
        while current_date <= end_date:
            # Checking for weekdays date
            if current_date.weekday() < 5 :
                weekdays_dates.append(current_date.strftime("%d.%m.%Y"))
            
            current_date += timedelta(days=1)
        return weekdays_dates

    # Get the exchange data from last 3 months and 
    # store it into Reshift database
    def load_3month_data(self):
        try:
            # Call the method to get the dates
            datefromthreemonths = self.weekdays_from_3months()
            # Empty dataframe to store all the data based on date
            result_df = pd.DataFrame()
            for date in datefromthreemonths:
                # Pulling exchange rate for each day
                df = self.get_exchange_rate(date)
                # Adding another column to check the exchange date
                df['exchange_date'] = date
                df['kurz'] = pd.to_numeric(df['kurz'].str.replace(',', ''), errors='coerce')
                # Appending all dataframes in sigle set
                result_df = pd.concat([result_df, df], ignore_index=True)
            
            print(result_df)
            
            # Create connection to redshift
            engine = sqlalchemy.create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}')
            # Store data into redshift
            result_df.to_sql(name=self.table_name, con=engine, if_exists='replace', index=False, schema='public')
        except Exception as e:
            print ('Caught exception while processing:', e) 
            raise e
        # Call the method to get the dates
        datefromthreemonths = self.weekdays_from_3months()
        # Empty dataframe to store all the data based on date
        result_df = pd.DataFrame()
        for date in datefromthreemonths:
            # Pulling exchange rate for each day
            df = self.get_exchange_rate(date)
            # Adding another column to check the exchange date
            df['exchange_date'] = date
            df['kurz'] = pd.to_numeric(df['kurz'].str.replace(',', ''), errors='coerce')
            # Appending all dataframes in sigle set
            result_df = pd.concat([result_df, df], ignore_index=True)
        
        print(result_df)
        
        # Create connection to redshift
        engine = sqlalchemy.create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}')
        # Store data into redshift
        result_df.to_sql(name=self.table_name, con=engine, if_exists='replace', index=False, schema='public')

# Unit testing for module
if __name__ == '__main__':
    
    date = "16.01.2024"
    start = time.perf_counter()
    # With Parameter
    # erd = ExchangeRateDatesCZ(date)
    # Without Parameter
    erd = ExchangeRateDatesCZ()
    erd.load_3month_data()
    end = time.perf_counter() - start
    print(f"Program finished in {end:0.2f} seconds.")

    # rates = erd.get_exchange_rate(date)
    # print(rates.columns.values)
    # print(rates)