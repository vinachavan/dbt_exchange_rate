import pandas as pd
import os
import unicodedata
from datetime import datetime, timedelta
import psycopg2
import sqlalchemy
import time
import configparser
config = configparser.ConfigParser()
config.read('dbt_config')
HOSTNAME = os.environ['DBT_HOST']
USERNAME = os.environ['DBT_USER']
PASSWORD = os.environ['DBT_PASS']
DATABASE = config['dev']['database']
PORT = config['dev']['port']

# URL="https://www.cnb.cz/cs/financni-trhy/devizovy-trh/kurzy-devizoveho-trhu/kurzy-devizoveho-trhu/denni_kurz.txt?date="
URL = config['dev']['api_url']
##proper commenting
class ExchangeRateDatesCZ:
    def __init__(self, table_name='exchange_rates'):
        #initial values to be set
        self.table_name = table_name
        self.redshift_connection_params={
            'host': HOSTNAME,
            'port': PORT,
            'user': USERNAME,
            'password': PASSWORD,
            'database': DATABASE
        }
    
    # to get the data from API based on date
    def get_exchange_rate(self, date):
        #reading the csv file into dataframe of pandas library 
        df = pd.read_csv(f"{URL}{date}", skiprows = 1, delimiter='|')
        return df        

    #Get weekdays dates only 
    def weekdays_from_3months(self):
        #choose start and end date for getting exchange data information 
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=90)
        weekdays_dates = []
        current_date = start_date
        #considering only weekdays
        while current_date <= end_date:
            if current_date.weekday() < 5 :
                weekdays_dates.append(current_date.strftime("%d.%m.%Y"))
            
            current_date += timedelta(days=1)
        return weekdays_dates

    #Get the exchange data for 3 months and store it into Reshift
    def load_3month_data(self):
        #call the method to get the dates
        datefromthreemonths = self.weekdays_from_3months()
        #emtry dataframe to store all the data based on date
        result_df = pd.DataFrame()
        for date in datefromthreemonths:
            df = self.get_exchange_rate(date)
            #adding another column to check the exchange date
            df['exchange_date'] = date
            df['kurz'] = pd.to_numeric(df['kurz'].str.replace(',', ''), errors='coerce')
            #appenging all dataframes in sigle set
            result_df = pd.concat([result_df, df], ignore_index=True)
        
        print(result_df)
        
        #create connection to redshift
        engine = sqlalchemy.create_engine(f'postgresql://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}')
        #store data into redshift
        result_df.to_sql(name=self.table_name, con=engine, if_exists='replace', index=False, schema='public')

# unit testing for module
if __name__ == '__main__':
    
    date = "16.01.2024"
    start = time.perf_counter()
    erd = ExchangeRateDatesCZ()
    erd.load_3month_data()
    end = time.perf_counter() - start
    print(f"Program finished in {end:0.2f} seconds.")

    # rates = erd.get_exchange_rate(date)
    # print(rates.columns.values)
    # print(rates)