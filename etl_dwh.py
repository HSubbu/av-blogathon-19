#importing necessary libraries
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import warnings
warnings.filterwarnings('ignore')
from clean_book import clean_book_genre

#reading from config file
config = {}
exec(open('config.py').read(), config)
user = config['user']
password = config['password']
port = config['port']
db = config['db']

# Connect to the PostgreSQL database server
engine = create_engine(f'postgresql://{user}:{password}@localhost/{db}')
conn = engine.connect()

# read transactional csv file as a dataframe
df_sales= pd.read_csv("book_sale.csv")
#read city-region from region.csv
region = pd.read_csv('region.csv')


# creata a dataframe for book dimension table
book_id = 10000 # the book_id starts from this number 
df_book = df_sales[['book','genre']]
df_book.drop_duplicates(keep='first', inplace=True)
#in case book and genre is not matching for all rows call function to clean df_book
if (df_book.shape[0] != df_book['book'].nunique()):
    df_book = clean_book_genre(df_book)
df_book['bookid'] = pd.RangeIndex(book_id, book_id + len(df_book)) + 1
#load data into tables
df_book.to_sql(name="DimBook", con=conn, schema="public", if_exists="append", index=False)
print("Book Dimension Table data loading completed")


#create a dataframe for location dimendion table
location_id = 50000
df_location = df_sales[['store']]
df_location.rename(columns = {'store':'city'}, inplace = True)
df_location.drop_duplicates(keep='first', inplace=True)
df_location['locationid'] = pd.RangeIndex(location_id, location_id + len(df_location)) + 1
df_location= pd.merge(df_location, region, on=['city'])
#load data into table
df_location.to_sql(name="DimLocation" ,con=conn,schema="public", if_exists='append',index=False)
print("Location  Dimension Table data loading completed")


#creata a fact table
fact_id = 0
df_fact = df_sales.copy()
df_fact.rename(columns = {'date':'dateid'}, inplace = True)
df_fact['rowid'] = pd.RangeIndex(fact_id, fact_id + len(df_fact)) + 1 # rowid for fact table
df_location.rename(columns = {'city':'store'}, inplace = True) # to match fact and dim table columns
df_fact= pd.merge(df_fact, df_location,how='left', on=['store']) # merge factsale and dimlocation tables
df_fact= pd.merge(df_fact, df_book,how='left', on=['book']) # merge factsale ad dimbook tables
df_fact = df_fact[['rowid','bookid','locationid','dateid','sale']] # filter factsale as per schema defineds
#load data into fact table
df_fact.to_sql(name="FactSale", con=conn, schema="public",if_exists='append',index=False)
print("Fact Table data loading completed")