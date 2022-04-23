#importing necessary libraries
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

#reading from config file
config = {}
exec(open('config.py').read(), config)
user = config['user']
password = config['password']
port = config['port']
db = config['db']


# Connect to the PostgreSQL database server
engine = create_engine('postgresql+psycopg2://{user}:{password}@localhost/{db}')
conn_string = f"host='localhost' dbname='{db}' user='{user}' password='{password}'"
conn = psycopg2.connect(conn_string)
# Get cursor object from the database connection
cursor = conn.cursor()

create_facttable = '''
CREATE TABLE public."FactSale"
(
    rowid integer NOT NULL,
    bookID INTEGER,
    locationID INTEGER,
    dateID VARCHAR(300),
    sale INTEGER,
    PRIMARY KEY (rowid)
);'''
create_dimlocation = '''
CREATE TABLE public."DimLocation"
(
    locationID integer NOT NULL,
    city VARCHAR(400),
    region VARCHAR(300),
    PRIMARY KEY (locationID)
);'''
create_dimbook = '''
CREATE TABLE public."DimBook"
(
    bookID integer NOT NULL,
    book VARCHAR(400),
    genre VARCHAR(300) ,
    PRIMARY KEY (bookID)
);'''



sqlGetTableList = "SELECT table_schema,table_name FROM information_schema.tables where table_schema='public' ORDER BY table_schema,table_name ;"

# execute sql queries to create tables and schema
cursor.execute(create_facttable)
cursor.execute(create_dimbook)
cursor.execute(create_dimlocation)

# run query to get tables
cursor.execute(sqlGetTableList)
tables = cursor.fetchall()
# Print the names of the tables
if not tables:
    print("Table List is empty")
for table in tables:
    print(table)

conn.commit()