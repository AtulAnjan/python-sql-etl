# Checking if python environment belongs to correct env
!which python

!pip install kaggle

# using kaggle api to download the dataset
import kaggle
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd
!kaggle datasets download ankitbansal06/retail-orders -f orders.csv

# Unzipping the downloaded dataset
import zipfile
zip_ref = zipfile.ZipFile('./orders.csv.zip')
zip_ref.extractall()
zip_ref.close()

# required library to connect to sql server
pip install sqlalchemy pyodbc

df = pd.read_csv('./orders.csv')

# handling the null values of column "Ship Mode"
df = pd.read_csv('./orders.csv', na_values = ['Not Available', 'unknown'])

# rename the columns to maintain the standard
df.rename(columns={"Ship Mode":"ship_mode"}, inplace = True)
# another way to rename column
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace(" ","_")

# creating the required columns
df['discount'] = df['list_price']*df['discount_percent']*0.01
df['sale_price'] = df['list_price'] - df['discount']
df['profit'] = df['sale_price'] - df['cost_price']

# Changing the datatype of order_date column
df['order_date'] = pd.to_datetime(df['order_date'], format="%Y-%m-%d")

# Dropping unnecessary columns from dataframe
df.drop(columns = ['cost_price', 'list_price', 'discount_percent'], inplace = True)

df.drop(columns=['discount_price'], inplace=True)

# password contains "@" and thats why need to replace it with "%40" in the connection string to establish the connection

connection_string = (
    "mssql+pyodbc://sa:Atul%40007@localhost:1433/master?"
    "driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes"
)

engine = create_engine(connection_string)

conn = engine.connect()

# Loading the data to sql server table
# used append because table is already created else use replace
df.to_sql('df_orders', con=conn, if_exists='append', index=False)