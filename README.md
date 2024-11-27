This is a small project which does the following:
Using kaggle pubic api, it downloads the zip file using python script
Then it extracts the data and import using pandas.
Then data cleansing takes place.
After all the cleansing of data, we establish connection to sql server using sqlalchemy and pyodbc.
After establishing connection, we load the data into the table.
After loading the data, we analyze the data using different sql scenarios and quesries.
