import pandas as pd
import os
import authenticate_credentials
from bigquery_query import table_id, query
import convert_datatypes

#This program is meant to authenticate bigquery and pull in data from the table being queried
#It then converts the bigquery datatypes to python datatypes and writes the resulting dataframe to a parquert file

#setting credentials as an environment variable
credentials_path = "C:/Users/seanberry/Desktop/bigquery/private_key_big_query.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

#authentication function
client = authenticate_credentials.main()

#Getting table properties
table = client.get_table(table_id)

# View table properties
print(f"Got table '{table.project}.{table.dataset_id}.{table.table_id}'.")
print(f"Table schema: {table.schema}")
print(f"Table description: {table.description}")
print(f"Table has {table.num_rows} rows")

# Execute the query
query_job = client.query(query)

# Get the results
results = query_job.result()

#Coverting the query to a dataframe
df = results.to_dataframe()

#converting to pandas/python datatypes from bigquery
df = convert_datatypes.bigquery_to_pandas(table, client, df)

#printing out the datatypes and writing a parquet file for analysis
print(df.dtypes)
df.to_parquet("data.parquet")