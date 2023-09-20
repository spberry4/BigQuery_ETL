#SQL Server to Bigquery ETL

import convert_datatypes
import auth_mssql
import numpy as np
import pandas as pd
import authenticate_credentials
from google.cloud import bigquery
import os
import pandas_gbq
from bigquery_query import key_file, write_project_id, write_table_id
from time import process_time

#setting credentials as an environment variable
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_file

#target table and project in bigquery, update this according to where the table should be in bigquery
table_id = write_table_id
project_id = write_project_id

#authenticating and downloading the data from SQl Server
t1_start = process_time()

try:
    df_sql = auth_mssql.auth_mssql()
    print("Data successfully downloaded from SQL Server")
    #df_sql = np.where(df_sql["IsDeleted"] == None, False,df_sql["IsDeleted"])
    #adding in a load date to the data
    df_sql["UT_LOAD_DT"] = pd.Timestamp.today()

    #df_sql = convert_datatypes.sqlserver_exception(df_sql)

    #if there is a nullable bool then it needs to be converted manually
    df_sql["IsDeleted"] = df_sql["IsDeleted"].astype("bool")
    print(df_sql.head())
    print(df_sql.dtypes)
    t1_stop = process_time()
    print(f"Data took {t1_stop-t1_start} seconds to download")

except:
    print("Data not downloaded from SQL Server")

#authenticating bigquery
client = authenticate_credentials.main()

#defining the schema for bigquery
schema = convert_datatypes.python_to_bigquery(df_sql)

#print(oracle_datatype_mapping_table)
#print(schema)
t2_start = process_time()

#loading the schema to the jobconfig variable to pass to bigquery
job_config = bigquery.LoadJobConfig()
job_config.schema=schema

#delete table if it exists
client.delete_table(table_id,not_found_ok=True)

#loading the table from the pandas dataframe to bigquery
try:
    job = client.load_table_from_dataframe(
        df_sql, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    t2_stop = process_time()

    print(f"Data took {t2_stop-t2_start} seconds to upload to Bigquery")

except:
   print("Data not loaded to Bigquery")