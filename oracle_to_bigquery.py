#oracle to bigquery

import convert_datatypes
import auth_oracle
import numpy as np
import pandas as pd
import authenticate_credentials
from google.cloud import bigquery
import os
import pandas_gbq

#setting credentials as an environment variable
credentials_path = "cred_location"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

#target table and project in bigquery, update this according to where the table should be in bigquery
table_id = "table_id"
project_id = "project_id"

#authenticating and downloading the data from oracle
try:
    df_oracle = auth_oracle.auth_oracle()
    print("Data successfully downloaded from Oracle")

    #adding in a load date to the data
    df_oracle["UT_LOAD_DT"] = pd.Timestamp.today()

    print(df_oracle.head())
    print(df_oracle.dtypes)
except:
    print("Data not downloaded from Oracle")

#authenticating bigquery
client = authenticate_credentials.main()

#defining the schema for bigquery
schema = convert_datatypes.python_to_bigquery(df_oracle)

#print(oracle_datatype_mapping_table)
#print(schema)

#loading the schema to the jobconfig variable to pass to bigquery
job_config = bigquery.LoadJobConfig()
job_config.schema=schema

#delete table if it exists
client.delete_table(table_id,not_found_ok=True)

#loading the table from the pandas dataframe to bigquery
try:
    job = client.load_table_from_dataframe(
        df_oracle, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
except:
    print("Data not loaded to Bigquery")