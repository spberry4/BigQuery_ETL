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
credentials_path = "insert file path here"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

#target table and project in bigquery
table_id = "table"
project_id = "project"

#authenticating and downloading the data from oracle
try:
    df_oracle = auth_oracle.auth_oracle()
    print("Data successfully downloaded from Oracle")
except:
    print("Data not downloaded from Oracle")

#creating a mapping table, this is actually not needed for this conversion since sqlalchemy and pandas take care of the datatypes
#mapping_table, schema = convert_datatypes.python_to_bigquery(df_oracle)

#authenticating bigquery
client = authenticate_credentials.main()

#writing the data from oracle to bigquery
try:
    df_oracle.to_gbq(table_id, project_id=project_id, if_exists="replace", progress_bar=True)
    print(f"Data has been written to {table_id}")
except:
    print("Data not written")

#print(schema)
#print(df_oracle.dtypes)
#print(df_oracle)