import sqlalchemy
from sqlalchemy.engine import create_engine
import pandas as pd
from oracle_query import query_oracle, password, username, service_name, server_name
import cx_Oracle
import numpy as np


#authenticate oracle and returns a dataframe
def auth_oracle():
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{server_name}/?service_name={service_name}")

    metadata = sqlalchemy.MetaData(engine)
    print(metadata)

    df_oracle = pd.read_sql(query_oracle, con = engine)

    return df_oracle

#df_oracle = auth_oracle()
#print(df_oracle)
