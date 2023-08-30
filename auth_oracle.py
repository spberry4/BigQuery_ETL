import sqlalchemy
from sqlalchemy.engine import create_engine
import pandas as pd
from oracle_query import query_oracle
import cx_Oracle
import numpy as np


#authenticate oracle and returns a dataframe
def auth_oracle():
    engine = create_engine("sqlalchemy string")

    metadata = sqlalchemy.MetaData(engine)
    print(metadata)

    df_oracle = pd.read_sql(query_oracle, con = engine)

    return df_oracle
