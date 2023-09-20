import sqlalchemy
from sqlalchemy.engine import create_engine
import pandas as pd
import numpy as np
import pyodbc
from SQLServer_Query import database, sql_query, password, username, host


#authenticate microsoft sql server and returns a dataframe
def auth_mssql():

    connection_url = sqlalchemy.engine.URL.create(
    "mssql+pyodbc",
    username=username,
    password=password,
    host=host,
    database=database,
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "TrustServerCertificate": "yes",
        "LongAsMax": "Yes",
    },
)

    #creating the engine to connect and query
    engine = create_engine(connection_url)

    metadata = sqlalchemy.MetaData(engine)
    print(metadata)

    #taking the query results and converting it to a dataframe
    df_sql = pd.read_sql(sql_query, con = engine)

    return df_sql