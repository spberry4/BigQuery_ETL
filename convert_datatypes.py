import pandas as pd
import io
import json
import numpy as np
from google.cloud import bigquery

#convert bigquery data types to pandas datatypes

def bigquery_to_pandas(table, client, df_to_convert):
    f = io.StringIO("")
    client.schema_to_json(table.schema, f)

    json_object = json.loads(f.getvalue())

    data_converter = pd.DataFrame(json_object, columns = json_object[0])

    data_converter["python_types"] = np.where(data_converter["type"] == "NUMERIC", "int", 
                                        np.where(data_converter["type"] == "DATE", "datetime64[ns]", 
                                            np.where(data_converter["type"] == "INTEGER", "int",
                                                np.where(data_converter["type"] == "FLOAT", "float",
                                                    np.where(data_converter["type"] == "BOLEAN", "bool",
                                                        np.where(data_converter["type"] == "DATETIME", "datetime64[ns]",
                                                            np.where(data_converter["type"] == "TIMESTAMP", "datetime64[ns]",
                                                                "str")))))))

    #Covert the datatypes dataframe into a dictionary that can then be fed into the astype function to update datatypes en masse
    datatype_data = dict(([x,y]) for x, y in zip(data_converter["name"], 
                                                 data_converter["python_types"]))
    
    df_to_convert = df_to_convert.astype(datatype_data)
    return df_to_convert


#function that converts oracle datatypes to bigquery
def python_to_bigquery(df):
    oracle_converter = pd.DataFrame(df.dtypes, columns=["type"])
    oracle_converter["big_query_types"] = np.where(oracle_converter["type"] == "int64", "NUMERIC", 
                                            np.where(oracle_converter["type"] == "datetime64[ns]", "date", 
                                                np.where(oracle_converter["type"] == "int32", "INTEGER",
                                                    np.where(oracle_converter["type"] == "float64", "FLOAT",
                                                        np.where(oracle_converter["type"] == "bool", "BOLEAN",
                                                            np.where(oracle_converter["type"] == "datetime64[ns]", "DATETIME",
                                                                np.where(oracle_converter["type"] == "datetime64[ns]", "TIMESTAMP",
                                                                    "STRING")))))))
    
    oracle_datatype_mapping_table = dict(([x,y]) for x, y in zip(df.columns, 
                                                 oracle_converter["big_query_types"]))
    
    #need to convert this to the bigquery schema in order to get this to work, a dictionary can be used but this is need to feed into bigquery
    schema = []

    for key, value in oracle_datatype_mapping_table.items():

        schemaField = bigquery.SchemaField(key, value) # NULLABLE BY DEFAULT:

        schema.append(schemaField)

    return oracle_datatype_mapping_table, schema
 