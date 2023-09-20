# ETL from Oracle and MS SQL to Bigquery

Scripts for a data dump from bigquery or to convert oracle data or MS SQL to bigquery

# Requirements

```python
import numpy as np
import pandas as pd
from google.cloud import bigquery
import os
import cx_Oracle
import sqlalchemy
from sqlalchemy import create_engine
from time import process_time
import pandas_gbq
```

# Usage

This is an ETL pipeline that is designed to take data from oracle or MS SQL and move it to Bigquery. This is done entirely in memory using pandas so the size of the data being moved is dependant on the memory of the machine.

# Notes

The pandas API has issues with boolean columns especially then there are nulls involved. While writing and testing this I had a manually convert the column to a boolean so that pandas would recognize it. This also converts all null values to false.

# Future Updates

- Convert this to use dask or modin in order to remove memory considerations
