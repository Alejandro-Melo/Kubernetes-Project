import pandas as pd
import polars as pl

# Load parquet file into DataFrame
df = pl.from_pandas(pd.read_parquet("files/Apache.parquet"))

query="""
SELECT
    EXTRACT('month', date) AS m,
    type,
    count(type) as type_count,
    FROM self
    GROUP BY m, type
    ORDER BY m DESC
"""

result_df = df.sql(query)
print(df)

