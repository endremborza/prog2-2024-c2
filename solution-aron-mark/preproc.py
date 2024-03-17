import polars as pl

df = pl.read_csv("input.csv")
df.write_parquet("input.parquet")
