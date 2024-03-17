import polars as pl


df = pl.read_parquet("input.parquet")
query_df = pl.read_csv("query.csv")

damage_types = df.unique(subset=["dmg_type"])["dmg_type"]

out = []
for row in query_df.iter_rows():
    out_row = {}
    for dt in damage_types:
        sub_df = df.filter(
            (pl.col("dmg_type") == dt)
            & (pl.col("dmg") >= row[2])
            & (pl.col("dmg") <= row[3])
        ).select(["x", "y", "dmg"])
        if sub_df.is_empty():
            out_row[dt] = 0
        else:
            diffs = ((sub_df["x"] - row[0]) ** 2) + ((sub_df["y"] - row[1]) ** 2)
            out_row[dt] = sub_df[diffs.arg_min(), 2]

    out.append(out_row)

pl.DataFrame(out).write_csv("out.csv")
