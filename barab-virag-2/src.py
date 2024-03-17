import pandas as pd
import polars as pl
from scipy import spatial
import pickle
import numpy as np

query_df = pd.read_csv("query.csv")
query_df = pl.DataFrame(query_df)
meta_df = pl.read_parquet("compute_workfile.parquet")
with open("dtypes.npy","rb") as file:
    dtypes = np.load(file,allow_pickle=True)

with open("smalltrees.pkl","rb") as file:
    tree_dict = pickle.load(file)

with open("bigtrees.pkl","rb") as file:
    tree_dict1 = pickle.load(file)
unique_dmgs = pl.read_parquet("dmgs_unique.parquet")
dmgs = pl.read_parquet("dmgs.parquet")

out_df = pl.DataFrame()
for dt in dtypes:
    dmg_list = []
    temp_dmgs = dmgs.filter(
        dmgs["dmg_type"] == dt
    ).select(
        pl.col("dmg")
    )
    for row in query_df.rows():
        valid_dmgs = unique_dmgs.filter(
            (unique_dmgs["dmg"] >= row[2])&
            (unique_dmgs["dmg"] <= row[3])&
            (unique_dmgs["dmg_type"] == dt)
        )["dmg"]

        if len(valid_dmgs)==0:
                dmg_list.append(0)

        elif len(valid_dmgs)>0 and len(valid_dmgs)<30:
            dmg_list.append(valid_dmgs[pd.Series([tree_dict[dt][dmg].query(row[0:2])[0] for
                dmg in valid_dmgs]).idxmin()])  
        else:
            counter = 1
            while True:
                    if counter == 1:
                        index = tree_dict1[dt].query(row[0:2])[1]
                    else:
                        index = tree_dict1[dt].query(row[0:2],k=counter)[1][-1]
                    if (temp_dmgs.item(index,0) >= row[2]) and (temp_dmgs.item(index,0) <= row[3]):
                        dmg_list.append(temp_dmgs.item(index,0))
                        break
                    else:
                        counter += 1                  

    out_df = out_df.with_columns(pl.Series(dt,dmg_list))

out_df.write_csv("out.csv")