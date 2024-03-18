import flask
import pandas as pd
from scipy import spatial
import numpy as np
import polars as pl

app = flask.Flask(__name__)
###Preproc
df = pd.read_csv("input.csv")
tree_dict = {}
tree_dict1 = {}
for dt in df["dmg_type"].unique():
    inner_dict = {}
    for dmg in df["dmg"].unique():
                            
        tree = spatial.cKDTree(df.loc[(df["dmg_type"] == dt)&
                                      (df["dmg"] == dmg),["x","y"]])
        inner_dict[dmg] = tree

    tree = spatial.cKDTree(df.loc[(df["dmg_type"] == dt),["x","y"]])
    tree_dict[dt] = inner_dict
    tree_dict1[dt] = tree

dtypes = df["dmg_type"].unique()
dmgs = pl.DataFrame(df[["dmg_type","dmg"]])
unique_dmgs = pl.DataFrame(df[["dmg_type","dmg"]].drop_duplicates())

### Compute
@app.route("/ping")
def ping():
    query_df = pd.read_csv("query.csv")
    query_df = pl.DataFrame(query_df)
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
    return "OK"


@app.route("/")
def ok():
    return "OK"


if __name__ == "__main__":
    app.run(port=8000)
