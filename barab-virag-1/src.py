import pandas as pd
from scipy import spatial
import pickle
import numpy as np

query_df = pd.read_csv("query.csv")
with open("dtypes.npy","rb") as file:
    dtypes = np.load(file,allow_pickle=True)

with open("smalltrees.pkl","rb") as file:
    tree_dict = pickle.load(file)

with open("bigtrees.pkl","rb") as file:
    tree_dict1 = pickle.load(file)

dmgs = pd.read_parquet("dmgs.parquet")

out_df = pd.DataFrame()
for dt in dtypes:
    dmg_list = []
    temp_dmgs = dmgs.loc[(dmgs["dmg_type"]) == dt,"dmg"].to_numpy()
    for idx, row in query_df.iterrows():
        
        valid_dmgs = np.unique(temp_dmgs[(temp_dmgs>=row["dmg_min"]) & (temp_dmgs<=row["dmg_max"])])
        
        #print(type([valid_dmgs]))
        if len(valid_dmgs)==0:
            dmg_list.append(0)
        elif len(valid_dmgs)<20:

            dmg_list.append(valid_dmgs[pd.Series([tree_dict[dt][dmg].query(row[["x","y"]])[0] for
                dmg in valid_dmgs]).idxmin()])   
        else:
            counter = 1
            while True:
                    if counter == 1:
                        index = tree_dict1[dt].query(row[["x","y"]])[1]
                    else:
                        index = tree_dict1[dt].query(row[["x","y"]],k=counter)[1][-1]
                    if (temp_dmgs[index] >= row["dmg_min"]) & (temp_dmgs[index] <= row["dmg_max"]):
                        dmg_list.append(temp_dmgs[index])
                        break
                    else:
                        counter += 1
    out_df[dt] = dmg_list

out_df.to_csv("out.csv",index= False)