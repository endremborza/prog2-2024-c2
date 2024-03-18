import flask
import pandas as pd
from sklearn.neighbors import KDTree
import pickle
from scipy import spatial

app = flask.Flask(__name__)

df = pd.read_csv("input.csv")
df = df.rename({df.columns[2]:"y"})

df_thunder = df[df["dmg_type"] == "thunder"].reset_index(drop=True)
df_acid = df[df["dmg_type"] == "acid"].reset_index(drop=True)
df_cold = df[df["dmg_type"] == "cold"].reset_index(drop=True)
df_fire = df[df["dmg_type"] == "fire"].reset_index(drop=True)
df_lightning = df[df["dmg_type"] == "lightning"].reset_index(drop=True)
df_poison = df[df["dmg_type"] == "poison"].reset_index(drop=True)

tree_thunder = spatial.KDTree(df_thunder[["x","y"]])

tree_poison = spatial.KDTree(df_poison[["x","y"]])

tree_cold = spatial.KDTree(df_cold[["x","y"]])

tree_acid = spatial.KDTree(df_acid[["x","y"]])

tree_lightning = spatial.KDTree(df_lightning[["x","y"]])

tree_fire = spatial.KDTree(df_fire[["x","y"]])

@app.route("/ping")
def ping():
    query_df = pd.read_csv("query.csv")
    results = []
    for idx, query in query_df.iterrows():
        query_point = query[['x', 'y']].values.reshape(1, -1)
        damage_type = query['dmg_type']
        
        # Choose the appropriate tree based on the damage type
        if damage_type == "thunder":
            tree = tree_thunder
        elif damage_type == "acid":
            tree = tree_acid
        elif damage_type == "cold":
            tree = tree_cold
        elif damage_type == "fire":
            tree = tree_fire
        elif damage_type == "lightning":
            tree = tree_lightning
        elif damage_type == "poison":
            tree = tree_poison
        else:
            raise ValueError(f"Unknown damage type: {damage_type}")
        
        # Query the tree for the nearest neighbor
        dist, ind = tree.query(query_point)
        
        # Get the corresponding damage value of the nearest neighbor
        nearest_neighbor_damage = df.iloc[ind[0]]['damage']
        
        results.append({
            'nearest_neighbor_damage': nearest_neighbor_damage,
        })
    
    # Convert results to DataFrame
    result_df = pd.DataFrame(results)
    
    # Write DataFrame to CSV
    result_df.to_csv('out.csv', index=False)

    return "OK"

@app.route("/")
def ok():
    return "OK"


if __name__ == "__main__":
    # Run the Flask app
    app.run(port=5678)




'''
# Load data and build KDTree
df = pd.read_csv("input.csv")
weps = df["weapon"].values
tree = KDTree(df[["x", "y"]])


@app.route("/ping")
def ping():
    # Load query data and perform KDTree query
    qdf = pd.read_csv("query.csv")
    dists, inds = tree.query(qdf)
    
    # Create DataFrame with results and save to CSV
    result_df = pd.DataFrame({"dist": dists.reshape(-1), "weapon": weps[inds.reshape(-1)]})
    result_df.to_csv("out.csv", index=False)
    
    return "OK"

'''