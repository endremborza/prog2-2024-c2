import multiprocessing as mp

import numpy as np
import pandas as pd
from flask import Flask, request

app = Flask(__name__)

df = pd.read_csv("input.csv")


def proc(q: mp.Queue, gdf: pd.DataFrame, dmg_type: str):

    out = []
    sorted_df = gdf.sort_values("dmg")
    dmgs = sorted_df["dmg"].values
    xys = sorted_df[["x", "y"]].values
    print("putting to q")
    q.put(0)
    query_df: pd.DataFrame = q.get()
    print("got querydf")
    s_inds = np.searchsorted(dmgs, query_df["dmg_min"], side="left")
    e_inds = np.searchsorted(dmgs, query_df["dmg_max"], side="right")
    for row, s_ind, e_ind in zip(
        query_df[["x", "y"]].values.reshape(-1, 1, 2), s_inds, e_inds
    ):
        if s_ind == e_ind:
            out.append(0)
            continue
        sub_arr = xys[s_ind:e_ind, :]
        diffs = np.sum((sub_arr - row) ** 2, axis=1)
        out.append(dmgs[s_ind + diffs.argmin()])

    q.put((dmg_type, out))


proces = []
queues = []

for dmg_type, gdf in df.groupby("dmg_type"):
    q = mp.Queue()
    queues.append(q)
    proces.append(mp.Process(target=proc, args=(q, gdf, dmg_type), daemon=True))
    proces[-1].start()
    q.get()


@app.route("/ping")
def ping():
    query_df = pd.read_csv("query.csv")
    for q in queues:
        q.put(query_df)
    cols = {}

    for q in queues:
        c, vals = q.get()
        cols[c] = vals

    pd.DataFrame(cols).to_csv("out.csv", index=False)
    return "OK"


@app.route("/")
def ok():
    return "OK"


@app.route("/kill")
def kill():
    for p in proces:
        p.terminate()
        p.join()

    request.environ.get("werkzeug.server.shutdown")()
    return "OK"


if __name__ == "__main__":
    app.run(port=5678)
