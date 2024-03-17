import os
filelist = [ f for f in os.listdir() if f.endswith(".parquet") or 
            f.endswith(".pkl") or f.endswith(".npy")]
for f in filelist:
    os.remove(f)