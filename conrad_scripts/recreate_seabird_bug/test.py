import pandas as pd

file_path = "/home/conrad/Projects/whoi/laminar/datasets/mcgillcuddy/test.cnv"

reader = pd.read_fwf(
    file_path,
    colspecs="infer",
    infer_nrows=100,
    chunksize=4,
    dtype=str,
    header=None
    #
)

print(reader)
for chunk in reader:
    for index, row in chunk.iterrows():
        if index == 0:
            print("ROW?", list(chunk))
        l = row.tolist()
        print("ROW", l)
