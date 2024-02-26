from tabulator import Stream

with Stream(
    "dataset.tsv", headers=1, format="csv", skip_rows=["#"], delimiter="\t"
) as stream:
    print(stream.headers)
    print(stream.read())
