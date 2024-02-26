from tabulator import Stream

file_path = "/home/conrad/Projects/whoi/laminar/scripts/recreate_tsv_bug/test.tsv"
with Stream(file_path, headers=1, format="tsv", skip_rows=["#"]) as stream:
    print(stream.headers)
    print(stream.read())
