from dataflows import Flow
from dataflows.processors import *

flows = [
    load(
        "./test.csv",
        name="res1",
        format="csv",
        skip_rows=None,
        missingValues={"col2": "nd"},
    ),
    update_schema("res1", missingValues=["nd"]),
    set_type("col2", type="number"),
    sort_rows("{col2}", reverse=False),
]

rows, datapackage, _ = Flow(*flows).results()
print(rows)
