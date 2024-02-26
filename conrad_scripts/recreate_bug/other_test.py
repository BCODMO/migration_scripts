from decimal import Decimal
from bcodmo_processors.bcodmo_pipeline_processors import boolean_filter_rows

from datapackage_pipelines.lib import (
    load as standard_load,
    duplicate as standard_duplicate,
    find_replace as standard_find_replace,
)

from dataflows import Flow

l = standard_load.flow(
    {
        "from": "/home/conrad/Projects/whoi/laminar/datasets/large/BCODMO_Formatted.csv",
        "name": "res",
        "limit_rows": 5,
        "format": "csv",
        "cast_strategy": "schema",
    }
)
d = standard_duplicate.flow({"source": "res", "target-name": "res_new"})
bf = boolean_filter_rows(
    {"boolean_statement": "{Temperature} < 29", "resources": ["res_new"]}
)


fr = standard_find_replace.flow(
    {
        "fields": [
            {"name": "Temperature", "patterns": [{"find": ".*", "replace": "20",},],}
        ],
        "resources": ["res"],
    }
)


def assert_number_row(rows):
    for row in rows:
        print(row, rows.res.name)
        assert type(row["Temperature"]) == Decimal
        yield row


def assert_number():
    def func(package):
        yield package.pkg
        for rows in package:
            yield assert_number_row(rows)

    return func


flow = Flow(
    *[
        l,
        d,
        assert_number()
        # bf,
        # fr,
        #
    ]
)
_, datapackage, __ = flow.results()
print(datapackage.resources[0].descriptor)
