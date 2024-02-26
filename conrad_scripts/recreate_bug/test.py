from dataflows import Flow, duplicate
from decimal import Decimal
from datapackage_pipelines.lib import (
    load as standard_load,
    duplicate as standard_duplicate,
    find_replace as standard_find_replace,
)

import time

data = [
    {"col1": 1.531},
    {"col1": 1.132},
]


def change_row(rows):
    for row in rows:
        row["col1"] = "AFTER"
        yield row


def change_rows(resource_name):
    def func(package):
        yield package.pkg
        for rows in package:
            if rows.res.name == resource_name:
                yield change_row(rows)
            else:
                yield rows

    return func


def assert_number_row(rows):
    for row in rows:
        print(type(row["col1"]))
        print(row, rows.res.name)
        assert type(row["col1"]) == Decimal
        yield row


def assert_number():
    def func(package):
        yield package.pkg
        for rows in package:
            yield assert_number_row(rows)

    return func


# When res_1 is changed first, the values in res_2 overwrite the new values in res_1
print("############################")
f = Flow(
    data,
    # duplicate(source="res_1", target_name="res_2"),
    standard_duplicate.flow({"source": "res_1", "target-name": "res_2"}),
    assert_number(),
    # change_rows("res_1"),
)

f.results()

print("############################")
