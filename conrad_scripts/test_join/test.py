from dataflows import Flow, join, dump_to_path
from decimal import Decimal

import time

data1 = [
    {"col1": 1.531, "col2": "hello"},
    {"col1": 1.132, "col2": "goodbye"},
]
data2 = [
    {"colA": 1.531, "colB": "123"},
    {"colA": 1.132, "colB": 1.132},
]


print("############################")
f = Flow(
    data1,
    data2,
    join(
        "res_1",
        ["col1"],
        "res_2",
        ["colA"],
        {"col2": {"name": "col2", "aggregate": "first"}},
        mode="full-outer"
    ),
    dump_to_path(),
)

print(f.results())

print("############################")
