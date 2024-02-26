from pprint import pprint
from frictionless import validate

descriptor = {
    "resources": [
        {"path": "s3://frictionless-test/table1.csv"},
        {"path": "s3://frictionless-test/table2.csv"},
    ]
}
report = validate(descriptor)
pprint(report)
