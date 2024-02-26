from dataflows import Flow
from bcodmo_processors.bcodmo_pipeline_processors import dump_to_s3, load
from datapackage_pipelines.lib import dump_to_path

# data = [{"col1": 1.531} for i in range(100000)]


# When res_1 is changed first, the values in res_2 overwrite the new values in res_1
print("############################")
f = Flow(
    load(
        {
            "from": "/home/conrad/Projects/whoi/laminar/datasets/large/BCODMO_Formatted.csv",
            "limit_rows": 1000,
            "name": "res",
        }
    ),
    dump_to_s3({"bucket_name": "laminar-dump", "prefix": "hash_testing"}),
    # dump_to_path.flow(
    #    {"out-path": "/home/conrad/Projects/whoi/laminar/scripts/test_hash",}, {},
    # ),
)

f.results()

print("############################")
