from dataflows import Flow
from bcodmo_processors.bcodmo_pipeline_processors import load


file_path = "/home/conrad/Projects/whoi/laminar/scripts/recreate_tsv_bug/test.tsv"
"""
flows = [load(file_path, name="res", format="csv", skip_rows=["#"], delimiter="\t")]
"""
flows = [
    load(
        {
            "from": file_path,
            "name": "res",
            "format": "csv",
            "skip_rows": ["#"],
            "delimiter": "\t",
        }
    )
]
print(Flow(*flows).results())
