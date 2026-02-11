import json
import requests
import re
import logging
import os
from decimal import Decimal
import csv

# TODO pull out version and make it 1 greater


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create formatters and add it to handlers
log_format = logging.Formatter(
    "%(asctime)s - [%(threadName)s] - %(levelname)s - %(message)s"
)

# Console handler (stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(log_format)

# File handler
file_handler = logging.FileHandler("pipeline_submission.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(log_format)

# Add handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)


def load_file(filename):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {filename}: {e}")
        raise


def get_dataset(dataset_id):
    url = f"https://www.bco-dmo.org/osprey_search/document/dataset/{dataset_id}"
    res = requests.get(
        url,
    )
    if res.status_code == 400:
        return None
    js = res.json()
    return js


def generate_change_summary(matches, dataset_id, pipeline):
    changes_summary = ""
    changes_detailed = []
    for match in matches:

        headers_impacted = set()
        rows_impacted = set()
        total_cells_impacted = 0
        example = ""
        for detailed_diff in pipeline["comparison"]["detailed_differences"]:
            if match["filename"] == detailed_diff["filename"]:
                changes_detailed = changes_detailed + [
                    {**d, "filename": match["filename"]}
                    for d in detailed_diff["cell_differences"]
                ]
                for cell_diff in detailed_diff["cell_differences"]:
                    total_cells_impacted += 1
                    headers_impacted.add(cell_diff["column"])
                    rows_impacted.add(cell_diff["row"])
                    if not example:
                        example = (
                            f"{cell_diff['original_value']} -> {cell_diff['new_value']}"
                        )
        headers_impacted = list(headers_impacted)
        headers_impacted_string = ", ".join(headers_impacted)
        if len(headers_impacted_string) > 100:
            headers_impacted_string = headers_impacted_string[:100] + "... (cut off)"
        rows_impacted = list(rows_impacted)
        rows_impacted_string = ", ".join([str(v) for v in rows_impacted])
        if len(rows_impacted_string) > 100:
            rows_impacted_string = rows_impacted_string[:100] + "... (cut off)"
        if changes_summary:
            changes_summary += "\n______________________________\n"
        changes_summary += f"""The file {match['filename']} from dataset {dataset_id} had {total_cells_impacted} value{'s that were' if total_cells_impacted > 1 else ' that was'} impacted.

The following header{'s were' if len(headers_impacted) > 1 else ' was'} impacted: {headers_impacted_string}

The following row{'s were' if len(rows_impacted) > 1 else ' was'} impacted: {rows_impacted_string}

Here is {'an example of one of the changes' if total_cells_impacted > 1 else 'the change'}: {example}"""
    return changes_summary, changes_detailed


js = load_file("../actual_buggy_pipelines.json")
buggy_dict = {}
for pipeline in js["buggy_pipelines"]:
    pipeline_title = pipeline["pipeline_name"]
    buggy_dict[pipeline_title] = pipeline


js = load_file("../filter/filtered.json")
final = {}
dataset_ids = {}
for orcid in js.keys():
    author_name = js[orcid]["name"]
    for p in js[orcid]["in_dataset_pipelines"]:
        pipeline_title = p["pipeline_title"]
        logger.info(f"Processing {pipeline_title}")
        pipeline = buggy_dict[pipeline_title]

        match = re.search(r"^(\d+)_", pipeline_title)
        assert match
        dataset_id = match.group(1)
        assert dataset_id not in dataset_ids
        dataset_ids[dataset_id] = True
        dataset = get_dataset(dataset_id)
        assert dataset is not None
        original_version = dataset["versionLabel"]
        new_version = str(int(original_version) + 1)

        matches = []
        for df in dataset.get("dataFiles", []) + dataset.get("supplementalFiles", []):
            filename = df["filename"]
            for file in pipeline["comparison"]["file_comparisons"]:
                if filename == file["filename"] and not file["etags_match"]:
                    assert file["original_size"] == df.get("bytesize", 0)
                    version_match = re.search(r"_v(\d+)_", filename)
                    if not version_match:
                        new_filename = ""
                    else:
                        version = version_match.group(1)
                        assert version == original_version
                        new_filename = filename.replace(
                            f"_v{original_version}_", f"_v{new_version}_"
                        )
                    matches.append(
                        {
                            "filename": filename,
                            "new_filename": new_filename,
                        }
                    )
        assert len(matches) != 0
        changes_summary, changes_detailed = generate_change_summary(
            matches, dataset_id, pipeline
        )

        final[dataset_id] = {
            "dataset_id": dataset_id,
            "link": f"https://www.bco-dmo.org/dataset/{dataset_id}",
            "original_version": original_version,
            "new_version": new_version,
            "files": [
                {
                    "original_filename": match["filename"],
                    "new_filename": match["new_filename"],
                    "fixed_file_path": f"{pipeline["test_prefix"]}/{match['filename']}",
                    "fixed_file_bucket": "laminar-dump",
                }
                for match in matches
            ],
            "changes_summary": changes_summary,
            "changes_detailed": changes_detailed,
            "original_author": author_name,
            "original_orcid": orcid,
        }

with open("final.json", "w") as f:
    json.dump(final, f, indent=4)
