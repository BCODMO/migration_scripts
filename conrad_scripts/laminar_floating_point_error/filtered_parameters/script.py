import itertools
import json
import requests
import re
import logging
import os


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
file_handler = logging.FileHandler("parameter_counts.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(log_format)

# Add handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)


def load_file(filename="../actual_buggy_pipelines.json"):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {filename}: {e}")
        raise


def get_key(pipeline):
    return f"{pipeline['pipeline_title']}-{pipeline['last_updated']}"

js = load_file()
results = []
pipeline_dict = {}
for pipeline in js["all_results"]:
    pipeline_dict[get_key(pipeline)] = pipeline

_parameters = load_file(filename="../parameters/parameters.json")
filtered = load_file(filename="../filter/filtered.json")
for orcid in filtered.keys():
    combined = itertools.chain(
        filtered[orcid]["in_dataset_pipelines"],
        filtered[orcid]["not_in_dataset_pipelines"],
        filtered[orcid]["inferred_submission_id_not_in_dataset_pipelines"],
        filtered[orcid]["no_submission_id"],
    )
    for p in combined:
        pipeline = pipeline_dict[get_key(p)]
        if "original_prefix" not in pipeline:
            logger.info("Skipping because missing parameters (likely not a real dataset)")
            continue
        parameters_dict = _parameters[pipeline["original_prefix"]]
        if "comparison" not in pipeline:
            logger.info("Skipping because no comparison (error when running)")
            continue
        for diff in pipeline["comparison"]["detailed_differences"]:
            r = {}
            for cell in diff["cell_differences"]:
                column = cell["column"]
                param = f"NOT FOUND ({column})"
                #print(parameters_dict.keys())
                if column in parameters_dict:
                    param = parameters_dict[column]
                if param not in r:
                    r[param] = 0
                r[param] += 1
            logger.info(f"Found info {r}")




with open("parameter_counts.json", "w") as f:
    json.dump(results, f, indent=2, default=str)

logger.info(f"Results saved to parameter_counts.json")
"""
