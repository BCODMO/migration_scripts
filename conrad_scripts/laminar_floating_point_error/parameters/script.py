import json
import requests
import re
import logging


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
file_handler = logging.FileHandler("pipeline_parameters.log")
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


def get_dataset(dataset_id):
    url = f"https://website-api-prod.bco-dmo.org/v1/dataset/{dataset_id}"
    res = requests.get(url)
    if res.status_code == 400:
        return None
    js = res.json()
    return js


js = load_file()
results = {}
for pipeline in js["all_results"]:
    submission_ids = pipeline["submission_ids"]
    prefix = pipeline["original_prefix"]
    logger.info(f"Looking at prefix {prefix}")
    match = re.search(r"^([^\/]*)", prefix)
    dataset_id = ""
    if match:
        dataset_id = match.group(1)
    else:
        logger.error(f"Didn't get a dataset ID for pipeline with prefix {prefix}")
        continue

    dataset = get_dataset(dataset_id)
    if dataset == None:
        logger.error(f"Didn't get a dataset for pipeline with dataset ID {dataset_id}")
        continue
    parameters = dataset["parameters"]
    logger.info(f"Found {len(parameters)} parameters")
    parameters_dict = {}
    for parameter in parameters:
        original_name = parameter["name"]
        bcodmo_parameter = parameter["type"]["name"]
        if (
            original_name in parameters_dict
            and parameters_dict[original_name] != bcodmo_parameter
        ):
            logger.error(
                f"Found a parameter twice: {original_name}, both {parameters_dict[original_name]} and {bcodmo_parameter}"
            )
            exit()
        parameters_dict[original_name] = bcodmo_parameter

    if prefix in results:
        logger.warning(f"Found a prefix twice: {prefix}")
    results[prefix] = parameters_dict

with open("parameters.json", "w") as f:
    json.dump(results, f, indent=2, default=str)

logger.info(f"Results saved to parameters.json")
