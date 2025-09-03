import json
import requests
import re
import logging
import os

SUBMISSION_ACCESS_KEY = os.getenv("SUBMISSION_ACCESS_KEY")

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


def load_file(filename="../actual_buggy_pipelines.json"):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {filename}: {e}")
        raise


def get_submission(submission_id):
    url = f"https://submit-api.bco-dmo.org/api/datapackage?type=submission&submissionId={submission_id}"
    res = requests.get(
        url,
        headers={"X-BCO-DMO-Access-Key": SUBMISSION_ACCESS_KEY},
    )
    if res.status_code == 400:
        return None
    js = res.json()
    return js.get("datapackage", {})


js = load_file()
results = {}
for pipeline in js["all_results"]:
    submission_ids = pipeline["submission_ids"]
    prefix = pipeline["original_prefix"]
    logger.info(f"Looking at prefix {prefix}")

    orcid = pipeline["author_orcid"]
    if orcid not in results:
        results[orcid] = {
            "name": pipeline["author_name"],
            "incomplete_pipelines": [],
        }

    for submission_id in submission_ids:
        submission = get_submission(submission_id)
        state = submission.get("bcodmo:", {}).get("state")
        if state != "complete":
            logger.info(f"Found a non complete state: {state}")
            contained = False
            for resource in submission["resources"]:
                if resource.get("bcodmo:", {}).get("category") == "laminar":
                    if (
                        resource["bcodmo:"]["laminarTitle"]
                        == pipeline["pipeline_title"]
                    ):
                        contained = True
                        break

            results[orcid]["incomplete_pipelines"].append(
                {
                    "submission_id": submission_id,
                    "state": state,
                    "pipeline_title": pipeline["pipeline_title"],
                    "contained_in_submission": contained,
                }
            )
count = 0
for orcid in results.keys():
    count += len(results[orcid]["incomplete_pipelines"])

logger.info(f"Found {count} pipelines linked to submissions that were not complete.")

logger.info(f"Results saved to submission_states.json")

with open("submission_states.json", "w") as f:
    json.dump(results, f, indent=2, default=str)

logger.info(f"Results saved to submission_states.json")
