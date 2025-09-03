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
duplicates = {}
duplicate_count = 0
total = 0
for pipeline in js["all_results"]:
    if "is_buggy" in pipeline and not pipeline["is_buggy"]:
        continue
    # Can skip, typo
    if (
        pipeline["pipeline_spec"] == "879380//data/pipeline-spec.yaml"
        or pipeline["pipeline_spec"] == "869011//data/pipeline-spec.yaml"
    ):
        continue
    pipeline_title = pipeline["pipeline_title"]
    if pipeline_title in duplicates:
        duplicate_count += 1
        if (
            pipeline["author_orcid"] == duplicates[pipeline_title]["author_orcid"]
            and pipeline["pipeline_spec"] == duplicates[pipeline_title]["pipeline_spec"]
            and pipeline["last_updated"] == duplicates[pipeline_title]["last_updated"]
        ):
            # This is a duplicate because it's the same pipeline we've already seen
            # OK to continue
            continue

        logger.error(f"DUPLICATE: {pipeline_title}")
        print(pipeline)
        print("________________________________________________")
        print(duplicates[pipeline_title])
        exit()
    duplicates[pipeline_title] = pipeline
    submission_ids = pipeline["submission_ids"]
    prefix = pipeline["original_prefix"]
    logger.info(
        f"Looking at prefix {prefix}, pipeline title {pipeline_title}, submissionIds {submission_ids}"
    )

    orcid = pipeline["author_orcid"]
    if orcid not in results:
        results[orcid] = {
            "name": pipeline["author_name"],
            "in_dataset_pipelines": [],
            "not_in_dataset_pipelines": [],
            "inferred_submission_id_not_in_dataset_pipelines": [],
            "no_submission_id": [],
        }
    pipeline_info = {
        "pipeline_title": pipeline_title,
        "last_updated": pipeline["last_updated"],
    }
    submission_id_originally_attached_to_pipeline = True
    if len(submission_ids) == 0:
        submission_id_originally_attached_to_pipeline = False
        submission_id = ""
        if pipeline["excel_file"].startswith("s3://bcodmo-submissions/"):
            match = re.search(
                r"^s3:\/\/bcodmo-submissions\/([^\/]*)", pipeline["excel_file"]
            )
            if match:
                submission_id = match.group(1)
        if not submission_id:
            results[orcid]["no_submission_id"].append(pipeline_info)
        else:
            submission_ids = [submission_id]

    for submission_id in submission_ids:
        pipeline_info["submission_id"] = submission_id
        pipeline_info["submission_id_originally_attached_to_pipeline"] = (
            submission_id_originally_attached_to_pipeline
        )
        submission = get_submission(submission_id)
        state = submission.get("bcodmo:", {}).get("state")
        pipeline_info["submission_state"] = state
        contained = False
        checked_in = []
        for resource in submission["resources"]:
            if resource.get("bcodmo:", {}).get("category") == "laminar":
                if resource["bcodmo:"]["laminarTitle"] == pipeline_title:
                    if (
                        resource["bcodmo:"].get("checkIn", {})
                        and resource["bcodmo:"].get("checkIn", {})["Status"]
                        == "SUCCESS"
                    ):
                        contained = True
                        checked_in.append(resource["bcodmo:"]["resourceId"])
        in_dataset = False
        for dataset in submission.get("bcodmo:", {}).get("datasets", []):
            for resourceId in dataset.get("primaryDataFiles", []) or []:
                if resourceId in checked_in:
                    in_dataset = True
            for resourceId in dataset.get("supplementalFiles", []) or []:
                if resourceId in checked_in:
                    in_dataset = True
                    break

        pipeline_info["checked_in"] = len(checked_in) > 0
        if in_dataset:
            results[orcid]["in_dataset_pipelines"].append(pipeline_info)
        else:
            if submission_id_originally_attached_to_pipeline:

                results[orcid]["not_in_dataset_pipelines"].append(pipeline_info)
            else:
                results[orcid][
                    "inferred_submission_id_not_in_dataset_pipelines"
                ].append(pipeline_info)

count = 0
count2 = 0
count3 = 0
count4 = 0
for orcid in results.keys():
    count += len(results[orcid]["in_dataset_pipelines"])
    count2 += len(results[orcid]["not_in_dataset_pipelines"])
    count3 += len(results[orcid]["inferred_submission_id_not_in_dataset_pipelines"])
    count4 += len(results[orcid]["no_submission_id"])

logger.info(f"Found {count} pipelines that were in a dataset.")
logger.info(
    f"Found {count2} pipelines with a submission ID that were NOT in a dataset."
)
logger.info(
    f"Found {count3} (likely old) pipelines with an inferred submission ID that were NOT in a dataset."
)
logger.info(f"Found {count4} (likely old) pipelines with no submission ID.")
logger.info(f"Found {duplicate_count} pipelines that were duplicated.")


with open("filtered.json", "w") as f:
    json.dump(results, f, indent=2, default=str)

logger.info(f"Results saved to filtered.json")
