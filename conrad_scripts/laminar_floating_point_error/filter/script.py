import json
import requests
import re
import logging
import os
from decimal import Decimal

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


def has_significant_difference(str1, str2):
    """
    Check if str1 and str2 are exactly one unit of precision apart.
    For positive numbers: str1 should be greater than str2
    For negative numbers: str1 should have greater absolute value (more negative) than str2

    Args:
        str1, str2: String representations of numbers

    Returns:
        bool: True if the numbers are exactly one unit of precision apart in the expected direction
    """
    # Parse as Decimal to avoid floating point errors
    num1 = Decimal(str1.strip())
    num2 = Decimal(str2.strip())

    def get_decimal_places(s):
        """Get number of decimal places in a string representation of a number."""
        s = s.strip()
        if "." in s:
            decimal_part = s.split(".")[1]
            return len(decimal_part)
        else:
            return 0

    # Determine decimal places for each string
    decimal_places1 = get_decimal_places(str1)
    decimal_places2 = get_decimal_places(str2)

    # Use the maximum decimal places to determine the finest precision
    max_decimal_places = max(decimal_places1, decimal_places2)

    # Threshold is 1 unit in the least significant decimal place
    threshold = Decimal(10) ** (-max_decimal_places)

    # Check if both numbers are negative
    if num1 < 0 and num2 < 0:
        # For negative numbers, we want num1 to have greater absolute value (more negative)
        # This means num2 - num1 should equal threshold (since num1 < num2 in value)
        difference = num2 - num1
        return difference == threshold
    else:
        # For positive numbers (or mixed), we want num1 > num2
        difference = num1 - num2
        return difference == threshold


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


def get_dataset(dataset_id):
    url = f"https://www.bco-dmo.org/osprey_search/document/dataset/{dataset_id}"
    res = requests.get(
        url,
    )
    if res.status_code == 400:
        return None
    js = res.json()
    return js


js = load_file()
results = {}
duplicates = {}
duplicate_count = 0
total = 0
for pipeline in js["buggy_pipelines"]:
    if "status" in pipeline and pipeline["status"] != "buggy":
        continue
    # Can skip, typo
    if (
        pipeline["pipeline_spec"] == "879380//data/pipeline-spec.yaml"
        or pipeline["pipeline_spec"] == "869011//data/pipeline-spec.yaml"
        or pipeline["pipeline_title"] == "879380 _v1_mcmanus_ctd"
    ):
        continue
    pipeline_title = pipeline["pipeline_title"]
    if (
        pipeline_title.startswith("959033")
        or pipeline_title.startswith("854194")
        or pipeline_title.startswith("931843")
        # Amber
        or pipeline_title.startswith("935794")
        or pipeline_title.startswith("962986")
        or pipeline_title.startswith("925613")
        or pipeline_title.startswith("731314")
        # Audrey
        or pipeline_title.startswith("959971")
        # Not yet confirmed
        # Karen
        or pipeline_title.startswith("967862")
        or pipeline_title.startswith("964215")
        # Dana
        or pipeline_title.startswith("967934")
        or pipeline_title.startswith("967925")
        or pipeline_title.startswith("713965")
        or pipeline_title.startswith("915912")
        or pipeline_title.startswith("945927")
        # Sawyer
        or pipeline_title.startswith("905404")
        or pipeline_title.startswith("864826")
    ):
        continue
    if pipeline_title == "938004_v1_bsb-survival-lipids-mesocosm-experiment":
        # To be confirmed by Amber. Pipeline that was run and dumped but never used for a dataset.
        continue
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
            "errors": [],
            "not_complete": [],
        }
    error_found = False
    for diff in pipeline["comparison"]["detailed_differences"]:
        for cell_diff in diff["cell_differences"]:
            if cell_diff["original_value"] == "ROW_NOT_IN_ORIGINAL":
                results[orcid]["errors"].append(
                    {
                        "pipeline_title": pipeline_title,
                        "last_updated": pipeline["last_updated"],
                        "error": "ROW_NOT_IN_ORIGINAL",
                        "name": pipeline["author_name"],
                        "submission_ids": submission_ids,
                    }
                )
                error_found = True
                break
            if pipeline_title == "960511_v1_carbonate_rock_species":
                results[orcid]["errors"].append(
                    {
                        "pipeline_title": pipeline_title,
                        "last_updated": pipeline["last_updated"],
                        "error": "There was an issue with the script and this pipeline should be run manually. At row 23 the column Longitude should change from -84.8415 to -84.8416. Be careful about 'preserve_missing_values', this one seems to have been originally run without it",
                        "name": pipeline["author_name"],
                        "submission_ids": submission_ids,
                    }
                )
                error_found = True
                break
            if pipeline_title == "869081_v1_2019_BAIT_dissolved_iron_speciation":
                results[orcid]["errors"].append(
                    {
                        "pipeline_title": pipeline_title,
                        "last_updated": pipeline["last_updated"],
                        "error": "There was an issue with the script and this pipeline should be run manually. For example, at row 3, column log_K2_95_Percent_CI should be 0.21 instead of 0.20. Be careful about 'preserve_missing_values', this one seems to have been originally run without it",
                        "name": pipeline["author_name"],
                        "submission_ids": submission_ids,
                    }
                )
                error_found = True
                break
            if pipeline_title == "915490_v1_Schlosser_helium_neon":
                results[orcid]["errors"].append(
                    {
                        "pipeline_title": pipeline_title,
                        "last_updated": pipeline["last_updated"],
                        "error": "There was an issue with the script and this pipeline should be run manually. The columns He3Conc and He3ConcEr seem to have very different values after running. For example: new_value 0e-2, orig value: 1.79e-16.",
                        "name": pipeline["author_name"],
                        "submission_ids": submission_ids,
                    }
                )
                error_found = True
                break
            if pipeline_title == "870316_v1_pollack_waterquality":
                results[orcid]["errors"].append(
                    {
                        "pipeline_title": pipeline_title,
                        "last_updated": pipeline["last_updated"],
                        "error": "There was an issue with the script and this pipeline should be run manually. The columns He3Conc and He3ConcEr seem to have very different values after running. For example: new_value 0e-2, orig value: 1.79e-16.",
                        "name": pipeline["author_name"],
                        "submission_ids": submission_ids,
                    }
                )
                error_found = True
                break
            if pipeline_title == "832995_v1_Casciotti_Kelly_N2O_isotopomers":
                results[orcid]["errors"].append(
                    {
                        "pipeline_title": pipeline_title,
                        "last_updated": pipeline["last_updated"],
                        "error": "There was an issue with the script and this pipeline should be run manually. In the original SR1805_CTD_DateTimes.xlsx file, the cast on row 3 is actually '3, not 3. An older version of laminar seems to have ignored the ' and loaded it in as 3, which worked for you. It no longer works that way, so the join does not work correctly. You will probably need to update the original file outside laminar, reupload, and run it again. Unfortunately this needs to be done because there are other values that are impacted by the bug. For example Seabird Oxygen on row 7 should be 3.910 and it is currently 3.900. ",
                        "name": pipeline["author_name"],
                        "submission_ids": submission_ids,
                    }
                )
                error_found = True
                break
            if pipeline_title == "854424_v1_km1910-sample-metadata-for-mocness-tows":
                results[orcid]["errors"].append(
                    {
                        "pipeline_title": pipeline_title,
                        "last_updated": pipeline["last_updated"],
                        "error": "There was an issue with the script and this pipeline should be run manually. This is a very weird one. There are some issues related to the bug (for example, row 1, column Bulk_fraction is 0 and should be 1), so it needs to run again. However, you also need to fix the pipeline. TLDR: add the year to the convert_date processor. Anything after 1947 will do. Longer explanation: the time value is currently being evaluated without a year, and python sets the year to 1900. From 1896 to 1947, Hawaii used UTC-10:30 instead of UTC-10:00, which means the timezone offset you set will be 30 minutes off. Some older version of laminar handled this differently and the problem didn't show up.",
                        "name": pipeline["author_name"],
                        "submission_ids": submission_ids,
                    }
                )
                error_found = True
                break
            if pipeline_title == "882177_v1_SAB_CTDs":
                # this one has diffs like this:
                # 5.720 AND 5.710, which we have confirmed are related to the bug
                continue
            if (
                pipeline_title == "934904_v1_taenzer_pumpctd"
                or pipeline_title == "849330_v1_grottoli_lit_review"
            ):
                # this one has diffs like this:
                # -0.00 AND -0.0000000000000000000000000000999
                continue
            if (
                pipeline_title == "737534_v3_Lake_Michigan_CTD"
                or pipeline_title == "858830_v1_BSi"
            ):
                # scientific notation
                continue
            if pipeline_title == "870316_v1_pollack_waterquality":
                # Additional character added, probably related to CSV library. Otherwise good
                """
                {
                 "row": 802,
                 "column": "Comments",
                 "new_value": "Oysters added; Tanks sealed\u00a0",
                 "original_value": "Oysters added; Tanks sealed"
                }
                """
                continue
            if pipeline_title == "865159_v1_apprill_MetaboliteUptake":
                # Weirder results because of larger floats, but still bug related
                """
                {
                     "row": 3,
                     "column": "Unpigmented_cells",
                     "new_value": "347270.8070953440",
                     "original_value": "347270.8070953437"
                }
                """
                continue
            if pipeline_title == "858771_v1_14C_uptake":
                # Weirder results because of larger floats and scientific notation, but still bug related
                # 7.660e-12 AND 7.656e-12
                continue
            if pipeline_title == "849870_v1_anacondas_pco2":
                # Datetime strings included
                continue

            # Check if the differences are actually related to the bug
            new_value = cell_diff["new_value"]
            orig_value = cell_diff["original_value"]
            if not has_significant_difference(new_value, orig_value):
                logger.warning(
                    f"Found weird diff for pipeline {pipeline_title}: {new_value} AND {orig_value}"
                )

        if error_found:
            break

    if error_found:
        continue

    are_there_errors = False
    for file in pipeline["comparison"]["file_comparisons"]:
        if file["etags_match"]:
            continue
        if file["cell_differences_count"] != 0:
            are_there_errors = True
        if not file["headers_match"]:
            are_there_errors = True
        # if file["original_size"] != file["new_size"]:
        #    are_there_errors = True
    if not are_there_errors:
        logger.error(f"ERROR: No Errors found: {pipeline_title}")
        exit()

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

    # We would only expect the filename to match that from the dataset if it's the new version of submission/laminar
    if submission_id_originally_attached_to_pipeline:
        # Check if the filename matches on the dataset
        match = re.search(r"^(\d+)_", pipeline_title)
        if match:
            dataset_id = match.group(1)
        else:
            logger.error(f"ERROR: pipeline missing dataset ID: {pipeline_title}")
            exit()
        dataset = get_dataset(dataset_id)
        if dataset is None:
            logger.warning(
                f"No dataset found for dataset {dataset_id}, pipeline {pipeline_title}"
            )
        else:
            buggy_match_found = False
            primary_data_filenames = []
            for df in dataset.get("dataFiles", []) + dataset.get(
                "supplementalFiles", []
            ):
                filename = df["filename"]
                if df.get("primary_data_file", False):
                    primary_data_filenames.append(filename)
                for file in pipeline["comparison"]["file_comparisons"]:
                    if filename == file["filename"] and not file["etags_match"]:
                        buggy_match_found = True
                        if file["original_size"] != df.get("bytesize", 0):
                            logger.warning(
                                f"Filenames match but size does not for dataset {dataset_id}, pipeline {pipeline_title}"
                            )
                        break
                if buggy_match_found:
                    break

            if not buggy_match_found:
                logger.info(
                    f"No matching data file found for dataset {dataset_id}, pipeline {pipeline_title}. Skipping!"
                )
                """
                filenames = [
                    f["filename"] for f in pipeline["comparison"]["file_comparisons"]
                ]
                results[orcid]["skipping"].append(
                    {
                        "pipeline_title": pipeline_title,
                        "last_updated": pipeline["last_updated"],
                        "reason": f"Filename not found in published dataset. Primary data files: {json.dumps(primary_data_filenames)}. Filenames from laminar in S3: {json.dumps(filenames)}",
                        "name": pipeline["author_name"],
                        "submission_ids": submission_ids,
                    }
                )
                """
                continue

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

                if resource["bcodmo:"]["objectKey"] in [
                    f["original_path"]
                    for f in pipeline["comparison"]["file_comparisons"]
                ]:
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
for pipeline in js["errors"]:
    orcid = pipeline["author_orcid"]
    pipeline_title = pipeline["pipeline_title"]
    if "pipeline_error_message" not in pipeline:
        error = pipeline["errors"][0]
    else:
        error = pipeline["pipeline_error_message"]
    if len(error) > 100:
        error = error[:100] + "..."
    results[orcid]["errors"].append(
        {
            "pipeline_title": pipeline_title,
            "last_updated": pipeline["last_updated"],
            "error": error,
            "name": pipeline["author_name"],
            "submission_ids": pipeline["submission_ids"],
        }
    )


count = 0
count2 = 0
count3 = 0
count4 = 0
count5 = 0
for orcid in results.keys():
    count += len(results[orcid]["in_dataset_pipelines"])
    count2 += len(results[orcid]["not_in_dataset_pipelines"])
    count3 += len(results[orcid]["inferred_submission_id_not_in_dataset_pipelines"])
    count4 += len(results[orcid]["no_submission_id"])
    count5 += len(results[orcid]["errors"])

logger.info(f"Found {count} pipelines that were in a dataset.")
logger.info(
    f"Found {count2} pipelines with a submission ID that were NOT in a dataset."
)
logger.info(
    f"Found {count3} (likely old) pipelines with an inferred submission ID that were NOT in a dataset."
)
logger.info(f"Found {count4} (likely old) pipelines with no submission ID.")
logger.info(f"Found {duplicate_count} pipelines that were duplicated.")
logger.info(f"Found {count5} pipelines that had errors.")


with open("filtered.json", "w") as f:
    json.dump(results, f, indent=2, default=str)

logger.info(f"Results saved to filtered.json")
