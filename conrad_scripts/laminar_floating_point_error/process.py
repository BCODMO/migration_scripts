#!/usr/bin/env python3
"""
Script to determine if newly found bugs are relevant for given pipelines.
Processes pipeline specs, runs them with modified output locations, and compares results.

Features:
- Reuses existing test output to avoid re-running pipelines
- Concurrent processing with configurable worker threads
- Automatic retry on HTTP/network failures (up to 5 attempts)
- Graceful shutdown on Ctrl+C with pipeline cancellation
- Interruptible file comparisons for large datasets
- Progress tracking for large file comparisons and long-running pipelines
- Extraction and storage of pipeline author information and submission IDs
- Proper handling of pipelines returning SUCCESS with errors
- Maintains result order despite concurrent processing

Optimization:
- Checks for existing output in test location before running pipelines
- Reuses existing output when found, significantly reducing processing time
- Tracks which pipelines were reused vs newly executed

Interrupt Handling:
- Press Ctrl+C to gracefully stop processing
- Active pipelines are cancelled via DELETE requests
- Large file comparisons are interrupted at checkpoints
- Partial results are saved automatically

Output:
- Saves comprehensive JSON with pipeline metadata including:
  - Author name and ORCID from pipeline spec
  - Submission IDs from pipeline spec
  - Detailed comparison results
  - Error tracking and status information
  - Reuse statistics
"""

import os
import json
import time
import yaml
import boto3
import requests
import pandas as pd
import io
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import logging
from pathlib import Path
import copy
import urllib3
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
import traceback

# Suppress SSL warnings since we're not verifying certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BCODMO_API_KEY = os.getenv("BCODMO_API_KEY")
# Constants
ENDPOINT_URL = "https://127.0.0.1:5300"
ENDPOINT_URL = "https://staging-laminar-api.bco-dmo.org"
S3_BUCKET = "laminar-dump"
TEST_PREFIX = "EXCEL_BUG_TEST"
POLL_INTERVAL = 1  # seconds
MAX_WORKERS = 5  # Number of concurrent threads (can be adjusted)
MAX_RETRIES = 5  # Maximum number of retries for HTTP requests
RETRY_DELAY = 1  # seconds to wait between retries

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
file_handler = logging.FileHandler("pipeline_bug_detector.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(log_format)

# Add handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Initialize S3 client with thread lock for safety
s3_client = boto3.client("s3")
s3_lock = Lock()

# Global shutdown event for graceful termination
shutdown_event = Event()

# Global results storage with lock
results_lock = Lock()
completed_results = {}

# Global active pipelines tracking for cancellation
active_pipelines_lock = Lock()
active_pipelines = {}  # Maps thread_id to (cache_id, pipeline_title)


def cancel_pipeline(cache_id: str, pipeline_title: str = "Unknown"):
    """Cancel a running pipeline by making a DELETE request."""
    try:
        delete_url = f"{ENDPOINT_URL}/pipeline/data"
        logger.info(f"Cancelling pipeline {pipeline_title} with cache_id: {cache_id}")

        # Try to cancel with limited retries (fewer than normal since this is cleanup)
        max_cancel_retries = 3
        last_exception = None

        for attempt in range(1, max_cancel_retries + 1):
            try:
                response = requests.delete(
                    delete_url,
                    params={"cache_id": cache_id},
                    verify=False,
                    headers={"BCODMO-API-KEY": BCODMO_API_KEY},
                    timeout=5,  # Short timeout for cancellation
                )

                if response.status_code == 200:
                    logger.info(f"Successfully cancelled pipeline {pipeline_title}")
                else:
                    logger.warning(
                        f"Cancellation request for {pipeline_title} returned status {response.status_code}"
                    )
                return

            except (
                requests.exceptions.RequestException,
                requests.exceptions.HTTPError,
            ) as e:
                last_exception = e
                if attempt < max_cancel_retries:
                    logger.warning(
                        f"Failed to cancel pipeline {pipeline_title} (attempt {attempt}/{max_cancel_retries}): {e}"
                    )
                    time.sleep(0.5)  # Shorter delay for cancellation retries

        if last_exception:
            logger.warning(
                f"Failed to cancel pipeline {pipeline_title} after {max_cancel_retries} attempts: {last_exception}"
            )

    except Exception as e:
        logger.warning(f"Unexpected error cancelling pipeline {pipeline_title}: {e}")


def signal_handler(signum, frame):
    """Handle interrupt signal for graceful shutdown."""
    logger.warning("\n\n" + "=" * 60)
    logger.warning("INTERRUPT RECEIVED! Starting graceful shutdown...")
    logger.warning("=" * 60)

    # Set shutdown event immediately
    shutdown_event.set()

    # Show what's currently active
    with active_pipelines_lock:
        active_count = len(active_pipelines)
        if active_count > 0:
            logger.warning(f"Currently processing {active_count} pipeline(s):")
            for thread_id, (cache_id, title) in active_pipelines.items():
                logger.warning(f"  - {title} (cache_id: {cache_id})")
            logger.warning("Sending cancellation requests...")

    # Cancel all active pipelines
    with active_pipelines_lock:
        active_cache_ids = list(active_pipelines.values())

    for cache_id, pipeline_title in active_cache_ids:
        cancel_pipeline(cache_id, pipeline_title)

    logger.warning("Interrupt handling complete. Waiting for threads to finish...")
    logger.warning(
        "Note: Large file comparisons will be stopped at the next checkpoint."
    )


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)


def retry_request(request_func, *args, **kwargs):
    """
    Retry a request function up to MAX_RETRIES times on HTTP errors.

    Args:
        request_func: The request function to call (e.g., requests.get, requests.post)
        *args, **kwargs: Arguments to pass to the request function

    Returns:
        The response object if successful

    Raises:
        The last exception if all retries fail
    """
    last_exception = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = request_func(*args, **kwargs)
            response.raise_for_status()  # Raises HTTPError for bad status codes
            return response
        except (
            requests.exceptions.RequestException,
            requests.exceptions.HTTPError,
        ) as e:
            last_exception = e
            if attempt < MAX_RETRIES:
                logger.warning(
                    f"HTTP request failed (attempt {attempt}/{MAX_RETRIES}): {e}. "
                    f"Retrying in {RETRY_DELAY} seconds..."
                )
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"HTTP request failed after {MAX_RETRIES} attempts: {e}")

    raise last_exception


def check_test_output_exists(test_prefix: str) -> bool:
    """
    Check if output files already exist in the test prefix location.
    Returns True if at least one file exists in the prefix.
    """
    try:
        # Ensure prefix has trailing slash for proper listing
        if not test_prefix.endswith("/"):
            test_prefix = test_prefix + "/"

        # Check if any objects exist with this prefix
        with s3_lock:
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=test_prefix,
                MaxKeys=1,  # We only need to know if at least one file exists
            )

        # If 'Contents' key exists and has items, files exist
        exists = "Contents" in response and len(response["Contents"]) > 0

        if exists:
            # Get count of files for logging
            with s3_lock:
                full_response = s3_client.list_objects_v2(
                    Bucket=S3_BUCKET, Prefix=test_prefix
                )
            file_count = len(full_response.get("Contents", []))
            logger.info(
                f"Found {file_count} existing file(s) in test output location: {test_prefix}"
            )

        return exists

    except Exception as e:
        logger.warning(f"Error checking for existing test output at {test_prefix}: {e}")
        return False


def load_potentially_buggy_pipelines(filename: str) -> List[Dict]:
    """Load the JSON file containing potentially buggy pipelines."""
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {filename}: {e}")
        raise


def download_pipeline_spec(pipeline_spec_path: str) -> Dict:
    """Download and parse pipeline spec from S3."""
    try:
        with s3_lock:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=pipeline_spec_path)
        content = response["Body"].read().decode("utf-8")
        spec = yaml.safe_load(content)
        return spec
    except Exception as e:
        logger.error(f"Failed to download pipeline spec {pipeline_spec_path}: {e}")
        raise


def download_csv_from_s3(key: str) -> pd.DataFrame:
    """Download CSV from S3 and return as pandas DataFrame with strings preserved."""
    # Check for shutdown before downloading
    if shutdown_event.is_set():
        raise Exception("Download cancelled due to interrupt")

    try:
        with s3_lock:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        content = response["Body"].read().decode("utf-8")

        # Check again after download in case of large files
        if shutdown_event.is_set():
            raise Exception("Processing cancelled due to interrupt")

        # Read with dtype=str to preserve exact string representations from CSV
        # This prevents pandas from converting to scientific notation or removing trailing zeros
        return pd.read_csv(
            io.StringIO(content), dtype=str, na_values=None, keep_default_na=False
        )
    except Exception as e:
        if shutdown_event.is_set():
            raise Exception("Download cancelled due to interrupt")
        logger.error(f"Failed to download CSV {key}: {e}")
        raise


def compare_csv_contents(new_path: str, original_path: str, filename: str) -> Dict:
    """
    Compare the actual contents of two CSV files and identify cell-level differences.
    Returns detailed comparison including specific cell mismatches.
    Now interruptible during long comparisons.
    """
    try:
        # Check for shutdown before starting
        if shutdown_event.is_set():
            return {
                "filename": filename,
                "new_path": new_path,
                "original_path": original_path,
                "error": "Comparison interrupted by user",
            }

        # Download both CSV files - read as strings to preserve original formatting
        new_df = download_csv_from_s3(new_path)
        original_df = download_csv_from_s3(original_path)

        comparison = {
            "filename": filename,
            "new_path": new_path,
            "original_path": original_path,
            "headers_match": True,
            "shape_match": True,
            "cell_differences": [],
            "new_shape": list(new_df.shape),
            "original_shape": list(original_df.shape),
            "error": None,
        }

        # Check if headers match
        new_columns = list(new_df.columns)
        original_columns = list(original_df.columns)

        if new_columns != original_columns:
            comparison["headers_match"] = False
            comparison["error"] = "Headers do not match"
            comparison["new_headers"] = new_columns
            comparison["original_headers"] = original_columns
            logger.error(
                f"Headers mismatch for {filename}. New: {new_columns}, Original: {original_columns}"
            )
            return comparison

        # Check if shapes match
        if new_df.shape != original_df.shape:
            comparison["shape_match"] = False
            logger.warning(
                f"Shape mismatch for {filename}. New: {new_df.shape}, Original: {original_df.shape}"
            )

        # Compare cell by cell (for the minimum common shape)
        min_rows = min(len(new_df), len(original_df))
        total_cells = min_rows * len(new_columns)
        cells_checked = 0
        last_progress_log = 0

        for row_idx in range(min_rows):
            # Check for interrupt every 100 rows
            if row_idx % 100 == 0 and shutdown_event.is_set():
                logger.warning(
                    f"Cell comparison interrupted for {filename} at row {row_idx}"
                )
                comparison["error"] = "Comparison interrupted by user"
                comparison["total_differences"] = len(comparison["cell_differences"])
                return comparison

            for col in new_columns:
                cells_checked += 1

                # Log progress for large files
                if total_cells > 10000 and cells_checked % 5000 == 0:
                    progress = (cells_checked / total_cells) * 100
                    if progress - last_progress_log >= 10:  # Log every 10%
                        logger.info(
                            f"Comparing {filename}: {progress:.0f}% complete ({cells_checked}/{total_cells} cells)"
                        )
                        last_progress_log = progress

                new_value = new_df.iloc[row_idx][col]
                original_value = original_df.iloc[row_idx][col]

                # Direct string comparison - this preserves trailing zeros and format
                if new_value != original_value:
                    comparison["cell_differences"].append(
                        {
                            "row": row_idx,
                            "column": col,
                            "new_value": new_value,
                            "original_value": original_value,
                        }
                    )

        # Check for interrupt before processing extra rows
        if shutdown_event.is_set():
            logger.warning(
                f"Cell comparison interrupted for {filename} before checking extra rows"
            )
            comparison["error"] = "Comparison interrupted by user"
            comparison["total_differences"] = len(comparison["cell_differences"])
            return comparison

        # Check for extra rows in new file
        if len(new_df) > len(original_df):
            for row_idx in range(len(original_df), len(new_df)):
                # Check for interrupt every 100 rows
                if row_idx % 100 == 0 and shutdown_event.is_set():
                    logger.warning(
                        f"Cell comparison interrupted for {filename} while checking extra rows"
                    )
                    comparison["error"] = "Comparison interrupted by user"
                    comparison["total_differences"] = len(
                        comparison["cell_differences"]
                    )
                    return comparison

                for col in new_columns:
                    comparison["cell_differences"].append(
                        {
                            "row": row_idx,
                            "column": col,
                            "new_value": new_df.iloc[row_idx][col],
                            "original_value": "ROW_NOT_IN_ORIGINAL",
                        }
                    )

        # Check for extra rows in original file
        elif len(original_df) > len(new_df):
            for row_idx in range(len(new_df), len(original_df)):
                # Check for interrupt every 100 rows
                if row_idx % 100 == 0 and shutdown_event.is_set():
                    logger.warning(
                        f"Cell comparison interrupted for {filename} while checking extra rows"
                    )
                    comparison["error"] = "Comparison interrupted by user"
                    comparison["total_differences"] = len(
                        comparison["cell_differences"]
                    )
                    return comparison

                for col in original_columns:
                    comparison["cell_differences"].append(
                        {
                            "row": row_idx,
                            "column": col,
                            "new_value": "ROW_NOT_IN_NEW",
                            "original_value": original_df.iloc[row_idx][col],
                        }
                    )

        comparison["total_differences"] = len(comparison["cell_differences"])

        if comparison["total_differences"] > 0:
            logger.info(
                f"Found {comparison['total_differences']} cell differences in {filename}"
            )
            # Log first few differences as examples
            for diff in comparison["cell_differences"][:5]:
                logger.debug(
                    f"  Row {diff['row']}, Column '{diff['column']}': '{diff['original_value']}' -> '{diff['new_value']}'"
                )
            if comparison["total_differences"] > 5:
                logger.debug(
                    f"  ... and {comparison['total_differences'] - 5} more differences"
                )

        return comparison

    except Exception as e:
        if shutdown_event.is_set():
            return {
                "filename": filename,
                "new_path": new_path,
                "original_path": original_path,
                "error": "Comparison interrupted by user",
            }
        logger.error(f"Error comparing CSV contents for {filename}: {e}")
        return {
            "filename": filename,
            "new_path": new_path,
            "original_path": original_path,
            "error": str(e),
        }


def validate_and_modify_pipeline_steps(
    pipeline_steps: List[Dict], pipeline_title: str
) -> Tuple[List[Dict], str, Dict]:
    """
    Validate pipeline steps and modify the dump_to_s3 step.
    Returns the modified steps, the original prefix, and metadata (author info, submission IDs).
    """
    # Deep copy to avoid modifying the original
    modified_steps = copy.deepcopy(pipeline_steps)

    # Find all dump_to_s3 steps
    dump_steps = []
    for i, step in enumerate(modified_steps):
        if step.get("run") == "bcodmo_pipeline_processors.dump_to_s3":
            dump_steps.append(i)

    # Validate: exactly one dump_to_s3 step at the end
    if len(dump_steps) == 0:
        error_msg = f"No dump_to_s3 step found in pipeline {pipeline_title}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    if len(dump_steps) > 1:
        error_msg = f"Multiple dump_to_s3 steps found in pipeline {pipeline_title} at positions: {dump_steps}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    if dump_steps[0] != len(modified_steps) - 1:
        error_msg = f"dump_to_s3 step is not the last step in pipeline {pipeline_title}. Found at position {dump_steps[0]} of {len(modified_steps)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Get original prefix and extract metadata from the dump_to_s3 step
    dump_step = modified_steps[-1]
    original_prefix = dump_step.get("parameters", {}).get("prefix", "")

    # Extract author information and submission IDs
    metadata = {"author_name": None, "author_orcid": None, "submission_ids": []}

    parameters = dump_step.get("parameters", {})
    data_manager = parameters.get("data_manager", {})

    if data_manager:
        metadata["author_name"] = data_manager.get("name")
        metadata["author_orcid"] = data_manager.get("orcid")

    submission_ids = parameters.get("submission_ids", [])
    if submission_ids:
        metadata["submission_ids"] = submission_ids

    # Modify the prefix
    dump_step["parameters"]["prefix"] = f"{TEST_PREFIX}/{pipeline_title}"

    return modified_steps, original_prefix, metadata


def run_pipeline(
    pipeline_steps: List[Dict], pipeline_title: str, preserve_missing_values: bool
) -> str:
    """Submit pipeline for execution and return cache_id."""
    payload = {
        "cache_id": None,
        "metadata": {
            "datasetId": "EXCEL_BUG_TEST",
            "datasetVersion": pipeline_title,
            "description": "",
            "title": "test",
            "submissionIds": [],
        },
        "preserve_missing_values": preserve_missing_values,
        "run_big_worker": True,
        "steps": pipeline_steps,
        "verbose": True,
    }

    try:
        # Use retry logic for the HTTP request
        response = retry_request(
            requests.post,
            f"{ENDPOINT_URL}/pipeline/run",
            json=payload,
            verify=False,  # Don't verify SSL
            headers={"BCODMO-API-KEY": BCODMO_API_KEY},
        )

        result = response.json()

        # Check for application-level errors (not HTTP errors)
        if result.get("status_code") != 0:
            raise ValueError(
                f"Pipeline submission failed with status_code: {result.get('status_code')}"
            )

        cache_id = result.get("cache_id")
        if not cache_id:
            raise ValueError("No cache_id returned from pipeline submission")

        logger.info(f"Pipeline {pipeline_title} submitted with cache_id: {cache_id}")
        return cache_id

    except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
        # HTTP-level errors that failed even after retries
        logger.error(
            f"Failed to submit pipeline {pipeline_title} after {MAX_RETRIES} retries: {e}"
        )
        raise
    except Exception as e:
        # Application-level errors or other exceptions
        logger.error(f"Failed to submit pipeline {pipeline_title}: {e}")
        raise


def wait_for_pipeline_completion(
    cache_id: str, pipeline_title: str
) -> Tuple[str, Optional[str]]:
    """Poll for pipeline completion and return final status and any error message."""
    status_url = f"{ENDPOINT_URL}/pipeline/status"

    # Register this as an active pipeline
    import threading

    thread_id = threading.current_thread().ident
    with active_pipelines_lock:
        active_pipelines[thread_id] = (cache_id, pipeline_title)

    # Track wait time for periodic logging
    start_time = time.time()
    last_log_time = start_time
    log_interval = 60  # seconds

    try:
        while not shutdown_event.is_set():
            try:
                # Log progress every 60 seconds
                current_time = time.time()
                elapsed_time = current_time - start_time
                if current_time - last_log_time >= log_interval:
                    minutes = int(elapsed_time // 60)
                    seconds = int(elapsed_time % 60)
                    logger.info(
                        f"Still waiting for pipeline '{pipeline_title}' to complete... "
                        f"(Total wait time: {minutes}m {seconds}s)"
                    )
                    last_log_time = current_time

                # Use retry logic for the HTTP request
                response = retry_request(
                    requests.get,
                    status_url,
                    params={"cache_id": cache_id},
                    verify=False,  # Don't verify SSL
                    headers={"BCODMO-API-KEY": BCODMO_API_KEY},
                )

                result = response.json()

                pipeline_status = result.get("pipeline_status", "UNKNOWN")
                error_message = result.get("error")

                if pipeline_status == "SUCCESS":
                    # Check for error even if status is SUCCESS
                    if error_message:
                        logger.error(
                            f"Pipeline {pipeline_title} completed with SUCCESS status but has error: {error_message}"
                        )
                        # Treat this as an error case
                        return "ERROR_WITH_SUCCESS", error_message
                    else:
                        total_wait = time.time() - start_time
                        minutes = int(total_wait // 60)
                        seconds = int(total_wait % 60)
                        logger.info(
                            f"Pipeline {pipeline_title} completed successfully "
                            f"(Total time: {minutes}m {seconds}s)"
                        )
                        return "SUCCESS", error_message
                elif pipeline_status == "SENT":
                    # Still processing
                    time.sleep(POLL_INTERVAL)
                else:
                    # Any other status is an error
                    error_msg = f"Pipeline {pipeline_title} failed with status: {pipeline_status}"
                    if error_message:
                        error_msg += f" - Error: {error_message}"
                    logger.error(error_msg)
                    return pipeline_status, error_message

            except (
                requests.exceptions.RequestException,
                requests.exceptions.HTTPError,
            ) as e:
                if shutdown_event.is_set():
                    return "INTERRUPTED", "Processing interrupted by user"
                # HTTP errors that failed even after retries
                logger.error(
                    f"Error checking pipeline status for {pipeline_title} after {MAX_RETRIES} retries: {e}"
                )
                raise
            except Exception as e:
                if shutdown_event.is_set():
                    return "INTERRUPTED", "Processing interrupted by user"
                logger.error(
                    f"Unexpected error checking pipeline status for {pipeline_title}: {e}"
                )
                raise

        return "INTERRUPTED", "Processing interrupted by user"

    finally:
        # Remove from active pipelines when done
        with active_pipelines_lock:
            if thread_id in active_pipelines:
                del active_pipelines[thread_id]


def list_csv_files(prefix: str) -> List[str]:
    """List all CSV files at the top level of the given S3 prefix."""
    try:
        # Ensure prefix has trailing slash for proper delimiter behavior
        if not prefix.endswith("/"):
            prefix = prefix + "/"

        with s3_lock:
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=prefix,
                Delimiter="/",  # This ensures we only get top-level files
            )

        files = []
        if "Contents" in response:
            for obj in response["Contents"]:
                key = obj["Key"]
                if key.endswith(".csv"):
                    files.append(key)

        return files

    except Exception as e:
        logger.error(f"Failed to list CSV files in {prefix}: {e}")
        return []


def get_file_etag(key: str) -> Optional[str]:
    """Get the ETag for a file in S3."""
    try:
        with s3_lock:
            response = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return response.get("ETag", "").strip('"')
    except Exception as e:
        logger.warning(f"Failed to get ETag for {key}: {e}")
        return None


def get_file_metadata(key: str) -> Dict:
    """Get metadata for a file including last modified timestamp."""
    try:
        with s3_lock:
            response = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return {
            "etag": response.get("ETag", "").strip('"'),
            "last_modified": (
                response.get("LastModified").isoformat()
                if response.get("LastModified")
                else None
            ),
            "size": response.get("ContentLength"),
        }
    except Exception as e:
        logger.warning(f"Failed to get metadata for {key}: {e}")
        return {}


def compare_csv_files(new_prefix: str, original_prefix: str) -> Dict:
    """Compare CSV files between new and original locations."""
    # Check for shutdown before starting
    if shutdown_event.is_set():
        return {
            "total_files_compared": 0,
            "files_with_differences": 0,
            "files_only_in_new": [],
            "files_only_in_original": [],
            "file_comparisons": [],
            "detailed_differences": [],
            "error": "Comparison interrupted by user",
        }

    # List CSV files in both locations
    new_files = list_csv_files(new_prefix)
    original_files = list_csv_files(original_prefix)

    # Extract just the filenames for comparison
    new_filenames = {Path(f).name: f for f in new_files}
    original_filenames = {Path(f).name: f for f in original_files}

    comparison_result = {
        "total_files_compared": 0,
        "files_with_differences": 0,
        "files_only_in_new": [],
        "files_only_in_original": [],
        "file_comparisons": [],
        "detailed_differences": [],
    }

    # Files only in new location
    for filename in new_filenames:
        if filename not in original_filenames:
            comparison_result["files_only_in_new"].append(filename)
            logger.error(
                f"CSV file '{filename}' found in new test path but not in original path: {original_prefix}"
            )

    # Files only in original location
    for filename in original_filenames:
        if filename not in new_filenames:
            comparison_result["files_only_in_original"].append(filename)

    # Compare files that exist in both locations
    files_to_compare = [
        (filename, new_filenames[filename], original_filenames[filename])
        for filename in new_filenames
        if filename in original_filenames
    ]

    for idx, (filename, new_path, original_path) in enumerate(files_to_compare):
        # Check for interrupt before each file comparison
        if shutdown_event.is_set():
            logger.warning(
                f"File comparison interrupted after {idx}/{len(files_to_compare)} files"
            )
            comparison_result["error"] = "Comparison interrupted by user"
            break

        new_metadata = get_file_metadata(new_path)
        original_metadata = get_file_metadata(original_path)

        if new_metadata.get("etag") and original_metadata.get("etag"):
            comparison_result["total_files_compared"] += 1

            file_comparison = {
                "filename": filename,
                "new_path": new_path,
                "original_path": original_path,
                "new_etag": new_metadata.get("etag"),
                "original_etag": original_metadata.get("etag"),
                "new_size": new_metadata.get("size"),
                "original_size": original_metadata.get("size"),
                "original_last_modified": original_metadata.get("last_modified"),
                "etags_match": new_metadata.get("etag")
                == original_metadata.get("etag"),
            }

            if not file_comparison["etags_match"]:
                comparison_result["files_with_differences"] += 1
                logger.warning(
                    f"ETag mismatch for {filename}: {new_metadata.get('etag')} != {original_metadata.get('etag')}"
                )

                # Check for interrupt before detailed comparison
                if shutdown_event.is_set():
                    logger.warning(
                        f"Skipping detailed comparison for {filename} due to interrupt"
                    )
                    file_comparison["comparison_error"] = (
                        "Detailed comparison skipped due to interrupt"
                    )
                else:
                    # Perform detailed cell-level comparison
                    logger.info(f"Performing detailed comparison for {filename}")
                    detailed_comparison = compare_csv_contents(
                        new_path, original_path, filename
                    )
                    comparison_result["detailed_differences"].append(
                        detailed_comparison
                    )

                    # Add summary of cell differences to file comparison
                    file_comparison["cell_differences_count"] = detailed_comparison.get(
                        "total_differences", 0
                    )
                    file_comparison["headers_match"] = detailed_comparison.get(
                        "headers_match", True
                    )
                    if detailed_comparison.get("error"):
                        file_comparison["comparison_error"] = detailed_comparison[
                            "error"
                        ]

            comparison_result["file_comparisons"].append(file_comparison)

    return comparison_result


def process_pipeline(entry: Dict, preserve_missing_values=True) -> Dict:
    """Process a single pipeline entry."""
    if shutdown_event.is_set():
        return {
            "pipeline_name": entry.get("pipeline_name"),
            "status": "skipped",
            "errors": ["Processing interrupted before start"],
            "author_name": None,
            "author_orcid": None,
            "submission_ids": [],
        }

    result = {
        "pipeline_name": entry.get("pipeline_name"),
        "pipeline_spec": entry.get("pipeline_spec"),
        "excel_file": entry.get("excel_file"),
        "sheet_name": entry.get("sheet_name"),
        "error_location": entry.get("error_location"),
        "row": entry.get("row"),
        "column": entry.get("column"),
        "old_value": entry.get("old_value"),
        "new_value": entry.get("new_value"),
        "format_type": entry.get("format_type"),
        "last_updated": entry.get("last_updated"),
        "processing_timestamp": datetime.utcnow().isoformat(),
        "status": "processing",
        "errors": [],
        # Initialize author and submission info
        "author_name": None,
        "author_orcid": None,
        "submission_ids": [],
    }

    try:
        # Download pipeline spec
        logger.info(f"Processing pipeline: {entry.get('pipeline_name')}")
        spec = download_pipeline_spec(entry["pipeline_spec"])

        # Get the pipeline title (assuming single top-level key)
        pipeline_title = list(spec.keys())[0]
        pipeline_config = spec[pipeline_title]
        pipeline_steps = pipeline_config.get("pipeline", [])

        result["pipeline_title"] = pipeline_title

        # Validate and modify pipeline steps, and extract metadata
        modified_steps, original_prefix, metadata = validate_and_modify_pipeline_steps(
            pipeline_steps, pipeline_title
        )
        result["original_prefix"] = original_prefix
        result["test_prefix"] = f"{TEST_PREFIX}/{pipeline_title}"

        # Add author and submission information
        result["author_name"] = metadata.get("author_name")
        result["author_orcid"] = metadata.get("author_orcid")
        result["submission_ids"] = metadata.get("submission_ids", [])

        # Check if test output already exists
        # Note: If output exists, we use it regardless of preserve_missing_values setting
        # since we assume the existing output is valid
        test_output_exists = check_test_output_exists(result["test_prefix"])

        if test_output_exists:
            # Skip pipeline execution and use existing output
            logger.info(f"Using existing test output for pipeline {pipeline_title}")
            result["cache_id"] = "REUSED_EXISTING_OUTPUT"
            result["pipeline_execution_status"] = "SUCCESS"
            result["pipeline_reused_existing"] = True

            # Log that we're skipping execution
            logger.info(
                f"Skipping pipeline execution for {pipeline_title} - output already exists"
            )
        else:
            # Check for interrupt before running pipeline
            if shutdown_event.is_set():
                result["status"] = "interrupted"
                result["errors"].append(
                    "Processing interrupted before pipeline execution"
                )
                return result

            # Run the pipeline
            logger.info(f"Running pipeline {pipeline_title} - no existing output found")
            cache_id = run_pipeline(
                modified_steps,
                pipeline_title,
                preserve_missing_values=preserve_missing_values,
            )
            result["cache_id"] = cache_id
            result["pipeline_reused_existing"] = False

            # Wait for completion
            final_status, error_message = wait_for_pipeline_completion(
                cache_id, pipeline_title
            )
            result["pipeline_execution_status"] = final_status

            if error_message:
                result["pipeline_error_message"] = error_message
                result["errors"].append(f"Pipeline returned error: {error_message}")

            if final_status == "INTERRUPTED":
                result["status"] = "interrupted"
                return result

            # Handle both ERROR_WITH_SUCCESS and other error statuses
            if final_status != "SUCCESS":
                result["status"] = "error"
                if final_status == "ERROR_WITH_SUCCESS":
                    result["errors"].append(
                        f"Pipeline returned SUCCESS but with error: {error_message}"
                    )
                else:
                    result["errors"].append(
                        f"Pipeline execution failed with status: {final_status}"
                    )
                return result

        # Compare CSV files
        comparison = compare_csv_files(
            f"{TEST_PREFIX}/{pipeline_title}", original_prefix
        )

        # Check if comparison was interrupted
        if comparison.get("error") == "Comparison interrupted by user":
            result["status"] = "interrupted"
            result["errors"].append("File comparison interrupted by user")
            result["comparison"] = comparison
            return result

        # Find differences that are actually issues of preserve_missing_values and rerun if necessary
        if preserve_missing_values == True and not shutdown_event.is_set():
            for detailed_diff in comparison["detailed_differences"]:
                # Skip if this comparison was interrupted
                if detailed_diff.get("error") == "Comparison interrupted by user":
                    continue

                for cell_diff in detailed_diff.get("cell_differences", []):
                    if (
                        cell_diff["original_value"] == ""
                        and cell_diff["new_value"] != ""
                    ):
                        logger.warn(
                            f"Pipeline {pipeline_title} has missing values differences - rerunning with preserve_missing_values=False"
                        )
                        # When rerunning, the check for existing output will happen again
                        return process_pipeline(entry, preserve_missing_values=False)
        result["comparison"] = comparison
        result["preserve_missing_values"] = preserve_missing_values

        # Check for header mismatches that would indicate an error
        has_header_mismatch = False
        has_interrupted_comparisons = False

        for detailed_diff in comparison.get("detailed_differences", []):
            if detailed_diff.get("error") == "Comparison interrupted by user":
                has_interrupted_comparisons = True
                continue
            if not detailed_diff.get("headers_match", True):
                has_header_mismatch = True
                result["errors"].append(
                    f"Headers mismatch in file {detailed_diff.get('filename')}"
                )
                break

        if has_interrupted_comparisons and not has_header_mismatch:
            result["status"] = "interrupted"
            result["is_buggy"] = None
            result["errors"].append("Some file comparisons were interrupted")
            logger.warning(f"Pipeline {pipeline_title} has interrupted comparisons")
        elif has_header_mismatch:
            result["status"] = "error"
            result["is_buggy"] = None
            logger.error(
                f"Pipeline {pipeline_title} has header mismatches - marking as error"
            )
        elif comparison["files_with_differences"] > 0:
            result["status"] = "buggy"
            result["is_buggy"] = True

            # Calculate total cell differences across all files
            total_cell_differences = sum(
                diff.get("total_differences", 0)
                for diff in comparison.get("detailed_differences", [])
            )
            result["total_cell_differences"] = total_cell_differences

            logger.warning(
                f"Pipeline {pipeline_title} has {comparison['files_with_differences']} files with differences "
                f"and {total_cell_differences} total cell differences"
            )
        else:
            result["status"] = "not_buggy"
            result["is_buggy"] = False
            logger.info(f"Pipeline {pipeline_title} has no differences")

    except Exception as e:
        logger.error(f"Error processing pipeline {entry.get('pipeline_name')}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        result["status"] = "error"
        result["errors"].append(str(e))
        result["is_buggy"] = None

    return result


def process_pipeline_with_index(index: int, entry: Dict) -> Tuple[int, Dict]:
    """Wrapper function to process pipeline and return with its index."""
    result = process_pipeline(entry)
    return index, result


def save_results(results: List[Dict], filename: str, total_expected: int):
    """Save results to JSON file with summary."""
    # Filter to only actually buggy pipelines and errors
    actual_buggy = [r for r in results if r.get("is_buggy") == True]
    errors = [r for r in results if r.get("status") == "error"]
    interrupted = [r for r in results if r.get("status") == "interrupted"]
    skipped = [r for r in results if r.get("status") == "skipped"]

    # Count pipelines that reused existing output
    reused_existing = [r for r in results if r.get("pipeline_reused_existing") == True]

    # Count pipelines with SUCCESS status but errors (these are now marked as "error")
    success_with_errors = [
        r for r in errors if r.get("pipeline_execution_status") == "ERROR_WITH_SUCCESS"
    ]

    # Count HTTP/network errors
    http_errors = [
        r
        for r in errors
        if any("HTTP/Network error" in str(e) for e in r.get("errors", []))
    ]

    # Count pipelines with interrupted comparisons
    comparison_interrupted = [
        r
        for r in results
        if r.get("comparison", {}).get("error") == "Comparison interrupted by user"
        or any(
            d.get("error") == "Comparison interrupted by user"
            for d in r.get("comparison", {}).get("detailed_differences", [])
        )
    ]

    # Collect author statistics
    authors_with_bugs = {}
    for pipeline in actual_buggy:
        author = pipeline.get("author_name", "Unknown")
        if author:
            if author not in authors_with_bugs:
                authors_with_bugs[author] = {
                    "count": 0,
                    "orcid": pipeline.get("author_orcid"),
                    "pipelines": [],
                }
            authors_with_bugs[author]["count"] += 1
            authors_with_bugs[author]["pipelines"].append(
                pipeline.get("pipeline_title")
            )

    # Create summary
    summary = {
        "execution_timestamp": datetime.utcnow().isoformat(),
        "total_pipelines_expected": total_expected,
        "total_pipelines_processed": len(results),
        "total_completed": len(
            [r for r in results if r.get("status") not in ["interrupted", "skipped"]]
        ),
        "total_buggy_pipelines": len(actual_buggy),
        "total_errors": len(errors),
        "total_reused_existing_output": len(reused_existing),
        "total_newly_executed": len(
            [
                r
                for r in results
                if r.get("pipeline_reused_existing") == False
                and r.get("status") not in ["skipped", "error"]
            ]
        ),
        "total_success_with_errors": len(success_with_errors),
        "total_http_errors": len(http_errors),
        "total_interrupted": len(interrupted),
        "total_comparison_interrupted": len(comparison_interrupted),
        "total_skipped": len(skipped),
        "was_interrupted": total_expected != len(results)
        or len(interrupted) > 0
        or len(comparison_interrupted) > 0,
        "authors_with_buggy_pipelines": authors_with_bugs,
        "buggy_pipelines": actual_buggy,
        "errors": errors,
        "interrupted_pipelines": interrupted,
        "all_results": results,  # Include all results for completeness
    }

    # Save results
    with open(filename, "w") as f:
        json.dump(summary, f, indent=2, default=str)

    logger.info(f"Results saved to {filename}")
    return summary


def main():
    """Main execution function with threading support."""
    logger.info("Starting pipeline bug detection script with threading")
    logger.info(f"Using {MAX_WORKERS} worker threads")
    logger.info(f"HTTP requests will be retried up to {MAX_RETRIES} times on failure")
    logger.info("Will check for existing test output and reuse when available")
    logger.info(
        "Pipeline author information and submission IDs will be extracted from specs"
    )
    logger.info(
        "Progress updates will be logged every 60 seconds for long-running pipelines"
    )
    logger.info("Logs are being written to pipeline_bug_detector.log")
    logger.info("Press Ctrl+C to interrupt and save partial results")
    logger.info(
        "Large file comparisons can be interrupted and will stop at checkpoints"
    )

    # Load potentially buggy pipelines
    try:
        pipelines = load_potentially_buggy_pipelines("potentially_buggy_pipelines.json")
        logger.info(f"Loaded {len(pipelines)} potentially buggy pipelines")
    except Exception as e:
        logger.error(f"Failed to load pipelines: {e}")
        return

    # Process pipelines using ThreadPoolExecutor
    results = [None] * len(pipelines)  # Pre-allocate list to maintain order
    completed_count = 0

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_index = {
                executor.submit(process_pipeline_with_index, i, entry): i
                for i, entry in enumerate(pipelines)
            }

            # Process completed tasks
            for future in as_completed(future_to_index):
                if shutdown_event.is_set():
                    logger.info(
                        "Interrupt detected - waiting for active pipelines to be cancelled..."
                    )
                    logger.info(
                        "Note: File comparisons in progress will be stopped at checkpoints"
                    )
                    # Cancel remaining futures
                    for f in future_to_index:
                        if not f.done():
                            f.cancel()

                    # Give a moment for cancellations to process
                    time.sleep(2)
                    break

                try:
                    index, result = future.result()
                    results[index] = result
                    completed_count += 1

                    # Store in global results for interrupt handling
                    with results_lock:
                        completed_results[index] = result

                    logger.info(
                        f"Completed {completed_count}/{len(pipelines)}: {result.get('pipeline_name')} - Status: {result.get('status')}"
                    )
                except Exception as e:
                    index = future_to_index[future]
                    pipeline_name = pipelines[index].get("pipeline_name", "Unknown")
                    error_msg = str(e)

                    # Check if it's an HTTP/network error
                    if isinstance(
                        e,
                        (
                            requests.exceptions.RequestException,
                            requests.exceptions.HTTPError,
                        ),
                    ):
                        error_msg = f"HTTP/Network error after {MAX_RETRIES} retries: {error_msg}"
                    else:
                        error_msg = f"Thread execution error: {error_msg}"

                    logger.error(
                        f"Error processing pipeline at index {index}: {error_msg}"
                    )
                    results[index] = {
                        "pipeline_name": pipeline_name,
                        "status": "error",
                        "errors": [error_msg],
                        "author_name": None,
                        "author_orcid": None,
                        "submission_ids": [],
                    }
                    completed_count += 1

    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt received during processing")
        shutdown_event.set()

    # Filter out None values (unprocessed pipelines)
    processed_results = [r for r in results if r is not None]

    # Mark unprocessed pipelines as skipped
    for i, result in enumerate(results):
        if result is None:
            results[i] = {
                "pipeline_name": pipelines[i].get("pipeline_name", "Unknown"),
                "status": "skipped",
                "errors": ["Pipeline was not processed due to interruption"],
                "author_name": None,
                "author_orcid": None,
                "submission_ids": [],
            }

    # Save results
    output_file = "actual_buggy_pipelines.json"
    summary = save_results(
        [r for r in results if r is not None], output_file, len(pipelines)
    )

    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Total pipelines expected: {len(pipelines)}")
    print(f"Total pipelines processed: {summary['total_pipelines_processed']}")
    print(f"Total completed: {summary['total_completed']}")

    if summary.get("total_reused_existing_output", 0) > 0:
        print(f"  - Reused existing output: {summary['total_reused_existing_output']}")
        print(f"  - Newly executed: {summary.get('total_newly_executed', 0)}")

    print(f"Buggy pipelines found: {summary['total_buggy_pipelines']}")
    print(f"Pipelines with errors: {summary['total_errors']}")

    if summary["total_success_with_errors"] > 0:
        print(
            f"  - SUCCESS status but with errors: {summary['total_success_with_errors']}"
        )
    if summary["total_http_errors"] > 0:
        print(
            f"  - HTTP/Network failures (after {MAX_RETRIES} retries): {summary['total_http_errors']}"
        )

    if summary["total_interrupted"] > 0:
        print(f"Pipelines interrupted: {summary['total_interrupted']}")
    if summary.get("total_comparison_interrupted", 0) > 0:
        print(
            f"Pipelines with interrupted comparisons: {summary['total_comparison_interrupted']}"
        )
    if summary["total_skipped"] > 0:
        print(f"Pipelines skipped: {summary['total_skipped']}")

    if summary["buggy_pipelines"]:
        print("\nBuggy pipelines:")
        for pipeline in summary["buggy_pipelines"]:
            files_diff = pipeline["comparison"]["files_with_differences"]
            files_total = pipeline["comparison"]["total_files_compared"]
            cell_diff = pipeline.get("total_cell_differences", 0)
            author_info = ""
            if pipeline.get("author_name"):
                author_info = f" (Author: {pipeline['author_name']})"
            print(
                f"  - {pipeline['pipeline_title']}{author_info}: {files_diff} files differ out of {files_total} "
                f"({cell_diff} total cell differences)"
            )

        # Print author statistics if available
        if summary.get("authors_with_buggy_pipelines"):
            print("\nAuthors with buggy pipelines:")
            for author, info in summary["authors_with_buggy_pipelines"].items():
                orcid_info = f" (ORCID: {info['orcid']})" if info.get("orcid") else ""
                print(f"  - {author}{orcid_info}: {info['count']} buggy pipeline(s)")

    if summary["errors"]:
        print("\nPipelines with errors:")
        for pipeline in summary["errors"]:
            error_type = ""
            if pipeline.get("pipeline_execution_status") == "ERROR_WITH_SUCCESS":
                error_type = " [SUCCESS with error]"

            # Check if any errors are HTTP/network related
            errors = pipeline.get("errors", [])
            has_http_error = any("HTTP/Network error" in str(e) for e in errors)
            if has_http_error:
                error_type += " [HTTP/Network failure]"

            print(
                f"  - {pipeline.get('pipeline_name', 'Unknown')}{error_type}: {', '.join(errors)}"
            )

    if summary["was_interrupted"]:
        print("\n⚠️  Processing was interrupted. Partial results have been saved.")
        if summary.get("total_comparison_interrupted", 0) > 0:
            print(
                f"   Note: {summary['total_comparison_interrupted']} pipeline(s) had file comparisons interrupted"
            )
        print(f"   Results file: {output_file}")


if __name__ == "__main__":
    main()
