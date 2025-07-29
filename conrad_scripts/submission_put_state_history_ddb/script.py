import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import json
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from dateutil.tz import tzutc
import pytz

DDB_ENDPOINT = "https://dynamodb.us-east-1.amazonaws.com"
BUCKET = "bcodmo-submissions"
OBJECT_TYPE = "submission"
DDB_STATE_TABLE = "submission-state-history-prod"
DDB_HISTORY_TABLE = "submission-object-history-prod"
ddb = boto3.client("dynamodb", endpoint_url=DDB_ENDPOINT)
ddb_table = boto3.resource("dynamodb").Table(DDB_STATE_TABLE)
ddb_history_table = boto3.resource("dynamodb").Table(DDB_HISTORY_TABLE)
s3 = boto3.resource("s3")
bucket = s3.Bucket(BUCKET)
s3_client = boto3.client("s3")


def get_version_id_to_orcid_dict(object_id):
    paginator = ddb.get_paginator("query")
    response_iterator = paginator.paginate(
        TableName=DDB_HISTORY_TABLE,
        KeyConditionExpression="ObjectId = :oid",
        ExpressionAttributeValues={":oid": {"S": object_id}},
    )
    results = {}
    for page in response_iterator:
        for r in page["Items"]:
            results[r["VersionIdAfter"]["S"]] = r["Orcid"]["S"]

    return results


def worker(object_key, version_id):
    response = s3_client.get_object(Bucket=BUCKET, Key=object_key, VersionId=version_id)
    content = json.loads(response["Body"].read().decode("utf-8"))

    current_value = content.get("bcodmo:", {}).get("state", "")
    return current_value


def get_s3_object_version_changes(
    object_id: str,
    object_key: str,
    start_date: datetime,
    end_date: datetime,
) -> List[Dict[str, any]]:
    versions = []
    paginator = s3_client.get_paginator("list_object_versions")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=object_key):
        if "Versions" in page:
            for version in page["Versions"]:
                if version["Key"] == object_key:
                    versions.append(
                        {
                            "VersionId": version["VersionId"],
                            "LastModified": version["LastModified"],
                            "IsLatest": version.get("IsLatest", False),
                        }
                    )

    versions.sort(key=lambda x: x["LastModified"], reverse=True)

    # Find the most recent version before start_date
    most_recent_before_start = None
    for version in versions:
        if version["LastModified"] < start_date:
            most_recent_before_start = version
            break

    # Filter versions within date range
    filtered_versions = [
        v for v in versions if start_date <= v["LastModified"] <= end_date
    ]

    # Include the most recent version before start if it exists
    if most_recent_before_start:
        filtered_versions.append(most_recent_before_start)

    # Sort by date (oldest first) for processing
    filtered_versions.sort(key=lambda x: x["LastModified"])

    print(
        f"Found {len(filtered_versions)} versions for object {object_id}, now downloading datapackages."
    )

    counter = 0
    total = len(filtered_versions)

    results = []
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(worker, object_key, version["VersionId"]): i
            for i, version in enumerate(filtered_versions)
        }
        results = [None] * len(filtered_versions)

        for future in as_completed(futures):
            counter += 1
            if total > 50 and counter % 50 == 0 and counter != 0:
                print(f"Downloaded {counter} of {total} datapackages...")
            index = futures[future]
            results[index] = future.result()

    # Track changes
    changes = []
    previous_value = None

    print("Finished downloading datapackages, now getting the DDB history.")
    version_id_to_orcid = get_version_id_to_orcid_dict(object_id)
    for version, current_value in zip(filtered_versions, results):
        try:
            counter += 1
            version_id = version["VersionId"]

            orcid = (
                version_id_to_orcid[version_id]
                if version_id in version_id_to_orcid
                else "unknown"
            )

            # Check if value changed
            if previous_value is None:
                # First version we're looking at
                if version == most_recent_before_start:
                    # This is the baseline version before our date range
                    previous_value = current_value
                else:
                    # This is the first version in our date range
                    changes.append(
                        {
                            "date": version["LastModified"].isoformat(),
                            "state": current_value,
                            "version_id": version_id,
                            "orcid": orcid,
                        }
                    )
                    previous_value = current_value
            elif current_value != previous_value:
                # Value changed
                changes.append(
                    {
                        "date": version["LastModified"],
                        "state": current_value,
                        "version_id": version_id,
                        "orcid": orcid,
                    }
                )
                previous_value = current_value

        except Exception as e:
            print(f"Error processing version {version['VersionId']}: {str(e)}")
            continue

    return changes


if __name__ == "__main__":
    key = "j2oMBkogGZgFGgmE/datapackage.json"
    start = datetime(2025, 6, 1, tzinfo=pytz.UTC)
    end = datetime(2035, 1, 1, tzinfo=pytz.UTC)
    rows_added = 0
    counter = 0
    updated = 0
    for obj in bucket.objects.all():
        counter += 1
        if counter % 500 == 0:
            print(f"{counter}... Processed")
        z = re.match("(.+)/datapackage.json", obj.key)
        if z:
            (oid,) = z.groups()

            last_modified = obj.last_modified.replace(tzinfo=pytz.utc)
            if last_modified > start:
                updated += 1
                print(f"Handling object {oid}")
                key = f"{oid}/datapackage.json"
                changes = get_s3_object_version_changes(
                    object_id=oid,
                    object_key=key,
                    start_date=start,
                    end_date=end,
                )
                print(
                    f"Found {len(changes)} changes for object {oid}, putting them into DDB."
                )
                prev_seconds = None
                prev_updated_seconds = None
                num_same = 0
                with ddb_table.batch_writer() as batch:
                    for v in changes:
                        rows_added += 1
                        modified = (
                            datetime.fromisoformat(v["date"])
                            if type(v["date"]) == str
                            else v["date"]
                        )
                        version_id = v["version_id"]
                        state = v["state"]
                        orcid = v["orcid"]

                        seconds = (
                            modified - datetime(1970, 1, 1, tzinfo=tzutc())
                        ).total_seconds()
                        updated_seconds = seconds
                        # We ensure that values with the same key (updated and objectId) will have slightly different keys, based on order
                        if prev_seconds == seconds:
                            num_same += 1
                            updated_seconds += num_same / 1000
                        else:
                            num_same = 0
                        assert (
                            not prev_updated_seconds
                            or prev_updated_seconds < updated_seconds
                        )

                        prev_seconds = seconds
                        prev_updated_seconds = updated_seconds

                        response = batch.put_item(
                            Item={
                                "ObjectId": oid,
                                "State": state,
                                "Updated": Decimal(updated_seconds),
                                "ObjectType": OBJECT_TYPE,
                                "VersionId": version_id,
                                "Orcid": orcid,
                            },
                        )
                print(f"Success!\n---------------\n")

    print(f"Total datapackages found: {updated}")
    print(f"Total rows added: {rows_added}")
    print(f"Total files processed {counter}")
