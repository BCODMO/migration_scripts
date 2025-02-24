import requests
import json
import re
import boto3
from decimal import Decimal
from dateutil.tz import tzutc

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz

utc = pytz.UTC

DDB_ENDPOINT = "https://dynamodb.us-east-1.amazonaws.com"

# BUCKET = "bcodmo-submissions-staging"
# BUCKET = "bcodmo-projects-staging"
# DDB_TABLE = "submission-object-history-staging"

BUCKET = "bcodmo-submissions"
# BUCKET = "bcodmo-projects"
DDB_TABLE = "submission-object-history-prod"

s3 = boto3.resource("s3")
s3cl = boto3.client("s3")
my_bucket = s3.Bucket(BUCKET)
ddb = boto3.client("dynamodb", endpoint_url=DDB_ENDPOINT)
ddb_table = boto3.resource("dynamodb").Table(DDB_TABLE)


counter = 0
updated = 0
rows_added = 0
res = s3cl.list_objects_v2(Bucket=BUCKET)
while len(res["Contents"]):
    token = res.get("NextContinuationToken", None)
    for obj in res["Contents"]:
        key = obj["Key"]
        counter += 1
        if counter % 100 == 0:
            print(f"{counter}... Processed")

        z = re.match("([a-zA-Z0-9]*)/datapackage.json", key)
        if z:
            (oid,) = z.groups()
            dp_key = f"{oid}/datapackage.json"
            res = s3cl.list_object_versions(
                Bucket=BUCKET,
                Prefix=dp_key,
            )
            total_versions = 0
            versions = []
            while True:
                versions += res["Versions"]
                if not "NextVersionIdMarker" in res:
                    break
                version_id_marker = res["NextVersionIdMarker"]
                key_marker = res["NextKeyMarker"]
                res = s3cl.list_object_versions(
                    Bucket=BUCKET,
                    Prefix=dp_key,
                    VersionIdMarker=version_id_marker,
                    KeyMarker=key_marker,
                )

            previous_version_id = ""
            prev_seconds = None
            prev_updated_seconds = None
            num_same = 0
            with ddb_table.batch_writer() as batch:
                for v in reversed(versions):
                    rows_added += 1
                    modified = v["LastModified"]
                    version_id = v["VersionId"]
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
                            "Updated": Decimal(updated_seconds),
                            "Orcid": "unknown",
                            "VersionIdAfter": version_id,
                            "VersionIdBefore": previous_version_id,
                        },
                    )
                    """
                        Item={
                            "ObjectId": {
                                "S": oid,
                            },
                            "Updated": {
                                "N": str(updated_seconds),
                            },
                            "Orcid": {
                                "S": "unknown",
                            },
                            "VersionIdAfter": {
                                "S": version_id,
                            },
                            "VersionIdBefore": {
                                "S": previous_version_id,
                            },
                        },
                    )
                    """
                    previous_version_id = version_id

            print(oid, len(versions))
            updated += 1
    if not token:
        break
    res = s3cl.list_objects_v2(Bucket=BUCKET, ContinuationToken=token)
print(f"Total datapackages found: {updated}")
print(f"Total rows added: {rows_added}")
print(f"Total files processed {counter}")
