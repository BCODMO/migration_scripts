import json
import random
from fnvhash import fnv1a_32
import base64
import zlib
import dateutil.parser
import datetime
import boto3
import re
import os


s3_endpoint = os.environ.get("MINIO_ENDPOINT")
s3_access_key = os.environ.get("MINIO_ACCESS_KEY")
s3_secret_key = os.environ.get("MINIO_SECRET_KEY")
s3_bucket = os.environ.get("LAMINAR_HISTORY_BUCKET")
ddb_endpoint = os.environ.get("DDB_ENDPOINT")
ddb_table = os.environ.get("DDB_TABLE")
session = boto3.session.Session()

s3_client = session.client(
    service_name="s3",
    aws_access_key_id=s3_access_key,
    aws_secret_access_key=s3_secret_key,
    endpoint_url=s3_endpoint,
)

bucket = s3_bucket
isTruncated = True
continuationToken = None
allObjects = []


def decode_string(s):
    decoded_bytes = base64.urlsafe_b64decode(s)
    decompressed_bytes = zlib.decompress(decoded_bytes)
    decoded_str = str(decompressed_bytes, "utf-8")
    return decoded_str


while isTruncated:
    if continuationToken:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            ContinuationToken=continuationToken,
        )
    else:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
        )
    isTruncated = response["IsTruncated"]
    continuationToken = response.get("NextContinuationToken", None)

    for obj in response["Contents"]:
        key = obj["Key"]
        match = re.findall(
            "([a-z0-9_-]*)/(.*)",
            key,
            re.IGNORECASE,
        )
        assert match
        orcid, hashed_title = match[0]
        title = decode_string(hashed_title)
        updated = obj["LastModified"]
        response = s3_client.get_object(
            Bucket=bucket,
            Key=key,
        )
        o = json.load(response["Body"])
        allObjects.append(
            {
                "orcid": orcid,
                "title": title,
                "updated": str(int(updated.timestamp()))
                + "."
                + str(round(random.random() * 10000)),
                "o": json.dumps(o),
            }
        )
        if len(allObjects) % 50 == 0:
            print(f"Completed {len(allObjects)}")

print(len(allObjects))

ddb = boto3.client(
    "dynamodb",
    endpoint_url=ddb_endpoint,
    aws_access_key_id=s3_access_key,
    aws_secret_access_key=s3_secret_key,
)

counter = 0
for obj in allObjects:

    response = ddb.put_item(
        TableName=ddb_table,
        Item={
            "Orcid": {
                "S": obj["orcid"],
            },
            "Updated": {
                "N": str(obj["updated"]),
            },
            "Title": {
                "S": obj["title"],
            },
            "Pipeline": {
                "S": obj["o"],
            },
        },
    )
    counter += 1
    if counter % 50 == 0:
        print(f"Put {counter} objects")
"""
print("Scanning...")

response = ddb.scan(
    TableName=ddb_table,
    ProjectionExpression="Orcid",
)
for item in response["Items"]:
    print(item)


"""
# TODO MIGRATE TO submission_ids instead of submission_id
