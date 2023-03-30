import json
import dateutil.parser
import datetime
import boto3
import re
import os


def put_dps(
    s3_endpoint, s3_access_key, s3_secret_key, s3_bucket, ddb_endpoint, ddb_table
):
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
    allDps = []
    p = re.compile("^[a-zA-z0-9]*/datapackage.json$")
    while isTruncated:
        if continuationToken:
            response = s3_client.list_objects_v2(
                Bucket=bucket, ContinuationToken=continuationToken,
            )
        else:
            response = s3_client.list_objects_v2(Bucket=bucket,)
        isTruncated = response["IsTruncated"]
        continuationToken = response.get("ContinuationToken", None)

        for obj in response["Contents"]:
            key = obj["Key"]
            if p.match(key):
                allDps.append(key)

    toBeAdded = []
    for key in allDps:
        response = s3_client.get_object(Bucket=bucket, Key=key,)
        dp = json.load(response["Body"])

        deleted = dp.get("bcodmo:", {}).get("deleted", False)
        if not deleted:
            state = dp.get("bcodmo:", {}).get("state", "")
            updated = dp.get("updated", "")
            if not updated:
                raise Exception("UPDATED NOT FOUND", key)

            updatedDate = dateutil.parser.isoparse(updated)
            toBeAdded.append(
                {
                    "objectId": key.strip("/datapackage.json"),
                    "updated": updatedDate.timestamp(),
                    "state": state,
                }
            )

    ddb = boto3.client("dynamodb", endpoint_url=ddb_endpoint)

    for obj in toBeAdded:
        print("Putting", obj)
        response = ddb.put_item(
            TableName=ddb_table,
            Item={
                "ObjectId": {"S": obj["objectId"],},
                "Updated": {"N": str(obj["updated"]),},
                "State": {"S": obj["state"],},
            },
        )
    print("Scanning...")

    response = ddb.scan(TableName=ddb_table,)
    for item in response["Items"]:
        print(item)


# Submissions
put_dps(
    os.environ.get("MINIO_ENDPOINT"),
    os.environ.get("MINIO_ACCESS_KEY"),
    os.environ.get("MINIO_SECRET_KEY"),
    os.environ.get("MINIO_SUBMISSIONS_BUCKET"),
    os.environ.get("DDB_ENDPOINT"),
    os.environ.get("DDB_TABLE_SUBMISSIONS"),
)
# Projects
put_dps(
    os.environ.get("MINIO_ENDPOINT"),
    os.environ.get("MINIO_ACCESS_KEY"),
    os.environ.get("MINIO_SECRET_KEY"),
    os.environ.get("MINIO_PROJECTS_BUCKET"),
    os.environ.get("DDB_ENDPOINT"),
    os.environ.get("DDB_TABLE_PROJECTS"),
)
