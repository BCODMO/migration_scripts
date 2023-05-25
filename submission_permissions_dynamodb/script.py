import json
from fnvhash import fnv1a_32
import dateutil.parser
import datetime
import boto3
import re
import os


def put_permissions(
    s3_endpoint, s3_access_key, s3_secret_key, s3_bucket, ddb_endpoint, ddb_table
):
    ddb = boto3.client("dynamodb", endpoint_url=ddb_endpoint)
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
    permissions = []
    while isTruncated:
        if continuationToken:
            response = s3_client.list_objects_v2(
                Bucket=bucket, ContinuationToken=continuationToken,
            )
        else:
            response = s3_client.list_objects_v2(Bucket=bucket,)
        isTruncated = response["IsTruncated"]
        continuationToken = response.get("NextContinuationToken", None)

        for obj in response["Contents"]:
            response = s3_client.get_object(Bucket=bucket, Key=obj["Key"],)
            js = json.load(response["Body"])
            if js is not None:
                for item in js:
                    permissions.append(
                        {
                            "orcid": obj["Key"],
                            "object_id": item["id"],
                            "object_type": item["type"],
                            "object_permission": item["permission"],
                        }
                    )
    for p in permissions:
        print("Putting", p)
        response = ddb.put_item(
            TableName=ddb_table,
            Item={
                "Orcid": {"S": p["orcid"],},
                "ObjectId": {"S": p["object_id"],},
                "ObjectType": {"S": p["object_type"],},
                "ObjectPermission": {"S": p["object_permission"],},
            },
        )
    print("Scanning...")

    response = ddb.scan(TableName=ddb_table,)
    for item in response["Items"]:
        print(item)


# Submissions
put_permissions(
    os.environ.get("MINIO_ENDPOINT"),
    os.environ.get("MINIO_ACCESS_KEY"),
    os.environ.get("MINIO_SECRET_KEY"),
    os.environ.get("MINIO_BUCKET"),
    os.environ.get("DDB_ENDPOINT"),
    os.environ.get("DDB_TABLE"),
)
