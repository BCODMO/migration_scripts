import boto3
from datetime import datetime
import re
import json
import numpy as np

BUCKET = "bcodmo-submissions"


s3 = boto3.resource("s3")
client = boto3.client("s3")
my_bucket = s3.Bucket(BUCKET)
target_date = datetime(2024, 3, 4)
date_floor = datetime(2024, 2, 26)
date_ceil = datetime(2024, 3, 11)

counter = 0
results = []
for obj in my_bucket.objects.all():
    counter += 1
    if counter % 500 == 0:
        print(f"{counter}... Processed")
    z = re.match("(.+)/datapackage.json", obj.key)
    if z:
        (oid,) = z.groups()
        dp_key = f"{oid}/datapackage.json"
        try:
            resp = client.select_object_content(
                Bucket=BUCKET,  # Put your own bucket name here.
                Key=dp_key,  # Put your own key name here.
                Expression="""
                SELECT s['bcodmo:'].metadata.dataset_name, s['created']
                FROM s3object AS s
                WHERE s.created is not null
                """,
                ExpressionType="SQL",
                InputSerialization={
                    "JSON": {
                        "Type": "DOCUMENT",
                    },
                },
                OutputSerialization={
                    "JSON": {},
                },
            )
        except Exception as e:
            print(f"Error on {dp_key}: {str(e)}")
            continue
        result = None
        r = ""
        for event in resp["Payload"]:
            if "Records" in event:
                r += event["Records"]["Payload"].decode("utf-8")
        if r:
            r = r.replace("\\n", ",").strip(",")
            result = json.loads(f"[{r}]")
            result = result[0]
            created = datetime.strptime(result["created"], "%Y-%m-%dT%H:%M:%SZ")
            if created > date_floor and created < date_ceil:
                result["created"] = created
                result["oid"] = oid
                results.append(result)

for r in results:
    print(
        f"Submission {r['oid']} created within 1 week of {target_date}: {r['dataset_name']}"
    )
