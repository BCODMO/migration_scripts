import boto3
import re
import json
import numpy as np

BUCKET = "bcodmo-submissions"


s3 = boto3.resource("s3")
client = boto3.client("s3")
my_bucket = s3.Bucket(BUCKET)

counter = 0
deleted = []
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
                SELECT s['name']
                FROM s3object AS s
                WHERE s['bcodmo:'].deleted is not null
                AND s['bcodmo:'].deleted = true
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
            result["oid"] = oid
            deleted.append(result)

for d in deleted:
    print(f"Submission {d['oid']} with name {d['name']} was deleted.")
