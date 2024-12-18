import boto3
from datetime import datetime
import re
import json
import numpy as np

BUCKET = "bcodmo-submissions"


s3 = boto3.resource("s3")
client = boto3.client("s3")
my_bucket = s3.Bucket(BUCKET)

counter = 0
fixed = []
not_fixed = []
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
                SELECT s['bcodmo:'].metadata.related_publications_methods_references,
                    s['bcodmo:'].metadata.related_publications_related_datasets,
                    s['bcodmo:'].metadata.related_publications_results_publications
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
            if result:
                dp_str = (
                    client.get_object(Bucket=BUCKET, Key=dp_key)["Body"]
                    .read()
                    .decode("utf-8")
                )
                dp = json.loads(dp_str)
                metadata = dp["bcodmo:"]["metadata"]

                if "related_publications" not in metadata:
                    metadata["related_publications"] = {}
                    if "related_publications_methods_references" in metadata:
                        val = metadata["related_publications_methods_references"]
                        del metadata["related_publications_methods_references"]
                        metadata["related_publications"]["methods_references"] = val
                    if "related_publications_related_datasets" in metadata:
                        val = metadata["related_publications_related_datasets"]
                        del metadata["related_publications_related_datasets"]
                        metadata["related_publications"]["related_datasets"] = val
                    if "related_publications_results_publications" in metadata:
                        val = metadata["related_publications_results_publications"]
                        del metadata["related_publications_results_publications"]
                        metadata["related_publications"]["results_publications"] = val

                else:
                    if "related_publications_methods_references" in metadata:
                        del metadata["related_publications_methods_references"]
                    if "related_publications_related_datasets" in metadata:
                        del metadata["related_publications_related_datasets"]
                    if "related_publications_results_publications" in metadata:
                        del metadata["related_publications_results_publications"]

                dp["bcodmo:"]["metadata"] = metadata
                r = client.put_object(
                    Body=json.dumps(dp).encode("utf-8"), Bucket=BUCKET, Key=dp_key
                )
                fixed.append(oid)
for oid in fixed:
    print(f"Fixed submission {oid}.")
