import requests
import csv
import json
import re
import boto3

BUCKET = "bcodmo-submissions"


s3 = boto3.resource("s3")
s3cl = boto3.client("s3")
my_bucket = s3.Bucket(BUCKET)


counter = 0
results = [["type", "id", "comments"]]
for obj in my_bucket.objects.all():
    counter += 1
    if counter % 100 == 0:
        print(f"{counter}... Processed")

    z = re.match("(.+)/datapackage.json", obj.key)
    if z:
        (oid,) = z.groups()
        try:
            dp_key = f"{oid}/datapackage.json"
            dp_str = (
                s3cl.get_object(Bucket=BUCKET, Key=dp_key)["Body"]
                .read()
                .decode("utf-8")
            )
        except Exception:
            print("No datapackage")
            continue
        dp = json.loads(dp_str)
        is_submission = "submissionId" in dp["bcodmo:"]
        comments = dp["bcodmo:"].get("metadata", {}).get("comments")

        if comments:
            results.append(["dataset" if is_submission else "project", oid, comments])

with open("results.csv", "w") as f:

    # using csv.writer method from CSV package
    write = csv.writer(f)

    write.writerows(results)
