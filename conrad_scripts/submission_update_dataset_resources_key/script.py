import requests
import json
import re
import boto3

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz

utc = pytz.UTC


BUCKET = "bcodmo-submissions-staging"
BUCKET = "bcodmo-submissions"

s3 = boto3.resource("s3")
s3cl = boto3.client("s3")
my_bucket = s3.Bucket(BUCKET)


counter = 0
updated = 0
no_datasets = 0
already_updated = 0
res = s3cl.list_objects_v2(Bucket=BUCKET)
while len(res["Contents"]):
    token = res.get("NextContinuationToken", None)
    for obj in res["Contents"]:
        key = obj["Key"]
        counter += 1
        if counter % 100 == 0:
            print(f"{counter}... Processed")

        z = re.match("(.+)/datapackage.json", key)
        if z:
            (oid,) = z.groups()
            # if oid == "lOoKk3ZmLwNi2GgO":
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
            update = False
            datasets = dp.get("bcodmo:", {}).get("datasets", [])
            if datasets and len(datasets):
                _continue = False
                for dataset in datasets:
                    if "supplementalFiles" in dataset:
                        already_updated += 1
                        _continue = True
                        break

                if _continue:
                    continue

                update = True
                for dataset in datasets:
                    resources = dataset.get("resources", [])
                    primaryDataFiles = dataset.get("primaryDataFiles", [])
                    if resources is None:
                        resources = []
                    if primaryDataFiles is None:
                        primaryDataFiles = []

                    supplementalFiles = []
                    for resourceId in resources:
                        if resourceId not in primaryDataFiles:
                            supplementalFiles.append(resourceId)

                    if "resources" in dataset:
                        del dataset["resources"]
                    dataset["supplementalFiles"] = supplementalFiles
            else:
                no_datasets += 1

            if update:
                updated += 1
                r = s3cl.put_object(
                    Body=json.dumps(dp).encode("utf-8"), Bucket=BUCKET, Key=dp_key
                )
                # if len(dp["resources"]) > 1:
                #    exit()
                #    pass
    if not token:
        break
    res = s3cl.list_objects_v2(Bucket=BUCKET, ContinuationToken=token)
print(f"Total updated: {updated}")
print(f"Total already updated: {already_updated}")
print(f"Total no datasets: {no_datasets}")
print(f"Total files processed {counter}")
