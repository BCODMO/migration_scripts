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
resources_counter = 0
updated = 0
updated_resources = 0
no_category = 0
already_updated = 0
res = s3cl.list_objects_v2(Bucket=BUCKET)
updated_oids = []
while len(res["Contents"]):
    token = res.get("NextContinuationToken", None)
    for obj in res["Contents"]:
        key = obj["Key"]
        counter += 1
        if counter % 100 == 0:
            print(f"{counter}... Processed")

        z = re.match("([A-za-z0-9]+)/datapackage.json", key)
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
            for resource in dp.get("resources", []):
                resources_counter += 1
                if resource.get("name") == "_bcodmo_no_resources":
                    continue
                if not resource.get("bcodmo:", {}).get("category"):
                    if not "bcodmo:" in resource:
                        print(oid)
                        continue
                    update = True
                    no_category += 1

                    resource["bcodmo:"]["category"] = "submitter"

            if update:
                updated_oids.append(oid)
                updated += 1
                r = s3cl.put_object(
                    Body=json.dumps(dp).encode("utf-8"), Bucket=BUCKET, Key=dp_key
                )
                # exit()
                # if len(dp["resources"]) > 1:
                #    exit()
                #    pass
    if not token:
        break
    res = s3cl.list_objects_v2(Bucket=BUCKET, ContinuationToken=token)
print(updated_oids)
print(f"Total updated: {updated}")
print(f"Total already updated: {already_updated}")
print(f"Total no category: {no_category}")
print(f"Total resources processed {resources_counter}")
print(f"Total files processed {counter}")
