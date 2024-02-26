import requests
import json
import re
import boto3

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz

utc = pytz.UTC


BUCKET = "bcodmo-submissions"

s3 = boto3.resource("s3")
s3cl = boto3.client("s3")
my_bucket = s3.Bucket(BUCKET)


counter = 0
for obj in my_bucket.objects.all():
    counter += 1
    if counter % 100 == 0:
        print(f"{counter}... Processed")

    z = re.match("(.+)/datapackage.json", obj.key)
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
            path = resource.get("path", "")
            z = re.match("(https:\/\/s3.amazonaws.com)\/minio\/(.+)", path)
            if z:
                update = True
                (before, after) = z.groups()
                new_path = f"{before}/{after}"

                print(
                    f"Switching :::{path}::: to :::{new_path}::: for object :::{oid}:::"
                )
                resource["path"] = new_path
        if update:
            r = s3cl.put_object(
                Body=json.dumps(dp).encode("utf-8"), Bucket=BUCKET, Key=dp_key
            )
            # if len(dp["resources"]) > 1:
            #    exit()
            #    pass
