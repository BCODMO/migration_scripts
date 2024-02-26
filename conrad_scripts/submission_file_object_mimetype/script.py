import requests
import json
import re
import boto3

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz

utc = pytz.UTC


BUCKET = "bcodmo-submissions"

four_months_ago = utc.localize(datetime.now() - relativedelta(months=+4))
new_content_type = "application/octet-stream"


def generate_version_request_url(t, submission_id, resource_id):
    return f"http://localhost:8080/api/file/versions?{t}Id={submission_id}&type={t}&resourceId={resource_id}"


s3 = boto3.resource("s3")
s3cl = boto3.client("s3")
my_bucket = s3.Bucket(BUCKET)

submission_ids = set()
project_ids = set()

counter = 0
edited_count = 0
for obj in my_bucket.objects.all():
    counter += 1
    if counter % 100 == 0:
        print(f"{counter}... Processed")

    z = re.match("(.+)/files/.+", obj.key)
    if z:
        if obj.last_modified > four_months_ago:
            (oid,) = z.groups()
            try:
                metadata = s3cl.head_object(Bucket=BUCKET, Key=obj.key)
                if not metadata["ContentType"]:
                    print(f"Updating {obj.key}: {obj.last_modified}")
                    obj.copy_from(
                        CopySource={"Bucket": BUCKET, "Key": obj.key},
                        MetadataDirective="REPLACE",
                        ContentType=new_content_type,
                    )
                    edited_count += 1

            except Exception as e:
                raise e

print(f"Num edited: {edited_count}")
