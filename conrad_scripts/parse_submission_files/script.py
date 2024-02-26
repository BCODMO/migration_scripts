import requests
import json
import re
import boto3

BUCKET = "bcodmo-submissions"


def generate_version_request_url(t, submission_id, resource_id):
    return f"http://localhost:8080/api/file/versions?{t}Id={submission_id}&type={t}&resourceId={resource_id}"


s3 = boto3.resource("s3")
s3cl = boto3.client("s3")
my_bucket = s3.Bucket(BUCKET)

submission_ids = set()
project_ids = set()

counter = 0
for obj in my_bucket.objects.all():
    counter += 1
    if counter % 100 == 0:
        print(f"{counter}... Processed")

    z = re.match("(.+)/files/.+", obj.key)
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

        """
        response = s3cl.list_object_versions(
            Bucket=BUCKET,
            Prefix=obj.key,
        )
        obj_versions = response["Versions"]

        print(len(obj_versions))

        if len(obj_versions) > 1:
        """
        if "Powerwater" in dp["title"]:
            print("FOUND: ", d["title"])
            if is_submission:
                submission_ids.add(oid)
            else:
                project_ids.add(oid)


print("SUBMISSION_IDS")
print(submission_ids)
print()
print()
print()
print("PROJECT")
print(project_ids)
