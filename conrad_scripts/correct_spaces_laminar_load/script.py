import boto3
import botocore
import json

BUCKET = "laminar-load"

s3 = boto3.resource("s3")
my_bucket = s3.Bucket(BUCKET)
counter = 0
to_be_corrected = []
for obj in my_bucket.objects.all():
    counter += 1
    if counter % 1000 == 0:
        print(f"{counter}...")

    if "%20" in obj.key:
        to_be_corrected.append(obj)

print(
    f"Done moving through the whole bucket ({counter} objects, {len(to_be_corrected)} picked)"
)

before_keys = []
after_keys = []
counter = 0
for obj in to_be_corrected:
    counter += 1

    if counter % 100 == 0:
        print(f"{counter}...")
    before_keys.append(obj.key)
    new_name = obj.key.replace("%20", " ")
    after_keys.append(new_name)
    try:
        s3.Object(BUCKET, new_name).load()
    except botocore.exceptions.ClientError as e:
        # creating
        s3.Object(BUCKET, new_name).copy_from(CopySource=f"{BUCKET}/{obj.key}")
    else:
        print("The new name already exists", new_name)

print(len(to_be_corrected), "corrected")

# print(to_be_corrected)

out_file = open("before_keys.json", "w")
json.dump(before_keys, out_file, indent=6)
out_file.close()

out_file = open("after_keys.json", "w")
json.dump(after_keys, out_file, indent=6)
out_file.close()
