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
results = [["person", "orcid", "num_submissions"]]
peopleDict = {}
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
        people = dp["bcodmo:"].get("collaborators", [])
        if is_submission and people:
            for person in people:
                if person.get("submitter", False):
                    name = (
                        person.get("firstName", "") + " " + person.get("lastName", "")
                    )
                    if name not in peopleDict:
                        peopleDict[name] = {
                            "name": name,
                            "orcid": person.get("orcid", ""),
                            "num_submissions": 1,
                        }
                    else:
                        peopleDict[name]["num_submissions"] += 1

for name in peopleDict.keys():
    results.append(
        [name, peopleDict[name]["orcid"], peopleDict[name]["num_submissions"]]
    )

with open("results.csv", "w") as f:

    # using csv.writer method from CSV package
    write = csv.writer(f)

    write.writerows(results)
