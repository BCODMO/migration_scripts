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
        is_submission = "bcodmo:" in dp and "submissionId" in dp["bcodmo:"]
        if is_submission:
            publications = (
                dp.get("bcodmo:", {})
                .get("metadata", {})
                .get("related_publications", None)
            )
            if publications is not None:
                methods_references = publications.get("methods_references", None)
                related_datasets = publications.get("related_datasets", None)
                results_publications = publications.get("results_publications", None)
                if (
                    isinstance(methods_references, str)
                    or isinstance(related_datasets, str)
                    or isinstance(results_publications, str)
                ):
                    print("BEFORE", oid, publications)
                    assert isinstance(methods_references, str)
                    assert isinstance(related_datasets, str)
                    assert isinstance(results_publications, str)

                    used = False
                    for key, v in [
                        (
                            "methods_references",
                            methods_references,
                        ),
                        ("related_datasets", related_datasets),
                        ("results_publications", results_publications),
                    ]:
                        if v == "":
                            del publications[key]
                        else:
                            used = True
                            publications[key] = [
                                {
                                    "_migrated": True,
                                    "_found_citation": "",
                                    "identifier": "",
                                    "identifier_type": "none",
                                    "description": v,
                                }
                            ]

                    if not used:
                        del dp["bcodmo:"]["metadata"]["related_publications"]
                    publications = (
                        dp.get("bcodmo:", {})
                        .get("metadata", {})
                        .get("related_publications", None)
                    )
                    print("AFTER", oid, publications)
                    print("____________________________")

                    dp_bytes = json.dumps(dp).encode("utf-8")
                    s3cl.put_object(Body=dp_bytes, Bucket=BUCKET, Key=dp_key)
