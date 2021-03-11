import boto3
import botocore
import re
import yaml
import zlib
import json
import base64

BUCKET = "laminar-dump"
HISTORY_BUCKET = "laminar-history"
FAILED_FILENAME = "missing.json"
FIXED_FILENAME = "fixed.json"

s3 = boto3.resource("s3")
my_bucket = s3.Bucket(BUCKET)
counter = 0
matched = 0
pipeline_specs = []
versions = {}
for obj in my_bucket.objects.all():
    if len(pipeline_specs) > 1000:
        break
    counter += 1
    if counter % 1000 == 0:
        pass
        # print(f"{counter}... Matched {matched}")

    z = re.match("(.+)/(.+)/data/pipeline-spec.yaml", obj.key)
    if z:
        dataset_id, dataset_version = z.groups()
        try:
            dataset_id = int(dataset_id)
            cur_version = int(dataset_version)
        except:
            # Poorly formatted objects, we will ignore
            continue
        # Handle multiple versions of a given dataset
        if dataset_id in versions:
            prev_version, index = versions[dataset_id]
            if prev_version < cur_version:
                pipeline_specs[index] = obj.key
                versions[dataset_id] = (
                    cur_version,
                    index,
                )
        else:
            versions[dataset_id] = (
                cur_version,
                matched,
            )
            pipeline_specs.append(obj.key)
            matched += 1

# Sanity check
dataset_ids = [
    re.match("(.+)/.+/data/pipeline-spec.yaml", k).groups()[0] for k in pipeline_specs
]
assert len(dataset_ids) == len(set(dataset_ids))

print(
    f"Iterated through {counter} objects and gathered {matched} pipeline-spec.yaml keys"
)


def _get_pipeline_spec(
    name,
    title,
    description,
    dataset_id,
    dataset_version,
    redmine_issue_number,
    version,
    submissionId,
    steps,
):
    yaml_string = yaml.dump(
        {
            name: {
                "title": title,
                "description": description,
                "datasetId": dataset_id,
                "datasetVersion": dataset_version,
                "redmineIssueNumber": redmine_issue_number,
                "version": version,
                "submissionId": submissionId,
                "pipeline": steps,
            }
        },
        sort_keys=False,
    )
    return yaml_string


def encode_string(s):
    """ Encode a string that is url/path not safe to a base64 string """
    compressed_bytes = zlib.compress(s.encode("utf-8"), 9)
    encoded_bytes = base64.urlsafe_b64encode(compressed_bytes)
    encoded_str = str(encoded_bytes, "utf-8")
    return encoded_str


def get_orcid(pipline_spec_key):
    obj = s3.Object(
        bucket_name=BUCKET,
        key=pipline_spec_key.replace("pipeline-spec.yaml", "datapackage.json",),
    )
    response = obj.get()
    dp = json.loads(response["Body"].read().decode())
    orcid = dp.get("bcodmo:", {}).get("dataManager", {}).get("orcid", None)
    return orcid


def get_history_version(title, orcid, steps):
    # Use the history bucket to find the pipeline that matches this pipeline
    key = f"{orcid}/{encode_string(title)}"

    try:
        obj = s3.Object(bucket_name=HISTORY_BUCKET, key=key,)
        response = obj.get()
        history = json.loads(response["Body"].read().decode())

        history_steps = history["pipeline"]["steps"]
        if len(steps) != len(history_steps):
            print(f"DIFFERNET LENGTH: {len(steps)} and {len(history_steps)}", title)
            return None
        for i, step in enumerate(steps):
            # print(i, step)
            if (
                step["run"] == "bcodmo_pipeline_processors.load"
                and step.get("parameters", {}).get("from", None) is not None
            ):
                print("ACTUALLY HAS FROM", title)
                # This pipeline-spec.yaml actually has from, we return False instead of None
                return False
            if history_steps[i]["run"] != step["run"]:
                print("DOESN'T MATCH", title)
                return None
        return history_steps

    except s3.meta.client.exceptions.NoSuchKey as e:
        print("no history found", title)
        return None
    return None


failed_pipelines = []
success_pipelines = []
all_steps = []
counter = 0
for key in pipeline_specs:
    counter += 1
    if counter % 25 == 0:
        print(f"Parsed {counter} pipeline-specs")

    obj = s3.Object(bucket_name=BUCKET, key=key,)
    response = obj.get()
    pipeline_str = response["Body"].read().decode()
    yaml_obj = yaml.load(pipeline_str, Loader=yaml.FullLoader)
    orcid = get_orcid(key)
    title = list(yaml_obj.keys())[0]
    steps = yaml_obj[title]["pipeline"]

    history_steps = get_history_version(title, orcid, steps)

    if history_steps is None:
        failed_pipelines.append({"path": key, "title": title})
        continue
    elif history_steps is False:
        # False return value means we don't need to fix the from step
        pass
    else:
        success_pipelines.append({"path": key, "title": title})

    step_objects = []
    for i, step in enumerate(steps):
        step_name = step["run"]
        if step_name == "bcodmo_pipeline_processors.load":
            _from = history_steps[i].get("parameters", {}).get("from", None)
            if not _from:
                print("HISTORY ALSO NO FROM")
            else:
                step["parameters"]["from"] = _from
        if step_name == "bcodmo_pipeline_processors.dump_to_s3":
            bucket_name = (
                history_steps[i].get("parameters", {}).get("bucket_name", None)
            )
            prefix = history_steps[i].get("parameters", {}).get("prefix", None)
            step["parameters"]["bucket_name"] = bucket_name
            step["parameters"]["prefix"] = prefix

    new_pipeline_spec = _get_pipeline_spec(
        title,
        title,
        yaml_obj[title].get("description", ""),
        yaml_obj[title].get("datasetId", ""),
        yaml_obj[title].get("datasetVersion", ""),
        yaml_obj[title].get("redmineIssueNumber", ""),
        yaml_obj[title].get("version", ""),
        yaml_obj[title].get("submissionId", ""),
        steps,
    )


print(
    f"Dumping {len(failed_pipelines)} pipelines to missing.json and {len(success_pipelines)} fixed to fixed.json"
)
with open(FAILED_FILENAME, "w") as fp:
    json.dump(failed_pipelines, fp)

with open(FIXED_FILENAME, "w") as fp:
    json.dump(success_pipelines, fp)
