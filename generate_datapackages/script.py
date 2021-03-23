import logging

for name in ["boto", "urllib3", "s3transfer", "boto3", "botocore", "nose", "requests"]:
    logging.getLogger(name).setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)
import boto3

boto3.set_stream_logger("", logging.CRITICAL)


import json
import pandas as pd
import csv
import requests
import io
import yaml
import time
import os
import re
import difflib
from dataflows import Flow
from dataflows.base.exceptions import ProcessorError
from datapackage_pipelines.lib import update_resource, update_package
from bcodmo_frictionless.bcodmo_pipeline_processors import (
    load,
    update_fields,
    add_schema_metadata,
    dump_to_s3,
)

s3 = boto3.client("s3")

dataset_ids = ["3300", "2292", "2291"]
# dataset_ids = ["2295"]
dataset_ids = ["2472"]

BUCKET_NAME = "conrad-migration-test"
LAMINAR_DUMP_BUCKET = "laminar-dump"
DATASETS_FILENAME = "datasets.csv"
# the result of a sparql query getting all of the species columns
SPECIES_FILENAME = "species.json"
# the result of a sparql query getting all of the lat lon columns
LATLON_FILENAME = "latlon.json"
# the result of a big "find" command that finds all pipeline-spec names in data302/data305
# find /data30* | grep pipeline-spec.yaml
PIPELINE_SPECS_FILENAME = "pipelines.txt"

# Whether the dump to s3 step should be used
ADD_DUMP = False
# Whether the list of dataset_ids should be used instead of all datasets
FILTER = False


def extract_dataset_id(url):
    return str(re.sub(".*\/(\d*)$", r"\1", url))


# Parse the csv file containing a list of datasets
with open(DATASETS_FILENAME, "r") as csv_file:
    reader = csv.reader(csv_file)
    # Ignore the header
    next(reader)
    datasets = [dataset for dataset in reader]


# Get the species info
with open(SPECIES_FILENAME, "r") as json_file:
    js = json.load(json_file)
    species_list = [
        {k: s[k]["value"] for k in s.keys()} for s in js["results"]["bindings"]
    ]

with open(LATLON_FILENAME, "r") as json_file:
    js = json.load(json_file)
    latlon_list = [
        {k: s[k]["value"] for k in s.keys()} for s in js["results"]["bindings"]
    ]
    latlon_dict = {extract_dataset_id(s["dataset"]): s for s in latlon_list}

with open(PIPELINE_SPECS_FILENAME, "r") as fp:
    pipeline_specs_list = []
    for line in fp:
        line = line.strip("\n\r")
        if not line.endswith("pipeline-spec.yaml"):
            continue
        pipeline_specs_list.append(line)


def generate_data_url(dataset_id):
    url = f"https://www.bco-dmo.org/dataset/{dataset_id}/data/download"
    return url


def download_data(url):
    return pd.read_csv(url, sep="\t", comment="#", error_bad_lines=False)


def find_pipeline_spec_match(dataset_id, dataset_version):
    matches = []
    for path in pipeline_specs_list:
        search_string = f"/{dataset_id}/{dataset_version}/data/pipeline-spec.yaml"
        if search_string in path and "/working/" not in path and "/work/" not in path:
            matches.append(path)

    assert len(matches) <= 1

    if len(matches) == 1:
        return matches[0]
    return None


def get_latlon_fields(dataset_id):
    if dataset_id in latlon_dict:
        return (
            latlon_dict[dataset_id]["lat_column"],
            latlon_dict[dataset_id]["lon_column"],
        )
    return None, None


def get_species_fields(dataset_id):
    return_list = []
    for species in species_list:
        new_dataset_id = extract_dataset_id(species["dataset"])
        if dataset_id == new_dataset_id:
            return_list.append(species["species_column"])

    return return_list


def get_unique_species(df, species):
    return_list = []
    for s in species:
        assert s in df
        return_list.append(df[s].unique().tolist())

    return return_list


def _get_pipeline_spec(
    title,
    description,
    dataset_id,
    dataset_version,
    version,
    steps,
):
    yaml_string = yaml.dump(
        {
            title: {
                "title": title,
                "description": description,
                "datasetId": dataset_id,
                "datasetVersion": dataset_version,
                "version": version,
                "pipeline": steps,
            }
        },
        sort_keys=False,
    )
    return yaml_string


processor_to_func = {
    # bcodmo processors
    "bcodmo_pipeline_processors.load": load,
    "bcodmo_pipeline_processors.update_fields": update_fields,
    "bcodmo_pipeline_processors.add_schema_metadata": add_schema_metadata,
    "bcodmo_pipeline_processors.dump_to_s3": dump_to_s3,
    "update_resource": update_resource.flow,
    "update_package": update_package.flow,
}

counter = 0

completed = []

false_versioned = []
repeated = []
found_pipeline = []
failed_inference = []
failed_found_pipeline = []
s3_and_local_different = []


def move_already_existing_pipeline(
    path, title, dataset_id, dataset_version, species, unique_species, lat, lon
):
    dp_path = path.replace("pipeline-spec.yaml", "datapackage.json")
    try:
        with open(dp_path, "r") as dp_fp:
            dp = json.load(dp_fp)
    except IOError:
        print("DP doesn't exist", dp_path)
        return False

    if len(dp["resources"]) != 1:
        print("More than one resource")
        return False

    # Here we confirm that the files are the same on the server as on s3
    with open(path, "r") as pipeline_spec_file:
        if "bcodmo_pipeline_processors.dump_to_path" not in "".join(
            pipeline_spec_file.readlines()
        ):
            res_path = dp["resources"][0]["path"]
            data_path = path.replace("pipeline-spec.yaml", res_path)
            print("dataPath", data_path)
            f = io.BytesIO()
            object_key = f"{dataset_id}/{dataset_version}/data/{res_path}"
            print("objectKey", object_key)
            s3.download_fileobj(LAMINAR_DUMP_BUCKET, object_key, f)
            with open(data_path, "r") as local_f:
                diff = difflib.ndiff(f.readlines(), local_f.readlines())
                print("RESULT OF DIFF", diff)
        else:
            print("Skipping the diff because this file wasn't dumped with dump_to_s3")

    # add unique species to dp
    if len(species):
        for i, s in enumerate(species):
            for field in dp["resources"][0]["schema"]["fields"]:
                if field["name"] == s:
                    if "bcodmo:" not in field:
                        field["bcodmo:"] = {}
                    field["bcodmo:"]["unique"] = unique_species[i]

    if lat and lon:
        resource = dp["resources"][0]
        if "bcodmo:" not in resource:
            resource["bcodmo:"] = {}
        resource["bcodmo:"]["lat_column"] = lat
        resource["bcodmo:"]["lon_column"] = lon
    dp["version"] = dataset_version
    dp["id"] = dataset_id

    print(dp)
    print()
    print()
    print()

    # TODO save dp, pipeline-spec, and data

    return True


def generate_and_run_pipeline(
    title, dataset_id, dataset_version, species, unique_species, lat, lon, retry=False
):
    try:
        """
        Create the pipeline, starting with load
        """
        steps = [
            {
                "run": "bcodmo_pipeline_processors.load",
                "parameters": {
                    "from": url,
                    # TODO add filename/clear name as resource name
                    "name": "res1",
                    "format": "csv",
                    "skip_rows": ["#"],
                    "delimiter": "\t",
                    "infer_strategy": "strings" if retry else "full",
                    "cast_strategy": "strings" if retry else "schema",
                    "override_schema": {
                        "missingValues": ["", "nd"],
                    },
                },
            }
        ]

        # Add the unique species list to each species column
        processor_fields = {}
        for i, col_name in enumerate(species):
            processor_fields[col_name] = {
                "bcodmo:": {
                    "unique": unique_species[i],
                }
            }
        if len(processor_fields.keys()):
            steps.append(
                {
                    "run": "bcodmo_pipeline_processors.update_fields",
                    "parameters": {"resources": ["res1"], "fields": processor_fields},
                }
            )

        # Add lat lon to the base of the resource
        if lat and lon:
            steps.append(
                {
                    "run": "update_resource",
                    "parameters": {
                        "resources": ["res1"],
                        "metadata": {"bcodmo:": {"lat_column": lat, "lon_column": lon}},
                    },
                }
            )

        # Add dataset_id and dataset_version into the datapackage
        steps.append(
            {
                "run": "update_package",
                "parameters": {
                    "version": dataset_version,
                    "id": dataset_id,
                },
            }
        )

        # TODO add dataset_id to identifier (look up in spec), and dataset_version to version
        if ADD_DUMP:
            steps.append(
                {
                    "run": "bcodmo_pipeline_processors.dump_to_s3",
                    "parameters": {
                        "prefix": f"{dataset_id}/{dataset_version}/data",
                        "force-format": True,
                        "format": "csv",
                        "save_pipeline_spec": True,
                        "temporal_format_property": "outputFormat",
                        "bucket_name": BUCKET_NAME,
                        # TODO- ask adam if empty data manager is necessary?
                        "data_manager": {
                            "name": "",
                            "orcid": "",
                            "submission_id": "",
                        },
                    },
                }
            )

            # Generate the pipeline spec to get added into the dump to s3 step
            pipeline_spec_str = _get_pipeline_spec(
                title, "", dataset_id, dataset_version, "v2.10.0", steps
            )

            steps[-1]["parameters"]["pipeline_spec"] = pipeline_spec_str

        flow_params = []
        for step in steps:
            processor = processor_to_func[step["run"]]

            flow_params.append(processor(step["parameters"]))

        r = Flow(
            *flow_params,
        ).process()
        return r, retry

    except ProcessorError as e:
        if not retry:
            return generate_and_run_pipeline(
                title,
                dataset_id,
                dataset_version,
                species,
                unique_species,
                lat,
                lon,
                retry=True,
            )
        raise e


for dataset in datasets:

    if FILTER and dataset[0] not in dataset_ids:
        continue

    if counter > 0 and counter % 50 == 0:
        print(f"Completed {counter} datasets of {len(datasets)}...")
    counter += 1

    """
    Set up the initial variables
    """
    dataset_id = dataset[0]
    if dataset_id in completed:
        repeated.append(dataset_id)
        continue
    print(f"Looking at {dataset_id}")

    dataset_version = dataset[1]
    try:
        int(dataset_version)
    except:
        dataset_version = "1"
        false_versioned.append(dataset_id)

    matched_pipeline_spec = find_pipeline_spec_match(dataset_id, dataset_version)
    if matched_pipeline_spec:
        found_pipeline.append({"dataset_id": dataset_id, "path": matched_pipeline_spec})

    url_type = dataset[2]
    title = dataset[4]

    url = generate_data_url(dataset_id)
    if url_type != "Primary":
        url = dataset[3]

    """
    Make the sparql queries to get lat_lon and species
    """
    lat, lon = get_latlon_fields(dataset_id)
    species = get_species_fields(dataset_id)
    unique_species = []
    # TODO REMOVE MATCHED PIPELINE SPEC BOOL
    if len(species) and matched_pipeline_spec:
        if dataset_id in ["2472"]:
            # We skip this species for now
            species = []
        else:
            df = download_data(url)
            unique_species = get_unique_species(df, species)

    generate_pipeline = not matched_pipeline_spec
    if not generate_pipeline:
        success = move_already_existing_pipeline(
            matched_pipeline_spec,
            title,
            dataset_id,
            dataset_version,
            species,
            unique_species,
            lat,
            lon,
        )
        if not success:
            failed_found_pipeline.append(
                {"dataset_id": dataset_id, "path": matched_pipeline_spec}
            )
            generate_pipeline = True

    if generate_pipeline:
        continue
        r, inference_failed = generate_and_run_pipeline(
            title,
            dataset_id,
            dataset_version,
            species,
            unique_species,
            lat,
            lon,
        )

        if inference_failed:
            print("Inference failed")
            failed_inference.append(dataset_id)

        print(r[0].descriptor)
        print()
        print()

    completed.append(dataset_id)

    # TODO
    """"
    - check if a dataset with a pipeline-spec & datapackage exists in laminar-dump/whoi server
    - do a find for all pipeline-spec, filter that later to see if there is a pipeline-spec.yaml
    - in embargo, if dataset_id has been seen before, ignore it
    - create a dump_to_s3 step
    """

print(
    f"""
Done!

{len(found_pipeline)} found pipeline-specs
{len(failed_found_pipeline)} failed found pipeline-specs
{len(failed_inference)} failed inference
{len(false_versioned)} false versions
{len(repeated)} repeated
"""
)


with open("output.json", "w") as fp:
    json.dump(
        {
            "found_pipeline": found_pipeline,
            "failed_found_pipeline": failed_found_pipeline,
            "failed_inference": failed_inference,
            "false_versioned": false_versioned,
            "repeated": repeated,
        },
        fp,
    )
