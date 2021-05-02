import logging

for name in ["boto", "urllib3", "s3transfer", "boto3", "botocore", "nose", "requests"]:
    logging.getLogger(name).setLevel(logging.CRITICAL)
logger = logging.getLogger(__name__)
import boto3

boto3.set_stream_logger("", logging.CRITICAL)


from datapackage import Package, Resource
import hashlib
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
dataset_ids = ["3293", "3292"]
dataset_ids = ["3300", "2292", "2291", "2297", "822549"]
dataset_ids = ["786098"]

datasets_prefix = "_jgofs_2"
# datasets_prefix = "_datasets"

# BUCKET_NAME = "conrad-migration-test"
BUCKET_NAME = "bcodmo.files"
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
ADD_DUMP = True
# Whether the list of dataset_ids should be used instead of all datasets
FILTER = False

# SKIP_DATASETS = ["2321"]

# Skipping 555780 because it is 18GB
SKIP_DATASETS = ["555780", "3747", "3458", "734541"]


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
    url = f"https://www.bco-dmo.org/dataset/{dataset_id}/data/download/tsv"
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
failed_move_pipeline = []
s3_and_local_different = []
s3_and_local_comparison_failed = []
failed_dump = []
failed_second_dump = []
failed_third_dump = []


def move_already_existing_pipeline(
    path, title, dataset_id, dataset_version, species, unique_species, lat, lon
):
    print(path)
    dp_path = path.replace("pipeline-spec.yaml", "datapackage.json")
    move_data = True
    try:
        with open(dp_path, "r") as dp_fp:
            dp = json.load(dp_fp)
    except IOError:
        print("DP doesn't exist", dp_path)
        return False

    if len(dp["resources"]) != 1:
        print("More than one resource")
        return False
    res_name = dp["resources"][0]["name"]
    res_filename = res_name + ".csv"
    data_path = path.replace("pipeline-spec.yaml", res_filename)

    # Here we confirm that the files are the same on the server as on s3
    file_hash = None
    with open(path, "r") as pipeline_spec_file:
        pipeline_str = "".join(pipeline_spec_file.readlines())
        if (
            "bcodmo_pipeline_processors.dump_to_s3" in pipeline_str
            and "datasetId: ''" not in pipeline_str
        ):
            object_key = f"{dataset_id}/{dataset_version}/data/{res_filename}"
            try:
                s3_str = (
                    s3.get_object(Bucket=LAMINAR_DUMP_BUCKET, Key=object_key)["Body"]
                    .read()
                    .decode("utf-8")
                )

                with open(data_path, "r") as local_f:
                    local_str = local_f.read()
                    m = hashlib.md5()
                    m.update(local_str.encode("utf-8"))
                    file_hash = m.hexdigest()
                    if local_str != s3_str:
                        print("NOT THE SAME BETWEEN S3 AND LOCAL")
                        move_data = False
                        s3_and_local_different.append(
                            {
                                "dataset_id": dataset_id,
                                "s3_key": object_key,
                                "path": path,
                            }
                        )
            except Exception as e:
                move_data = False
                s3_and_local_comparison_failed.append(
                    {"dataset_id": dataset_id, "s3_key": object_key, "path": path}
                )

        else:
            print("Skipping the diff because this file wasn't dumped with dump_to_s3")
            move_data = False

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

    print("Moving datapackage and pipeline-spec to s3")
    if move_data:
        dp_file_name = "datapackage.json"
        pipeline_spec_file_name = "pipeline-spec.yaml"
        assert file_hash is not None
        dp["resources"][0]["hash"] = file_hash
        if "bcodmo:" in dp and dp["bcodmo:"].get("submissionId", None) == False:
            dp["bcodmo:"]["submissionId"] = None
    else:
        dp_file_name = "_preserved_datapackage.json"
        pipeline_spec_file_name = "_preserved_pipeline-spec.yaml"
        if file_hash is not None:
            dp["resources"][0]["hash"] = file_hash

    dp_obj_key = f"{datasets_prefix}/{dataset_id}/{dataset_version}/{dp_file_name}"
    pipeline_spec_obj_key = (
        f"{datasets_prefix}/{dataset_id}/{dataset_version}/{pipeline_spec_file_name}"
    )

    # We put the object instead of the file because we've updated the hash in the datapackage to reflect the actual hash of the file
    r = s3.put_object(
        Bucket=BUCKET_NAME,
        Key=dp_obj_key,
        Body=io.BytesIO(
            json.dumps(dp, indent=2, sort_keys=True, ensure_ascii=False).encode()
        ),
    )
    r = s3.upload_file(path, BUCKET_NAME, pipeline_spec_obj_key)

    if move_data:
        data_obj_key = (
            f"{datasets_prefix}/{dataset_id}/{dataset_version}/{res_filename}"
        )
        r = s3.upload_file(data_path, BUCKET_NAME, data_obj_key)
    else:
        return False

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
                    "name": title,
                    "format": "csv",
                    "skip_rows": ["#"],
                    "delimiter": "\t",
                    # We've removed infer for now
                    # "infer_strategy": "strings" if retry else "full",
                    # "cast_strategy": "strings" if retry else "schema",
                    "infer_strategy": "strings",
                    "cast_strategy": "strings",
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
                        "prefix": f"{datasets_prefix}/{dataset_id}/{dataset_version}",
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


if __name__ == "__main__":

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

        if dataset_id in SKIP_DATASETS:
            failed_dump.append(dataset_id)
            failed_second_dump.append(dataset_id)
            failed_third_dump.append(dataset_id)
            continue

        dataset_version = dataset[1]
        try:
            int(dataset_version)
        except:
            dataset_version = "0"
            false_versioned.append(dataset_id)

        print()
        print()
        print()
        print(f"Looking at {dataset_id}")
        url_type = dataset[2]
        title = dataset[4]
        if title.endswith(".tsv"):
            title = title[:-4]

        url = generate_data_url(dataset_id)
        if url_type != "Primary":
            url = dataset[3]

        lat, lon, species, unique_species = (None, None, None, None)
        try:

            matched_pipeline_spec = find_pipeline_spec_match(
                dataset_id, dataset_version
            )
            if matched_pipeline_spec:
                found_pipeline.append(
                    {"dataset_id": dataset_id, "path": matched_pipeline_spec}
                )

            """
            Make the sparql queries to get lat_lon and species
            """
            lat, lon = get_latlon_fields(dataset_id)
            species = get_species_fields(dataset_id)
            unique_species = []
            if len(species):
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
                    failed_move_pipeline.append(
                        {"dataset_id": dataset_id, "path": matched_pipeline_spec}
                    )
                    generate_pipeline = True

            if generate_pipeline:
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

            completed.append(dataset_id)
        except Exception as e:
            print("FAILED. Manufacturing a datapackage and uploading", e)
            try:
                failed_dump.append(dataset_id)

                response = requests.get(generate_data_url(dataset_id))
                bytes_obj = io.BytesIO(response.content)
                bytes_str = bytes_obj.read()
                text_obj = bytes_str.decode("utf-8")
                bytes_str = None
                tab_obj = io.StringIO(text_obj)
                text_obj = None
                tabin = csv.reader(tab_obj, dialect=csv.excel_tab)
                str_obj = io.StringIO()
                commaout = csv.writer(str_obj, dialect=csv.excel)
                fields = None
                for row in tabin:
                    if not fields:
                        fields = [
                            {"format": "default", "type": "string", "name": v}
                            for v in row
                        ]

                    commaout.writerow(row)

                str_obj.seek(0)

                obj = io.BytesIO(str_obj.read().encode("utf-8"))
                bytes_obj.seek(0)

                m = hashlib.md5()
                f = obj.read()
                m.update(f)
                h = m.hexdigest()
                num_bytes = len(f)
                f = None
                obj.seek(0)

                dump_path = f"{datasets_prefix}/{dataset_id}/{dataset_version}"
                object_key = f"{dump_path}/{title}.csv"
                dp_object_key = f"{dump_path}/datapackage.json"
                r = s3.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=obj)

                resource_specific_metadata = {}
                if lat and lon:
                    resource_specific_metadata["lat_column"] = lat
                    resource_specific_metadata["lon_column"] = lon
                if species and len(species) and unique_species and len(unique_species):
                    for field in fields:
                        try:
                            i = species.index(field["name"])
                        except:
                            continue
                        field["bcodmo:"] = {
                            "unique": unique_species[i],
                        }

                dp = Package(
                    descriptor={
                        "bcodmo:": {
                            "dataManager": {
                                "name": "",
                                "orcid": "",
                                "submission_id": "",
                            },
                            "submissionId": None,
                        },
                        "dump_bucket": BUCKET_NAME,
                        "dump_path": dump_path,
                        "id": dataset_id,
                        "resources": [
                            {
                                "path": f"{title}.csv",
                                "profile": "data-resource",
                                "name": title,
                                "mediatype": "text/csv",
                                "hash": h,
                                "encoding": "utf-8",
                                "format": "csv",
                                "bytes": num_bytes,
                                "dialect": {
                                    "delimiter": ",",
                                    "doubleQuote": True,
                                    "lineTerminator": "\r\n",
                                    "quoteChar": '"',
                                    "skipInitialSpace": False,
                                },
                                "bcodmo:": resource_specific_metadata,
                                "schema": {
                                    "fields": fields,
                                    "missingValues": [],
                                },
                            }
                        ],
                    }
                )
                r = s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=dp_object_key,
                    Body=io.BytesIO(
                        json.dumps(
                            dp.descriptor, indent=2, sort_keys=True, ensure_ascii=False
                        ).encode()
                    ),
                )

                #
            except Exception as e:
                print("ALSO FAILED SECOND DUMPING. Dump to .errors", e)
                try:
                    failed_second_dump.append(dataset_id)
                    # Still dump to .errors
                    response = requests.get(generate_data_url(dataset_id))
                    bytes_obj = io.BytesIO(response.content)
                    object_key = f"{datasets_prefix}/.errors/{dataset_id}/{dataset_version}/dataset_{dataset_id}.tsv"
                    r = s3.put_object(
                        Bucket=BUCKET_NAME, Key=object_key, Body=bytes_obj
                    )
                except Exception as e:
                    print("ALSO FAILED THIRD DUMPING. Dump to .errors", e)
                    failed_third_dump.append(dataset_id)

        # TODO
        """"
        - check if a dataset with a pipeline-spec & datapackage exists in laminar-dump/whoi server
        - do a find for all pipeline-spec, filter that later to see if there is a pipeline-spec.yaml
        - in embargo, if dataset_id has been seen before, ignore it
        - create a dump_to_s3 step


        - run without infer types, (later need to implement hash compare)
        -
        """

    print(
        f"""
    Done!

    {len(found_pipeline)} found pipeline-specs
    {len(failed_move_pipeline)} failed found pipeline-specs
    {len(failed_inference)} failed inference
    {len(false_versioned)} false versions
    {len(repeated)} repeated
    {len(s3_and_local_different)} different between s3 and local
    {len(s3_and_local_comparison_failed)} comparisons between s3 and local failed
    {len(failed_dump)} failed dumps
    {len(failed_second_dump)} failed second dumps
    {len(failed_third_dump)} failed third dumps
    """
    )

    with open("output.json", "w") as fp:
        json.dump(
            {
                "found_pipeline": found_pipeline,
                "failed_move_pipeline": failed_move_pipeline,
                "failed_inference": failed_inference,
                "false_versioned": false_versioned,
                "repeated": repeated,
                "s3_and_local_different": s3_and_local_different,
                "s3_and_local_comparison_failed": s3_and_local_comparison_failed,
                "failed_dump": failed_dump,
                "failed_second_dump": failed_second_dump,
                "failed_third_dump": failed_third_dump,
            },
            fp,
        )
