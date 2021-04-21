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

from script import get_latlon_fields, get_species_fields

s3 = boto3.client("s3")

datasets_prefix = "_jgofs_1"
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
SKIP_DATASETS = ["555780"]

with open("output.json") as fp:
    output_json = json.load(fp)

for dataset_id in output_json["failed_dump"]:
    if dataset_id in output_json["failed_second_dump"]:
        continue
    print(dataset_id)
