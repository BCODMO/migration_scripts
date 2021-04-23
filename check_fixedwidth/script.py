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

# the result of a big "find" command that finds all pipeline-spec names in data302/data305
# find /data30* | grep pipeline-spec.yaml
PIPELINE_SPECS_FILENAME = "pipelines.txt"


with open(PIPELINE_SPECS_FILENAME, "r") as fp:
    pipeline_specs_list = []
    for line in fp:
        line = line.strip("\n\r")
        if not line.endswith("pipeline-spec.yaml"):
            continue
        pipeline_specs_list.append(line)

final_list = []
counter = 0
for pipeline_path in pipeline_specs_list:
    if counter % 50:
        print(f"Completed {counter} of {len(pipeline_specs_list)}")
    try:
        with open(pipeline_path, "r") as pipeline_spec_file:
            pipeline_str = "".join(pipeline_spec_file.readlines())
            if (
                "format: bcodmo-fixedwidth" in pipeline_str
                and "infer: true" in pipeline_str
            ):
                final_list.append(pipeline_path)
    except:
        print("Skipping", pipeline_path)
    counter += 1

with open("dump.json", "rw") as fp:
    json.dump(fp, final_list)
