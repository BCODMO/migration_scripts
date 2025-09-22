import itertools
import json
import requests
import re
import logging
import os
import matplotlib.pyplot as plt


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create formatters and add it to handlers
log_format = logging.Formatter(
    "%(asctime)s - [%(threadName)s] - %(levelname)s - %(message)s"
)

# Console handler (stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(log_format)

# File handler
file_handler = logging.FileHandler("parameter_counts.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(log_format)

# Add handlers to the logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)


def load_file(filename="../actual_buggy_pipelines.json"):
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {filename}: {e}")
        raise


def get_key(pipeline):
    return f"{pipeline['pipeline_title']}-{pipeline['last_updated']}"


js = load_file()
results_pipelines = []
results_occurances = []
pipeline_dict = {}
for pipeline in js["all_results"]:
    pipeline_dict[get_key(pipeline)] = pipeline

_parameters = load_file(filename="../parameters/parameters.json")
filtered = load_file(filename="../filter/filtered.json")

q_flag_pipelines = {}
for orcid in filtered.keys():
    combined = itertools.chain(
        filtered[orcid]["in_dataset_pipelines"],
        filtered[orcid]["not_in_dataset_pipelines"],
        filtered[orcid]["inferred_submission_id_not_in_dataset_pipelines"],
        filtered[orcid]["no_submission_id"],
    )
    for p in combined:

        pipeline_key = get_key(p)
        pipeline = pipeline_dict[pipeline_key]
        if pipeline["original_prefix"] not in _parameters:
            logger.info(
                "Skipping because missing parameters (likely not a real dataset)"
            )
            continue
        parameters_dict = _parameters[pipeline["original_prefix"]]
        if "comparison" not in pipeline:
            logger.info("Skipping because no comparison (error when running)")
            continue
        for diff in pipeline["comparison"]["detailed_differences"]:
            num_occurances = {}
            num_pipelines = {}
            for cell in diff["cell_differences"]:
                column = cell["column"]
                param = f"NOT FOUND"
                # print(parameters_dict.keys())
                if column in parameters_dict:
                    param = parameters_dict[column]
                if param not in num_occurances:
                    num_occurances[param] = 0
                if param == "q_flag":
                    if pipeline_key not in q_flag_pipelines:
                        q_flag_pipelines[pipeline_key] = set()
                    q_flag_pipelines[pipeline_key].add(column)

                num_pipelines[param] = 1
                num_occurances[param] += 1
            logger.info(f"Found info {num_occurances}")
            results_occurances.append(num_occurances)
            results_pipelines.append(num_pipelines)
for k in q_flag_pipelines.keys():
    q_flag_pipelines[k] = list(q_flag_pipelines[k])
print(json.dumps(q_flag_pipelines, sort_keys=True, indent=4))


with open("parameter_counts.json", "w") as f:
    json.dump(
        {"occurances": results_occurances, "pipelines": results_pipelines},
        f,
        indent=2,
        default=str,
    )

logger.info(f"Results saved to parameter_counts.json")


all_parameters_occurances = {}
for pipeline_parameters in results_occurances:
    for parameter in pipeline_parameters.keys():
        if parameter not in all_parameters_occurances:
            all_parameters_occurances[parameter] = 0
        all_parameters_occurances[parameter] += pipeline_parameters[parameter]

with open("parameter_occurances.json", "w") as f:
    json.dump(all_parameters_occurances, f, indent=2, default=str)

all_parameters = {}
for pipeline_parameters in results_pipelines:
    for parameter in pipeline_parameters.keys():
        if parameter not in all_parameters:
            all_parameters[parameter] = 0
        all_parameters[parameter] += pipeline_parameters[parameter]


# Create the bar plot
plt.figure(figsize=(32, 24))
sorted_pairs = sorted(zip(all_parameters.values(), all_parameters.keys()))
sorted_parameters_num, sorted_parameters = zip(*sorted_pairs)
plt.barh(
    sorted_parameters,
    sorted_parameters_num,
    height=0.5,
    color="skyblue",
    edgecolor="navy",
    alpha=0.7,
)

# Add labels and title
plt.xlabel("Parameter")
plt.ylabel("# Pipeline")
plt.title("# Pipelines that contain a parameter that has an error")
plt.tick_params(axis="y", labelsize=6)

# Show the plot
plt.tight_layout()
plt.show()
