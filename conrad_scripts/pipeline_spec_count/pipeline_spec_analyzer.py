#!/usr/bin/env python3

import boto3
import yaml
import json
from datetime import datetime
from typing import List, Dict, Any


def main():
    # Initialize S3 client
    s3_client = boto3.client("s3")
    bucket_name = "laminar-dump"

    # Results tracking
    total_files = 0
    pipeline_data = []

    try:
        # List all objects in the bucket
        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name)

        for page in page_iterator:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                key = obj["Key"]

                # Check if the file is named pipeline-spec.yaml
                if key.endswith("pipeline-spec.yaml"):
                    total_files += 1
                    print(f"Processing file {total_files}: {key}")

                    # Get file metadata
                    file_creation_date = obj["LastModified"].isoformat()

                    try:
                        # Download and parse the YAML file
                        response = s3_client.get_object(Bucket=bucket_name, Key=key)
                        yaml_content = response["Body"].read().decode("utf-8")

                        # Parse YAML
                        parsed_yaml = yaml.safe_load(yaml_content)

                        # Count pipeline steps - pipeline is nested under unknown top-level key
                        pipeline_steps = 0
                        if isinstance(parsed_yaml, dict):
                            # Look through all top-level keys to find the one containing "pipeline"
                            for top_key, top_value in parsed_yaml.items():
                                if isinstance(top_value, dict) and "pipeline" in top_value:
                                    pipeline_list = top_value["pipeline"]
                                    if isinstance(pipeline_list, list):
                                        pipeline_steps = len(pipeline_list)
                                    break
                        # Store the data
                        pipeline_info = {
                            "path": key,
                            "creation_date": file_creation_date,
                            "pipeline_steps": pipeline_steps,
                        }
                        pipeline_data.append(pipeline_info)

                    except Exception as e:
                        print(f"Error processing {key}: {str(e)}")
                        # Still add the file to results but with 0 steps
                        pipeline_info = {
                            "path": key,
                            "creation_date": file_creation_date,
                            "pipeline_steps": 0,
                            "error": str(e),
                        }
                        pipeline_data.append(pipeline_info)

        # Sort pipeline_data by creation_date for ordered list
        pipeline_data.sort(key=lambda x: x["creation_date"])

        # Prepare final results
        results = {
            "total_pipeline_spec_files": total_files,
            "pipeline_files": pipeline_data,
            "analysis_timestamp": datetime.now().isoformat(),
        }

        # Save results to JSON file
        output_filename = "pipeline_spec_analysis.json"
        with open(output_filename, "w") as f:
            json.dump(results, f, indent=2)

        print(f"\nAnalysis complete!")
        print(f"Total pipeline-spec.yaml files found: {total_files}")
        print(f"Results saved to: {output_filename}")

        # Print summary statistics
        if pipeline_data:
            step_counts = [
                item["pipeline_steps"] for item in pipeline_data if "error" not in item
            ]
            if step_counts:
                avg_steps = sum(step_counts) / len(step_counts)
                max_steps = max(step_counts)
                min_steps = min(step_counts)
                print(
                    f"Pipeline steps - Min: {min_steps}, Max: {max_steps}, Average: {avg_steps:.2f}"
                )

    except Exception as e:
        print(f"Error accessing S3 bucket: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
