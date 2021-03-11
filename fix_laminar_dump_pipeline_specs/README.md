I just discovered an issue where pipeline-specs in laminar dump don't have the "from" key in their pipeline-spec for the load step.
They are also missing the bucket_name and prefix key from dump_to_s3

The issue was popping values off of the object before the object was converted to a yaml

This script should try to find the missing "from" and flag pipelines for which it couldn't find the fix

UPDATE:
- it turns out this script is not necessary, as the SOP has been to download all of the files from the UI
(where the bug didn't exist) and upload them to the data server. Therefore all of the pipeline-specs
on the dataserver should have the proper keys.
