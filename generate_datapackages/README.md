The following paths were removed from the result of the find command:

`sudo find /data30* -path /data305/.snapshot -prune -o -path /data302/.snapshot -prune -o -name "pipeline-spec.yaml" > pipelines.txt`
```
/data302/kim_kiho/Guam_Reefs/639865/1/data/pipeline-spec.yaml
/data302/TestProject/770442/1/data/pipeline-spec.yaml
/data302/Fish_Derived_Coral_Nutrients2/770442/1/data/pipeline-spec.yaml
/data305/pipeline_results/770442/1/data/pipeline-spec.yaml

```


Dataset IDs to note:

- Dataset 2472 has non utf-8 encoded character in it, so we ignore it for the purpose of unique stuff (it won't load into pandas)
- Dataset 2321 has non utf-8 encoded character, skip ENTIRELY for now because it won't dump



