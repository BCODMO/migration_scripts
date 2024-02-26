import boto3
import json

client = boto3.client("lambda")

payload = {
    "cache_id": "123",
    "steps": [
        {
            "run": "bcodmo_pipeline_processors.load",
            "parameters": {
                "format": "csv",
                "from": "https://www.dropbox.com/s/rsfoyl01w8vuepu/BCODMO_Formatted.csv?dl=1",
                "name": "res1",
                "limit_rows": 500000,
            },
        },
    ],
    "metadata": {"title": "hello"},
    "verbose": True,
    "num_rows": -1,
}
response = client.invoke(
    FunctionName="laminar-pipeline",
    InvocationType="RequestResponse",
    LogType="Tail",
    Payload=json.dumps(payload).encode(),
)
print(json.loads(response["Payload"].read()))
