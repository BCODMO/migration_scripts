# exit on error
set -eo pipefail

SUBMISSION_BUCKET_SOURCE=bcodmo-submissions
SUBMISSION_BUCKET_TARGET=bcodmo-submissions-staging

PROJECT_BUCKET_SOURCE=bcodmo-projects
PROJECT_BUCKET_TARGET=bcodmo-projects-staging

# tables
TABLE_FROM=submission-prod
TABLE_TO=submission-staging

echo "Moving all recent objects from "$TABLE_FROM" to "$TABLE_TO"!"
read -p "Are you sure? " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then

    currentTime=$(date -u +%s)
    days10=864000
    daysAgo="$(($currentTime - $days10))"
    # read
    aws dynamodb scan \
      --table-name "$TABLE_FROM" \
      --filter-expression "#v >= :num" \
      --expression-attribute-names '{"#v": "Updated"}' \
      --expression-attribute-values "{\":num\": {\"N\":\"$daysAgo\"}}" \
      --output json \
     | jq "[ .Items[] | { PutRequest: { Item: . } } ]" \
     > "$TABLE_FROM-dump.json"


    table_size="$(cat "${TABLE_FROM}-dump.json" | jq '. | length')"
    echo "table size: ${table_size}"

    # write in batches of 25
    for i in $(seq 0 25 $table_size); do
      j=$(( i + 25 ))
      cat "${TABLE_FROM}-dump.json" | jq -c '{ "'$TABLE_TO'": .['$i':'$j'] }' > "${TABLE_TO}-batch-payload.json"
      echo "Loading records $i through $j (up to $table_size) into ${TABLE_TO}"
      aws dynamodb batch-write-item --request-items file://"${TABLE_TO}-batch-payload.json"
      rm "${TABLE_TO}-batch-payload.json"
    done

    echo "Removing all objects from \"$SUBMISSION_BUCKET_TARGET\"!"
    read -p "Are you sure? " -n 1 -r
    echo    # (optional) move to a new line
    if [[ $REPLY =~ ^[Yy]$ ]]
    then
        aws s3 rm --recursive s3://$SUBMISSION_BUCKET_TARGET


        echo "Copying recent objects from \"$SUBMISSION_BUCKET_SOURCE\" to \"$SUBMISSION_BUCKET_TARGET\"."
        read -p "Are you sure? " -n 1 -r
        echo    # (optional) move to a new line
        if [[ $REPLY =~ ^[Yy]$ ]]
        then
            # TABLE FROM might need to be manual
            includeString=$(cat $TABLE_FROM-dump.json | python3 -c "import sys, json; print(' '.join(['--include \"' + o['PutRequest']['Item']['ObjectId']['S'] + '/*\"' for o in json.load(sys.stdin)]))")
            c="aws s3 sync s3://$SUBMISSION_BUCKET_SOURCE s3://$SUBMISSION_BUCKET_TARGET --exclude \"*\" $includeString"
            eval "$c"
        fi
    fi

    echo "Removing all objects from \"$PROJECT_BUCKET_TARGET\"!"
    read -p "Are you sure? " -n 1 -r
    echo    # (optional) move to a new line
    if [[ $REPLY =~ ^[Yy]$ ]]
    then
        aws s3 rm --recursive s3://$PROJECT_BUCKET_TARGET


        echo "Copying recent objects from \"$PROJECT_BUCKET_SOURCE\" to \"$PROJECT_BUCKET_TARGET\"."
        read -p "Are you sure? " -n 1 -r
        echo    # (optional) move to a new line
        if [[ $REPLY =~ ^[Yy]$ ]]
        then
            # TABLE FROM might need to be manual
            includeString=$(cat $TABLE_FROM-dump.json | python3 -c "import sys, json; print(' '.join(['--include \"' + o['PutRequest']['Item']['ObjectId']['S'] + '/*\"' for o in json.load(sys.stdin)]))")
            c="aws s3 sync s3://$PROJECT_BUCKET_SOURCE s3://$PROJECT_BUCKET_TARGET --exclude \"*\" $includeString"
            eval "$c"
        fi
    fi

    rm "${TABLE_FROM}-dump.json"
fi
