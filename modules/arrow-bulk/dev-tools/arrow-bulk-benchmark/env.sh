AUTH="elastic-admin:elastic-password"
ES=localhost:9200
create() { # name batch_indexing
curl -s -u $AUTH -XDELETE "$ES/$1" >/dev/null
curl -s -u $AUTH -XPUT "$ES/$1" -H 'Content-Type: application/json' -d "{
 \"settings\":{\"index.number_of_shards\":1,\"index.number_of_replicas\":0,\"index.mode\":\"${3:-standard}\",\"index.batch_indexing\":$2,\"index.refresh_interval\":\"-1\"},
 \"mappings\":{\"dynamic\":\"strict\",\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"host\":{\"type\":\"keyword\"},\"level\":{\"type\":\"keyword\"},\"service\":{\"type\":\"keyword\"},\"region\":{\"type\":\"keyword\"},\"message\":{\"type\":\"keyword\"},\"status\":{\"type\":\"integer\"},\"bytes\":{\"type\":\"long\"},\"duration_ms\":{\"type\":\"double\"},\"user_id\":{\"type\":\"long\"},\"success\":{\"type\":\"boolean\"},\"bucket\":{\"type\":\"integer\"}}}}"
echo
}
arrow() { curl -s -u $AUTH -o /tmp/arrow_resp.bin -w "%{http_code} %{time_total}s\n" -XPOST "$ES/_arrow/$1/_bulk" -H 'Content-Type: application/vnd.apache.arrow.stream' --data-binary @data.arrow; }
json()  { curl -s -u $AUTH -o /tmp/json_resp.json -w "%{http_code} %{time_total}s\n" -XPOST "$ES/$1/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @data.ndjson; }
count() { curl -s -u $AUTH "$ES/$1/_count" ; echo; }
