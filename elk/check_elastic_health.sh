#!/usr/bin/env bash
set -euo pipefail

echo "=== cluster health ==="
curl -sS http://127.0.0.1:9200/_cluster/health?pretty
echo
echo "=== nodes ==="
curl -sS http://127.0.0.1:9200/_cat/nodes?v
echo
echo "=== homework indices ==="
curl -sS http://127.0.0.1:9200/_cat/indices/python-logstash-homework-*?v
