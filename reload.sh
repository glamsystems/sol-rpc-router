#!/usr/bin/env bash
set -euo pipefail

pid=$(pgrep -x sol-rpc-router)
if [ -z "$pid" ]; then
  echo "sol-rpc-router is not running"
  exit 1
fi

kill -HUP "$pid"
echo "Sent SIGHUP to sol-rpc-router (pid $pid)"
