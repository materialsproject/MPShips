#!/bin/bash

set -e
PMGRC="$HOME/.pmgrc.yaml"
[[ ! -e "$PMGRC" ]] && echo "PMG_DUMMY_VAR: dummy" >"$PMGRC"

STATS_ARG=""
SERVER_APP="${PACKAGE_NAME}.wsgi:application"
BIND_ARG="-b 0.0.0.0:8081"
WORKER_ARGS="-k gevent -w $NUM_WORKERS"
# Logging (incl. the access log as structured JSON) is configured in gunicorn.conf.py
CONFIG_ARGS="-c gunicorn.conf.py $RELOAD"
# Recycle workers after MAX_REQUESTS (+ up to MAX_REQUESTS_JITTER) requests:
# bounds memory growth and refreshes per-worker caches (e.g. DeltaTable snapshots)
REQS_ARGS="--max-requests ${MAX_REQUESTS:-0} --max-requests-jitter ${MAX_REQUESTS_JITTER:-0}"
# --graceful-timeout: let in-flight requests of a recycled worker finish
OTHER_ARGS="--name $SUPERVISOR_PROCESS_NAME --timeout 120 --graceful-timeout 120"
MAIN_ARGS="$BIND_ARG $WORKER_ARGS $CONFIG_ARGS $REQS_ARGS $OTHER_ARGS"

set -x

if [[ -n "$DD_TRACE_HOST" ]]; then
  wait-for-it.sh "$DD_TRACE_HOST" -q -s -t 10 && STATS_ARG="--statsd-host $DD_AGENT_HOST:8125" || echo "WARNING: datadog agent unreachable"
fi

[[ -n "$STATS_ARG" ]] && CMD="ddtrace-run gunicorn $STATS_ARG" || CMD="gunicorn"
exec $CMD $MAIN_ARGS $SERVER_APP
