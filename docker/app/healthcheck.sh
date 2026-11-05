#!/bin/sh

curl -f "http://localhost:8081/healthcheck" || exit 1
