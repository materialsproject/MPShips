#!/usr/bin/env python

import logging
import os

import boto3
import requests
from mp_ui.core.logging import configure_logging
from supervisor.options import ClientOptions
from supervisor.supervisorctl import Controller

configure_logging()
# runs as __main__; use an mp_ui.* name so it gets the app log level
logger = logging.getLogger("mp_ui.devops.main")


def start(program):
    args = ["start", program]
    options = ClientOptions()
    options.realize(args)
    c = Controller(options)
    c.onecmd(" ".join(options.args))


start("web")

metadata_uri = os.environ.get("METADATA_URI", "")
start_rq = True

if metadata_uri:
    # in AWS Fargate with potentially multiple tasks
    logger.debug("ECS metadata URI", extra={"metadata_uri": metadata_uri})
    r = requests.get(metadata_uri)
    labels = r.json()["Labels"]
    prefix = "com.amazonaws.ecs"
    cluster = labels[f"{prefix}.cluster"].split("/", 1)[1]
    family = labels[f"{prefix}.task-definition-family"]
    version = int(labels[f"{prefix}.task-definition-version"])
    client = boto3.client("ecs")
    task_arns = client.list_tasks(cluster=cluster, family=family).get("taskArns", [])
    tasks = client.describe_tasks(cluster=cluster, tasks=task_arns).get("tasks", [])
    ntasks = 0

    for task in tasks:
        v = int(task["taskDefinitionArn"].rsplit(":", 1)[1])
        ntasks += int(v == version)

    start_rq = ntasks == 1  # this task included in metadata response
    logger.info(
        "Decided whether to start the RQ worker",
        extra={
            "ecs_tasks_current": ntasks,
            "ecs_tasks": len(tasks),
            "start_rq": start_rq,
        },
    )

# if start_rq:
#     start("worker")
