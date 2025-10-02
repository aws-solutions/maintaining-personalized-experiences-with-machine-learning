# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Any

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.utilities.typing import LambdaContext

from shared.sfn_middleware import PersonalizeResource

RESOURCE = "batchInferenceJob"
STATUS = "batchInferenceJob.status"
CONFIG = {
    "jobName": {
        "source": "event",
        "path": "serviceConfig.jobName",
    },
    "solutionVersionArn": {
        "source": "event",
        "path": "serviceConfig.solutionVersionArn",
    },
    "filterArn": {
        "source": "event",
        "path": "serviceConfig.filterArn",
        "default": "omit",
    },
    "numResults": {
        "source": "event",
        "path": "serviceConfig.numResults",
        "default": "omit",
    },
    "jobInput": {
        "source": "event",
        "path": "serviceConfig.jobInput",
    },
    "jobOutput": {"source": "event", "path": "serviceConfig.jobOutput"},
    "roleArn": {"source": "environment", "path": "ROLE_ARN"},
    "batchInferenceJobConfig": {
        "source": "event",
        "path": "serviceConfig.batchInferenceJobConfig",
        "default": "omit",
    },
    "maxAge": {
        "source": "event",
        "path": "workflowConfig.maxAge",
        "default": "omit",
        "as": "seconds",
    },
    "timeStarted": {
        "source": "event",
        "path": "workflowConfig.timeStarted",
        "default": "omit",
        "as": "iso8601",
    },
    "tags": {
        "source": "event",
        "path": "serviceConfig.tags",
        "default": "omit",
    },
}

logger = Logger()
tracer = Tracer()
metrics = Metrics()


@metrics.log_metrics
@tracer.capture_lambda_handler
@PersonalizeResource(
    resource=RESOURCE,
    status=STATUS,
    config=CONFIG,
)
def lambda_handler(event: Dict[str, Any], context: LambdaContext) -> Dict:
    """Create a batch inference job in Amazon Personalize based on the configuration in `event`
    :param event: AWS Lambda Event
    :param context: AWS Lambda Context
    :return: the configured batch inference job
    """
    return event.get("resource")  # return the batch inference job
