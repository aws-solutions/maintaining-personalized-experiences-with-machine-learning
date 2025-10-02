# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Any

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.utilities.typing import LambdaContext

from shared.sfn_middleware import PersonalizeResource

RESOURCE = "solution"
STATUS = "solution.status"
CONFIG = {
    "name": {
        "source": "event",
        "path": "serviceConfig.name",
    },
    "performHPO": {
        "source": "event",
        "path": "serviceConfig.performHPO",
        "default": "omit",
    },
    "recipeArn": {
        "source": "event",
        "path": "serviceConfig.recipeArn",
        "default": "omit",
    },
    "datasetGroupArn": {
        "source": "event",
        "path": "serviceConfig.datasetGroupArn",
    },
    "eventType": {
        "source": "event",
        "path": "serviceConfig.eventType",
        "default": "omit",
    },
    "solutionConfig": {
        "source": "event",
        "path": "serviceConfig.solutionConfig",
        "default": "omit",
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
    """Create a solution in Amazon Personalize based on the configuration in `event`
    :param event: AWS Lambda Event
    :param context: AWS Lambda Context
    :return: the configured solution version
    """
    return event.get("resource")  # return the solution
