# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Any

from aws_lambda_powertools import Tracer, Logger, Metrics
from aws_lambda_powertools.utilities.typing import LambdaContext

from shared.sfn_middleware import PersonalizeResource

RESOURCE = "datasetGroup"
STATUS = "datasetGroup.status"
CONFIG = {
    "name": {
        "source": "event",
        "path": "serviceConfig.name",
    },
    "domain": {
        "source": "event",
        "path": "serviceConfig.domain",
        "default": "omit",
    },
    "roleArn": {
        "source": "environment",
        "path": "KMS_ROLE_ARN",
        "default": "omit",
    },
    "kmsKeyArn": {
        "source": "environment",
        "path": "KMS_KEY_ARN",
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

tracer = Tracer()
logger = Logger()
metrics = Metrics()


@metrics.log_metrics
@tracer.capture_lambda_handler
@PersonalizeResource(
    resource=RESOURCE,
    status=STATUS,
    config=CONFIG,
)
def lambda_handler(event: Dict[str, Any], context: LambdaContext) -> Dict:
    """Create a dataset group in Amazon Personalize based on the configuration in `event`
    :param event: AWS Lambda Event
    :param context: AWS Lambda Context
    :return: the configured dataset group
    """
    return event.get("resource")  # return the dataset group
