# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Any

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.utilities.typing import LambdaContext

from shared.sfn_middleware import PersonalizeResource

RESOURCE = "schema"
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
    "schema": {"source": "event", "path": "serviceConfig.schema", "as": "string"},
}
logger = Logger()
tracer = Tracer()
metrics = Metrics()


@metrics.log_metrics
@tracer.capture_lambda_handler
@PersonalizeResource(
    resource=RESOURCE,
    config=CONFIG,
)
def lambda_handler(event: Dict[str, Any], context: LambdaContext) -> Dict:
    """Create a schema in Amazon Personalize based on the configuration in `event`
    :param event: AWS Lambda Event
    :param context: AWS Lambda Context
    :return: the configured schema
    """
    return event.get("resource")  # return the resource
