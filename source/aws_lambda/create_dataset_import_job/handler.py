# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Any

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.utilities.typing import LambdaContext

from shared.sfn_middleware import PersonalizeResource

RESOURCE = "datasetImportJob"
STATUS = "datasetImportJob.status"
CONFIG = {
    "jobName": {
        "source": "event",
        "path": "serviceConfig.jobName",
    },
    "datasetArn": {
        "source": "event",
        "path": "serviceConfig.datasetArn",
    },
    "dataSource": {
        "source": "event",
        "path": "serviceConfig.dataSource",
    },
    "roleArn": {"source": "environment", "path": "ROLE_ARN"},
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
    "importMode": {"source": "event", "path": "serviceConfig.importMode", "default": "omit"},
    "tags": {
        "source": "event",
        "path": "serviceConfig.tags",
        "default": "omit",
    },
    "publishAttributionMetricsToS3": {
        "source": "event",
        "path": "serviceConfig.publishAttributionMetricsToS3",
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
    """Create a dataset import job in Amazon Personalize based on the configuration in `event`
    :param event: AWS Lambda Event
    :param context: AWS Lambda Context
    :return: the configured dataset import job
    """
    return event.get("resource")  # return the dataset import job
