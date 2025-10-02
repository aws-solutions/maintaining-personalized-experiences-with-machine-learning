# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Any

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.utilities.typing import LambdaContext

from shared.sfn_middleware import PersonalizeResource

RESOURCE = "campaign"
STATUS = "campaign.latestCampaignUpdate.status || campaign.status"
CONFIG = {
    "name": {
        "source": "event",
        "path": "serviceConfig.name",
    },
    "solutionVersionArn": {
        "source": "event",
        "path": "serviceConfig.solutionVersionArn",
    },
    "minProvisionedTPS": {
        "source": "event",
        "path": "serviceConfig.minProvisionedTPS",
        "as": "int",
    },
    "campaignConfig": {
        "source": "event",
        "path": "serviceConfig.campaignConfig",
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
    config=CONFIG,
    status=STATUS,
)
def lambda_handler(event: Dict[str, Any], context: LambdaContext) -> Dict:
    """Create a campaign in Amazon Personalize based on the configuration in `event`
    :param event: AWS Lambda Event
    :param context: AWS Lambda Context
    :return: the configured dataset
    """
    return event.get("resource")  # return the campaign
