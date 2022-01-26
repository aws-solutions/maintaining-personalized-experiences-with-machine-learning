# ######################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                  #
#                                                                                                                      #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance      #
#  with the License. You may obtain a copy of the License at                                                           #
#                                                                                                                      #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                      #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed    #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for   #
#  the specific language governing permissions and limitations under the License.                                      #
# ######################################################################################################################

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
