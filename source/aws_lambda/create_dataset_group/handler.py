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

from aws_lambda_powertools import Tracer, Logger, Metrics
from aws_lambda_powertools.utilities.typing import LambdaContext

from shared.sfn_middleware import PersonalizeResource

tracer = Tracer()
logger = Logger()
metrics = Metrics()


@metrics.log_metrics
@tracer.capture_lambda_handler
@PersonalizeResource(
    resource="datasetGroup",
    status="datasetGroup.status",
    config={
        "name": {
            "source": "event",
            "path": "serviceConfig.name",
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
    },
)
def lambda_handler(event: Dict[str, Any], context: LambdaContext) -> Dict:
    """Create a dataset group in Amazon Personalize based on the configuration in `event`
    :param event: AWS Lambda Event
    :param context: AWS Lambda Context
    :return: the configured dataset group
    """
    return event.get("resource")  # return the dataset group
