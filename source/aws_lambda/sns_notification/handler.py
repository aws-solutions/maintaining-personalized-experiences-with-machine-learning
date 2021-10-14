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
import json
import os
from typing import Dict, Optional

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

from aws_solutions.core.helpers import (
    get_service_client,
    get_aws_region,
    get_aws_partition,
)

logger = Logger()
tracer = Tracer()
metrics = Metrics()


UNKNOWN_SOURCE = "UNKNOWN"


def topic_arn() -> str:
    """
    Get the SNS topic ARN from environment variable
    :return: The SNS topic ARN
    """
    return os.environ["SNS_TOPIC_ARN"]


def solution_name() -> str:
    """
    Get the Solution Name from environment variable
    :return: the solution name
    """
    return os.environ["SOLUTION_NAME"]


class MessageBuilder:
    """Builds error messages from AWS Step Functions Output"""

    def __init__(self, event: Dict, context: LambdaContext):
        self.dataset_group = event.get("datasetGroup", UNKNOWN_SOURCE)
        self.states_error = event.get("statesError", None)
        self.service_error = event.get("serviceError", None)
        self.error = event.get("statesError", event.get("serviceError", {}))
        self.region = get_aws_region()
        self.partition = get_aws_partition()
        self.account = context.invoked_function_arn.split(":")[4]

        if self.error:
            metrics.add_metric("JobFailure", unit=MetricUnit.Count, value=1)
            self.message = self._build_error_message()
        else:
            metrics.add_metric("JobSuccess", unit=MetricUnit.Count, value=1)
            self.message = self._build_success_message()

        self.default = self._build_default_message()
        self.sms = self._build_sms_message()
        self.json = self._build_json_message()

    def _build_json_message(self):
        return json.dumps(
            {
                "datasetGroup": self.dataset_group,
                "status": "UPDATE FAILED" if self.error else "UPDATE COMPLETE",
                "summary": self._build_default_message(),
                "description": self.message,
            }
        )

    def _build_default_message(self) -> str:
        return f"The personalization workflow for {self.dataset_group} completed {'with errors' if self.error else 'successfully'}"

    def _build_sms_message(self) -> str:
        return self._build_default_message()

    def _build_error_message(self) -> str:
        """
        Build the error message
        :return: the error message (with optional traces)
        """
        error_cause = json.loads(self.error.get("Cause", "{}"))
        error_message = error_cause.get("errorMessage", UNKNOWN_SOURCE)

        message = f"There was an error running the personalization job for dataset group {self.dataset_group}\n\n"
        message += f"Message: {error_message}\n\n"
        traces = self.get_trace_link()
        if traces:
            message += f"Traces: {traces}"
        return message

    def _build_success_message(self) -> str:
        """
        Build the success message
        :return: the success message
        """
        console_link = f"https://console.aws.amazon.com/personalize/home?region={self.region}#arn:{self.partition}:personalize:{self.region}:{self.account}:dataset-group${self.dataset_group}/setup"

        message = f"The Personalization job for dataset group {self.dataset_group} is complete\n\n"
        message += f"Link: {console_link}"
        return message

    def get_trace_link(self) -> Optional[str]:
        """
        Check if an X-Ray Trace Link deep can be provided, and provide it
        :return: The X-Ray Trace link or None
        """
        trace_id = os.environ.get("_X_AMZN_TRACE_ID", "").split(";")[0].strip("Root=")

        if trace_id:
            trace_deep_link = f"https://console.aws.amazon.com/xray/home?region={self.region}#/traces/{trace_id}"
            return trace_deep_link
        else:
            return None


@metrics.log_metrics
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """Create an SNS notification email"
    :param dict event: AWS Lambda Event
    :param context:
    :return: None
    """
    sns = get_service_client("sns")
    message_builder = MessageBuilder(event, context)
    subject = f"{solution_name()} Notifications"

    logger.info("publishing message for event", extra={"event": event})
    sns.publish(
        TopicArn=topic_arn(),
        Message=json.dumps(
            {
                "default": message_builder.default,
                "sms": message_builder.sms,
                "email": message_builder.message,
                "email-json": message_builder.json,
                "sqs": message_builder.json,
            }
        ),
        MessageStructure="json",
        Subject=subject,
    )
