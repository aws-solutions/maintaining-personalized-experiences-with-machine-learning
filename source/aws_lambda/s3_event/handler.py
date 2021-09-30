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

from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.data_classes import S3Event

from aws_solutions.core.helpers import get_service_client
from shared.personalize_service import Configuration
from shared.sfn_middleware import set_bucket, start_execution

logger = Logger()
tracer = Tracer()
metrics = Metrics()


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


def send_configuration_error(configuration: Configuration):
    errors = configuration.errors
    sns = get_service_client("sns")
    dataset_group = configuration.dataset_group

    subject = f"{solution_name()} Notifications"

    def build_default_message():
        f"The personalization workflow for {configuration.dataset_group} completed with errors."

    def build_json_message():
        return json.dumps(
            {
                "datasetGroup": dataset_group,
                "status": "UPDATE FAILED",
                "summary": "There were errors detected when reading a personalization job configuration file",
                "description": [error for error in errors],
            }
        )

    def build_long_message():
        message = "There were errors detected when reading a personalization job configuration file:\n\n"
        for error in errors:
            logger.error(f"Personalization job configuration error: {error}")
            message += f"   - {error}\n"
        message += "\nPlease correct these errors and upload the configuration again."
        return message

    logger.error("publishing configuration error to SQS")
    sns.publish(
        TopicArn=topic_arn(),
        Message=json.dumps(
            {
                "default": build_default_message(),
                "sms": build_default_message(),
                "email": build_long_message(),
                "email-json": build_json_message(),
                "sqs": build_json_message(),
            }
        ),
        MessageStructure="json",
        Subject=subject,
    )
    logger.error("published configuration error to SQS")


@metrics.log_metrics
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """Handles an S3 Event Notification (for any .json file written to any subfolder in "train/"
    :param dict event: AWS Lambda Event (in this case, an S3 Event message)
    :param context:
    :return: None
    """
    event: S3Event = S3Event(event)
    bucket = event.bucket_name
    s3 = get_service_client("s3")

    for record in event.records:
        key = record.s3.get_object.key
        logger.info(
            f"processing Amazon S3 event notification record for s3://{bucket}/{key}"
        )
        metrics.add_metric("ConfigurationsProcessed", unit=MetricUnit.Count, value=1)

        s3_config = s3.get_object(Bucket=bucket, Key=key)
        config_text = s3_config.get("Body").read().decode("utf-8")

        # create the configuration, check for errors
        configuration = Configuration()
        configuration.load(config_text)
        if configuration.errors:
            send_configuration_error(configuration)
            metrics.add_metric(
                "ConfigurationsProcessedFailures", unit=MetricUnit.Count, value=1
            )
            return

        # configuration has loaded, validate it
        configuration.validate()
        if configuration.errors:
            metrics.add_metric(
                "ConfigurationsProcessedFailures", unit=MetricUnit.Count, value=1
            )
            send_configuration_error(configuration)
        else:
            config = configuration.config_dict
            config = set_bucket(config, bucket, key)
            metrics.add_metric(
                "ConfigurationsProcessedSuccesses", unit=MetricUnit.Count, value=1
            )
            start_execution(config)
