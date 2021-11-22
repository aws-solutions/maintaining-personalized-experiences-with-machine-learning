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

from shared.personalize.service_model import ServiceModel
from shared.personalize_service import Personalize


logger = Logger()
tracer = Tracer()
metrics = Metrics()


@metrics.log_metrics
@tracer.capture_lambda_handler
def lambda_handler(event, context):
    """Generate and return a solution configuration file derived from the properties of a dataset group
    :param dict event: AWS Lambda Event (in this case, the dataset group and schedules)
    :param context: AWS Lambda Context
    :return: Dict
    """
    dataset_group_name = event["datasetGroupName"]
    schedules = event.get("schedules")

    cli = Personalize()
    model = ServiceModel(cli, dataset_group_name=dataset_group_name)
    return model.get_config(dataset_group_name=dataset_group_name, schedules=schedules)
