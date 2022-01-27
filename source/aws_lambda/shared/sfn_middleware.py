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
from __future__ import annotations

import datetime
import decimal
import json
import os
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Dict, Any, Callable, Optional, List, Union
from uuid import uuid4

import jmespath
from aws_lambda_powertools import Logger
from dateutil.parser import isoparse

from aws_solutions.core import get_service_client
from shared.date_helpers import parse_datetime
from shared.exceptions import (
    ResourcePending,
    ResourceInvalid,
    ResourceFailed,
    ResourceNeedsUpdate,
)
from shared.personalize_service import Personalize
from shared.resource import get_resource

logger = Logger()

STATUS_IN_PROGRESS = (
    "CREATE PENDING",
    "CREATE IN_PROGRESS",
    "DELETE PENDING",
    "DELETE IN_PROGRESS",
)
STATUS_FAILED = "CREATE FAILED"
STATUS_ACTIVE = "ACTIVE"

WORKFLOW_PARAMETERS = {
    "maxAge",
    "timeStarted",
}
WORKFLOW_CONFIG_DEFAULT = {
    "timeStarted": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
}


class Arity(Enum):
    ONE = auto()
    MANY = auto()


def json_handler(item):
    if isinstance(item, datetime.datetime):
        return item.isoformat()
    elif isinstance(item, decimal.Decimal) and item.as_integer_ratio()[1] == 1:
        return int(item)
    elif isinstance(item, decimal.Decimal) and item.as_integer_ratio()[1] != 1:
        return float(item)
    raise TypeError("Unknown Type")


def set_workflow_config(config: Dict) -> Dict:
    """
    Set the defaults for workflowConfiguration for all configured items
    :param config: the configuration dictionary
    :return: the configuration with defaults set
    """

    resources = {
        "datasetGroup": Arity.ONE,
        "solutions": Arity.MANY,
        "recommenders": Arity.MANY,
        "campaigns": Arity.MANY,
        "batchInferenceJobs": Arity.MANY,
        "batchSegmentJobs": Arity.MANY,
        "filters": Arity.MANY,
        "solutionVersion": Arity.ONE,
    }
    # Note: schema creation notification is not supported at this time
    # Note: dataset, dataset import job, event tracker notifications are added in the workflow

    for k, v in config.items():
        if k in {"serviceConfig", "workflowConfig", "bucket", "currentDate"}:
            pass  # do not modify any serviceConfig keys
        elif k in resources.keys() and resources[k] == Arity.ONE:
            config[k].setdefault("workflowConfig", {})
            config[k]["workflowConfig"] |= WORKFLOW_CONFIG_DEFAULT
        elif k in resources.keys() and resources[k] == Arity.MANY:
            for idx, i in enumerate(v):
                config[k][idx].setdefault("workflowConfig", {})
                config[k][idx]["workflowConfig"] |= WORKFLOW_CONFIG_DEFAULT
                config[k][idx] = set_workflow_config(config[k][idx])
        else:
            config[k] = set_workflow_config(config[k]) if config[k] else config[k]

    return config


def set_defaults(config: Dict) -> Dict:
    """
    Set the defaults for schedule/ solutions/ solution versions/ campaigns as empty if not set
    :param config: the configuration dictionary
    :return: the configuration with defaults set
    """
    # always include/ override the current date
    config["currentDate"] = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    # always include a maxAge for the datasets
    config.setdefault("datasetGroup", {})
    config["datasetGroup"].setdefault("workflowConfig", {})
    config["datasetGroup"]["workflowConfig"].setdefault("maxAge", "365 days")

    # by default, don't include a solution
    solutions = config.setdefault("solutions", [])
    for s_idx, solution in enumerate(solutions):
        # by default, don't include a solution version
        config["solutions"][s_idx].setdefault("solutionVersions", [])
        # by default, don't include a campaign or batch inference or segment job
        config["solutions"][s_idx].setdefault("campaigns", [])
        config["solutions"][s_idx].setdefault("batchInferenceJobs", [])
        config["solutions"][s_idx].setdefault("batchSegmentJobs", [])

    # by default, don't include a recommender
    recommenders = config.setdefault("recommenders", [])
    for r_idx, recommender in enumerate(recommenders):
        # by default, don't include a campaign or batch inference or segment job
        config["recommenders"][r_idx].setdefault("batchInferenceJobs", [])
        config["recommenders"][r_idx].setdefault("batchSegmentJobs", [])

    return config


def set_bucket(config: Dict, bucket: str, key: str) -> Dict:
    config["bucket"] = {"name": bucket, "key": str(Path(key).parent)}
    return config


def start_execution(config):
    sfn = get_service_client("stepfunctions")
    state_machine_arn = os.environ.get("STATE_MACHINE_ARN")
    config = set_defaults(config)

    logger.info("starting state machine execution")
    sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=str(uuid4()),
        input=json.dumps(config, default=json_handler),
    )


@dataclass
class Parameter:
    key: str
    source: str
    path: str
    format_as: Optional[str]
    default: Optional[str]

    def get_default(self):
        if self.default == "omit":
            return None
        else:
            return self.default

    def format(self, resolved):
        if not self.format_as:
            return resolved

        if self.format_as == "string":
            return json.dumps(resolved)
        elif self.format_as == "seconds":
            return parse_datetime(resolved)
        elif self.format_as == "iso8601":
            return isoparse(resolved)
        elif self.format_as == "int":
            return int(resolved)
        else:
            raise ValueError(f"Invalid format_as value {self.format_as}")

    def resolve(self, event) -> Optional[Union[str, Dict, None]]:
        if self.source == "event":
            resolved = jmespath.search(self.path, event)
        elif self.source == "environment":
            resolved = os.environ.get(self.path)
        else:
            raise ValueError(
                f"Missing or misconfigured event `source` (got {self.source})"
            )

        if not resolved:
            resolved = self.get_default()

        if not resolved and self.default != "omit":
            raise ValueError(
                f"missing configuration for {self.key}, expected from {self.source} at path {self.path}"
            )

        if resolved:
            return self.format(resolved)
        else:
            return None


@dataclass
class ResourceConfiguration:
    event: Dict
    config: Dict
    parameters: List[Parameter] = field(default_factory=list, init=False)

    def __post_init__(self):
        for key, source_configuration in self.config.items():
            if not isinstance(source_configuration, dict):
                raise ValueError("config must be Dict[str, Dict[str, str]]")

            parameter = Parameter(
                key=key,
                source=source_configuration["source"],
                path=source_configuration["path"],
                default=source_configuration.get("default", None),
                format_as=source_configuration.get("as", None),
            )
            self.parameters.append(parameter)

    @property
    def kwargs(self):
        configuration = {}
        for parameter in self.parameters:
            resolved = parameter.resolve(self.event)
            if resolved:
                configuration[parameter.key] = resolved
        logger.debug(configuration)
        return configuration


class PersonalizeResource:
    def __init__(
        self, resource: str, status: str = None, config: Optional[Dict] = None
    ):
        self.resource: str = resource
        self.status: str = status
        self.config: Dict[str, Dict] = config if config else {}

    def check_status(  # NOSONAR - allow higher complexity
        self, resource: Dict[str, Any], **expected
    ) -> Dict:
        # Check for resource property mismatch (filters, solutions are not scoped to their dataset group)
        received = resource.get(self.resource)
        mismatch = []
        case_insensitive_keys = ["datasetType"]

        for expected_key, expected_value in expected.items():
            actual_value = received.get(expected_key)

            # some keys are json strings and should be converted to dict for comparison
            if self.config.get(expected_key, {}).get("as") == "string":
                expected_value = json.loads(expected_value)
                actual_value = json.loads(actual_value)

            # some keys are case insensitive
            if expected_key in case_insensitive_keys:
                actual_value = actual_value.lower()
                expected_value = expected_value.lower()

            # some parameters don't require checking:
            if self.resource == "datasetImportJob" and expected_key in {
                "jobName",
                "dataSource",
                "roleArn",
            }:
                continue
            if self.resource.startswith("batch") and expected_key in {
                "jobName",
                "jobInput",
                "jobOutput",
                "roleArn",
            }:
                continue
            if self.resource == "solutionVersion" and expected_key == "trainingMode":
                continue
            if expected_key in WORKFLOW_PARAMETERS:
                continue

            if actual_value != expected_value:
                mismatch.append(
                    f"expected {expected_key} to be {expected_value} but got {actual_value}"
                )
        if mismatch:
            raise ResourceFailed(
                f"{'. '.join(mismatch)}. This can happen if a user modifies a resource out-of-band "
                f"with the solution, if you have attempted to use a resource of the same name and "
                f"a different configuration across dataset groups, or are attempting multiple "
                f"solution maintenance jobs at the same time"
            )

        # certain resources do not have a status (e.g. Schema)
        if not self.status:
            return resource

        status = jmespath.search(self.status, resource) or "invalid"
        if status in STATUS_ACTIVE:
            return resource
        elif status in STATUS_IN_PROGRESS:
            logger.debug({"message": "resource is pending", "resource": {**resource}})
            raise ResourcePending()
        elif status in STATUS_FAILED:
            logger.error({"message": "resource has failed", "resource": {**resource}})
            raise ResourceFailed()
        else:
            logger.error({"message": "resource is invalid", "resource": {**resource}})
            raise ResourceInvalid()

    def __call__(self, func: Callable):
        def decorator(event, context):
            cli = Personalize()

            config = ResourceConfiguration(event, self.config)
            kwargs = config.kwargs

            # describe or create
            resource = get_resource(self.resource)
            try:
                resource = cli.describe(resource, **kwargs)
            except cli.exceptions.ResourceNotFoundException:
                cli.create(resource, **kwargs)
                raise ResourcePending()
            except cli.exceptions.ResourceInUseException:
                # this occurs during an update or a create on resume
                raise ResourcePending()
            except ResourceNeedsUpdate:
                cli.update(resource, **kwargs)
                raise ResourcePending()

            # check the status of the resource
            self.check_status(resource, **kwargs)

            # convert any non-processable fields to something we can handle
            event["resource"] = json.loads(
                json.dumps(
                    jmespath.search(self.resource, resource), default=json_handler
                )
            )
            return func(event, context)

        return decorator
