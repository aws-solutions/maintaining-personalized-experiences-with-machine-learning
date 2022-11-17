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

import json
from dataclasses import dataclass, field
from typing import List, Dict, Callable, Optional

from aws_solutions.core import get_aws_partition, get_aws_region, get_aws_account
from shared.personalize_service import Personalize, logger
from shared.resource import DatasetGroup, Resource, Filter
from shared.resource import (
    EventTracker,
    Dataset,
    Schema,
    Solution,
    Campaign,
    BatchInferenceJob,
)


@dataclass(eq=True, frozen=True)
class ResourceElement:
    resource: Resource = field(repr=False, compare=True)
    arn: str = field(repr=True, compare=True)


@dataclass
class ResourceTree:
    resources: ResourceElement = field(default_factory=dict, init=False, repr=False)
    _resource_elements: Dict = field(default_factory=dict, init=False, repr=False)
    _resource_parentage: Dict = field(default_factory=dict, init=False, repr=False)

    def add(self, parent: ResourceElement, child: ResourceElement):
        if child not in self._resource_parentage.keys():
            self._resource_parentage[child] = parent
            self._resource_elements.setdefault(parent, []).append(child)
        else:
            raise ValueError("element already exists")

    def children(self, of: ResourceElement, where: Callable = lambda _: True) -> List[ResourceElement]:
        return [elem for elem in self._resource_elements[of] if where(elem)]


class ServiceModel:
    """Lists all resources in Amazon Personalize for lookup against the dataset group ARN"""

    def __init__(self, cli: Personalize, dataset_group_name=None):
        self.cli = cli
        self._arn_ownership = {}
        self._resource_tree = ResourceTree()

        if dataset_group_name:
            dsgs = [DatasetGroup().arn(dataset_group_name)]
        else:
            dsgs = self._arns(self.cli.list(DatasetGroup()))

        for dsg in dsgs:
            logger.debug(f"listing children of {dsg}")
            self._list_children(DatasetGroup(), dsg, dsg)

    def owned_by(self, resource_arn, dataset_group_owner: str) -> bool:
        """
        Check
        :param resource_arn: the resource ARN to check
        :param dataset_group_owner: the dataset group owner expected
        :return: True if the resource is managed by the dataset group, otherwise False
        """
        if not dataset_group_owner.startswith("arn:"):
            dataset_group_owner = f"arn:{get_aws_partition()}:personalize:{get_aws_region()}:{get_aws_account()}:dataset-group/{dataset_group_owner}"

        return dataset_group_owner == self._arn_ownership.get(resource_arn, False)

    def available(self, resource_arn: str) -> bool:
        """
        Check if the requested ARN is available
        :param resource_arn: requested ARN
        :return: True if the ARN is available, otherwise False
        """
        all_arns = set(self._arn_ownership.keys()).union(set(self._arn_ownership.values()))
        return resource_arn not in all_arns

    def _list_children(self, parent: Resource, parent_arn, dsg: str) -> None:
        """
        Recursively list the children of a resource
        :param parent: the parent Resource
        :param parent_arn: the parent Resource ARN
        :param dsg: the parent dataset group ARN
        :return: None
        """
        for c in parent.children:
            child_arns = self._arns(self.cli.list(c, filters={f"{parent.name.camel}Arn": parent_arn}))

            for arn in child_arns:
                logger.debug(f"listing children of {arn}")
                self._resource_tree.add(
                    parent=ResourceElement(parent, parent_arn),
                    child=ResourceElement(c, arn),
                )
                self._arn_ownership[arn] = dsg
                self._list_children(c, arn, dsg)

    def _arns(self, l: List[Dict]) -> List[str]:
        """
        Lists the first ARN found for each resource in a list of resources
        :param l: the list of resources
        :return: the list of ARNs
        """
        return [[v for k, v in resource.items() if k.endswith("Arn")][0] for resource in l]

    def _filter(self, result: Dict) -> Dict:
        resource_key = next(iter(k for k in result.keys() if k != "ResponseMetadata"))
        result = result[resource_key]
        result = {k: v for k, v in result.items() if k == "recipeArn" or not k.endswith("Arn")}

        # common
        result.pop("status", None)
        result.pop("creationDateTime", None)
        result.pop("lastUpdatedDateTime", None)

        # event tracker
        result.pop("accountId", None)
        result.pop("trackingId", None)

        # datset
        result.pop("datasetType", None)

        # schema
        if resource_key == "schema":
            result["schema"] = json.loads(result["schema"])

        # solution
        result.pop("latestSolutionVersion", None)

        # campaign
        result.pop("latestCampaignUpdate", None)

        # batch job
        for item in {
            "failureReason",
            "jobInput",
            "jobOutput",
            "jobName",
            "roleArn",
            "solutionVersionArn",
        }:
            result.pop(item, None)

        return result

    def get_config(self, dataset_group_name, schedules: Optional[Dict]) -> Dict:
        dataset_group_arn = DatasetGroup().arn(dataset_group_name)
        dataset_group = ResourceElement(DatasetGroup(), dataset_group_arn)

        config = {
            "datasetGroup": {"serviceConfig": self._filter(self.cli.describe(DatasetGroup(), name=dataset_group_name))}
        }

        self._add_filter_config(config, dataset_group)
        self._add_event_tracker_config(config, dataset_group)
        self._add_datasets(config, dataset_group)
        self._add_solutions(config, dataset_group)
        self._add_schedules(config, schedules)

        return config

    def _add_schedules(self, config: Dict, schedules: Optional[Dict]) -> None:
        """
        Modify config in place to add schedules
        :param config: the config dictionary
        :param schedules: the schedules to add
        :return: None
        """
        if not schedules:
            return

        if schedules.get("import"):
            config["datasetGroup"]["workflowConfig"] = {"schedules": {"import": schedules.get("import")}}

        solution_schedules = schedules.get("solutions", {})
        for idx, solution in enumerate(config.get("solutions", [])):
            name = solution.get("serviceConfig", {}).get("name")
            schedules = solution_schedules.get(name)
            if schedules:
                config["solutions"][idx]["workflowConfig"] = {"schedules": schedules}

    def _add_solutions(self, config, of: ResourceElement) -> None:
        """
        Modify the config in place to add solutions, campaigns, and batch inference jobs
        :param config: the config dictionary
        :param of: the solution ResourceElement
        :return: None
        """
        solutions = self._resource_tree.children(of, where=lambda x: x.resource == Solution())
        if not solutions:
            return

        config.setdefault("solutions", [])
        for solution in solutions:
            _solution = self.cli.describe_by_arn(Solution(), solution.arn)
            _solution_config = {"serviceConfig": self._filter(_solution)}

            campaigns = self._resource_tree.children(of=solution, where=lambda x: x.resource == Campaign())
            for campaign in campaigns:
                _campaign = self.cli.describe_by_arn(Campaign(), campaign.arn)
                _solution_config.setdefault("campaigns", []).append({"serviceConfig": self._filter(_campaign)})

            batch_jobs = self._resource_tree.children(of=solution, where=lambda x: x.resource == BatchInferenceJob())
            for batch_job in batch_jobs:
                _batch_job = self.cli.describe_by_arn(BatchInferenceJob(), batch_job.arn)
                _solution_config.setdefault("batchInferenceJobs", []).append(
                    {"serviceConfig": self._filter(_batch_job)}
                )
            config["solutions"].append(_solution_config)

    def _add_filter_config(self, config: Dict, of: ResourceElement) -> None:
        """
        Modify the config in place to add filters
        :param config: the config dictionary
        :param of: the DatasetGroup ResourceElement
        :return: None
        """
        filters = self._resource_tree.children(of, where=lambda x: x.resource == Filter())
        if not filters:
            return

        config["filters"] = [
            {"serviceConfig": self._filter(self.cli.describe_by_arn(filter.resource, filter.arn))} for filter in filters
        ]

    def _add_event_tracker_config(self, config: Dict, of: ResourceElement) -> None:
        """
        Modify the config in place to add an event tracker
        :param config: the config dictionary
        :param of: the DatasetGroup ResourceElement
        :return: None
        """
        event_tracker = next(
            iter(self._resource_tree.children(of, where=lambda x: x.resource == EventTracker())),
            None,
        )
        if not event_tracker:
            return
        config["eventTracker"] = {
            "serviceConfig": self._filter(self.cli.describe_by_arn(event_tracker.resource, event_tracker.arn))
        }

    def _add_datasets(self, config, of: ResourceElement) -> None:
        """
        Modify the config in place to add all datasets
        :param config: the config dictionary
        :param of: the DatasetGroup ResourceElement
        :return: None
        """
        for dataset_type in Dataset().allowed_types:
            self._add_dataset(config, dataset_type, of)

    def _add_dataset(self, config: Dict, dataset_type: str, of: ResourceElement) -> None:
        """
        Modify the config in place to add a dataset and schema
        :param config: the config dictionary
        :param dataset_type: the dataset type (must be ITEMS, INTERACTIONS, or USERS)
        :param of: the DatasetGroup ResourceElement
        :return: None
        """
        if dataset_type not in Dataset().allowed_types:
            raise ValueError(f"dataset type {dataset_type} must be one of {Dataset().allowed_types}")

        dataset = next(
            iter(
                self._resource_tree.children(
                    of,
                    where=lambda x: x.resource == Dataset() and x.arn.endswith(dataset_type),
                )
            ),
            None,
        )
        if not dataset:
            return

        dataset = self.cli.describe_by_arn(Dataset(), dataset.arn)
        config.setdefault("datasets", {})
        config["datasets"].setdefault(dataset_type.lower(), {})
        config["datasets"][dataset_type.lower()].setdefault("dataset", {"serviceConfig": self._filter(dataset)})
        config["datasets"][dataset_type.lower()].setdefault(
            "schema",
            {"serviceConfig": self._filter(self.cli.describe_by_arn(Schema(), arn=dataset["dataset"]["schemaArn"]))},
        )
