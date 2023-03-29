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
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Union

import avro.schema
import botocore.exceptions
import jmespath
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit, SchemaValidationError
from aws_solutions.core import (
    get_aws_account,
    get_aws_partition,
    get_aws_region,
    get_service_client,
)
from aws_solutions.scheduler.common import Schedule, ScheduleError
from botocore.stub import Stubber
from dateutil.tz import tzlocal
from shared.events import Notifies
from shared.exceptions import (
    ResourceFailed,
    ResourceNeedsUpdate,
    ResourcePending,
    SolutionVersionPending,
)
from shared.resource import (
    BatchInferenceJob,
    BatchSegmentJob,
    Campaign,
    Dataset,
    DatasetGroup,
    DatasetImportJob,
    EventTracker,
    Filter,
    Resource,
    Schema,
    Solution,
    SolutionVersion,
)
from shared.s3 import S3

logger = Logger()
metrics = Metrics()

STATUS_CREATING = ("ACTIVE", "CREATE PENDING", "CREATE IN_PROGRESS")
CRON_ANY_WILDCARD = "?"
CRON_MIN_MAX_YEAR = (1970, 2199)
WORKFLOW_PARAMETERS = (
    ("maxAge", Resource),
    ("timeStarted", Resource),
    ("solutionVersionArn", SolutionVersion),
)


def get_duplicates(items):
    if isinstance(items, str):
        return []
    elif isinstance(items, list):
        s = set()
        return list(set(i for i in items if i in s or s.add(i)))


class Personalize:
    def __init__(self):
        self.cli = get_service_client("personalize")

    def arn(self, resource: Resource, name: str):
        arn = (
            f"arn:{get_aws_partition()}:personalize:{get_aws_region()}:{get_aws_account()}:{resource.name.dash}/{name}"
        )
        return {f"{resource.name.camel}Arn": arn}

    def list(self, resource: Resource, filters: Optional[Dict] = None):
        if not filters:
            filters = {}
        list_fn_name = f"list_{resource.name.snake}s"
        paginator = self.cli.get_paginator(list_fn_name)
        iterator = paginator.paginate(**filters)
        for page in iterator:
            resource_key = [k for k in list(page.keys()) if k not in ("ResponseMetadata", "nextToken")].pop()
            for item in page[resource_key]:
                yield item

    @Notifies("ACTIVE")
    def describe(self, resource: Resource, **kwargs):
        """
        Describe a resource in Amazon Personalize
        :param resource: the resource to describe
        :param kwargs:  the resource keyword arguments
        :return: the resource from Amazon Personalize
        """
        logger.debug(f"describing {resource.name.camel}")
        if resource.name.camel == "dataset":
            return self.describe_dataset(**kwargs)
        elif resource.name.camel == "datasetImportJob":
            return self.describe_dataset_import_job(**kwargs)
        elif resource.name.camel == "solutionVersion":
            return self.describe_solution_version(**kwargs)
        elif resource.name.camel == "eventTracker":
            return self.describe_event_tracker(**kwargs)
        elif resource.name.camel == "batchInferenceJob":
            return self.describe_batch_inference_job(**kwargs)
        elif resource.name.camel == "batchSegmentJob":
            return self.describe_batch_segment_job(**kwargs)
        elif resource.name.camel == "campaign":
            return self.describe_with_update(resource, **kwargs)
        else:
            return self.describe_default(resource, **kwargs)

    def list_tags_for_resource(self, **kwargs):
        logger.debug(f"listing tags for {kwargs}")
        describe_fn_name = f"list_tags_for_resource"
        describe_fn = getattr(self.cli, describe_fn_name)
        return describe_fn(**kwargs)

    def describe_default(self, resource: Resource, **kwargs):
        """
        Describe a resource in Amazon Personalize by deriving its ARN from its name
        :param resource: the resource to describe
        :param kwargs: the resource keyword arguments
        :return: the response from Amazon Personalize
        """
        describe_fn_name = f"describe_{resource.name.snake}"
        describe_fn = getattr(self.cli, describe_fn_name)
        return describe_fn(**self.arn(resource, kwargs["name"]))

    def describe_by_arn(self, resource: Resource, arn: str):
        describe_fn_name = f"describe_{resource.name.snake}"
        describe_fn = getattr(self.cli, describe_fn_name)
        return describe_fn(**{f"{resource.name.camel}Arn": arn})

    def _check_solution(self, sv_arn_expected: str, sv_arn_received: str) -> bool:
        """
        Check if solution versions sv_received and sv_expected have the same solution ARN
        :param sv_arn_expected: the first solution version
        :param sv_arn_received: the second solution version
        :return: None
        """
        sol_arn_expected = sv_arn_expected.rsplit("/", 1)[0]
        sol_arn_received = sv_arn_received.rsplit("/", 1)[0]
        if sol_arn_expected != sol_arn_received:
            raise ResourceFailed(
                f"Expected solution ARN {sol_arn_expected} but got {sol_arn_received}. This can happen if a user "
                f"modifies a resource out-of-band with the solution, or if you have attempted to use a resource of the "
                f"same name and a different configuration across dataset groups"
            )

    def describe_with_update(self, resource: Resource, **kwargs):
        """
        Describe a resource / determine if it requires an update
        :param resource: the resource to update/ describe
        :param kwargs: the resource keyword arguments to validate
        :return: the response from Amazon Personalize
        """
        kwargs = self._remove_workflow_parameters(resource, kwargs.copy())
        result = self.describe_default(resource, **kwargs)
        for k, v in kwargs.items():
            # tags are not returned in any describe call
            if k == "tags":
                continue

            received = result[resource.name.camel][k]
            expected = v

            # check the solution matches
            if k == "solutionVersionArn":
                self._check_solution(expected, received)

            if result[resource.name.camel].get(k) != v:
                raise ResourceNeedsUpdate(result[resource.name.camel][f"{resource.name.camel}Arn"])
        return result

    def _remove_workflow_parameters(self, resource: Resource, kwargs):
        """
        Remove workflow parameters for the keyword arguments presented
        :param kwargs:
        :return: the kwargs with the solution parameters removed
        """
        for key, resource_type in WORKFLOW_PARAMETERS:
            if isinstance(resource, resource_type):
                kwargs.pop(key, None)
        return kwargs

    def _describe_from_parent(self, resource: Resource, parent: Resource, condition: Callable = None, **kwargs):
        """
        Describe a resource from Amazon Personalize by listing from its parent, then filtering the list on `condition`
        :param resource: the Amazon Personalize resource (e.g. dataset)
        :param parent: the Amazon Personalize resources' parent (e.g. dataset_group)
        :param condition: a condition to filter the child resources on (e.g. lambda job: job["status"] in STATUS_CREATING)
        :param kwargs: the keyword arguments that would be passed to create this Amazon Personalize Resource
        :return: the first discovered child fulfilling the `condition` as listed from the parent
        """
        parent_arn = kwargs[f"{parent.name.camel}Arn"]
        list_fn_kwargs = {f"{parent.name.camel}Arn": parent_arn}

        children = self.list(resource=resource, filters=list_fn_kwargs)
        if condition:
            child = next(
                iter(
                    sorted(
                        [child for child in children if condition(child)],
                        key=lambda child: child["creationDateTime"],
                        reverse=True,
                    )
                ),
                None,
            )
        else:
            child = next(iter(child for child in children), None)

        if not child:
            raise self.cli.exceptions.ResourceNotFoundException(
                {
                    "Code": "ResourceNotFoundException",
                    "Message": f"Could not find {resource.name.camel} for {parent.name.camel} {parent_arn}",
                },
                f"List{resource.name.camel[0].upper()}{resource.name.camel[1:]}s",
            )
        else:
            # finalize by describing the listed child

            describe_fn_name = f"describe_{resource.name.snake}"
            describe_fn = getattr(self.cli, describe_fn_name)
            describe_arn = f"{resource.name.camel}Arn"
            child = describe_fn(**{describe_arn: child[describe_arn]})

        return child

    def describe_dataset(self, **kwargs):
        """
        Do a list to list all datasets for a specific dataset group instead of a describe
        :param kwargs: the resource keyword arguments
        :return: the response from Amazon Personalize representing the listed dataset
        """
        dataset_type = kwargs["datasetType"].upper()
        dataset = self._describe_from_parent(
            resource=Dataset(),
            parent=DatasetGroup(),
            condition=lambda dataset: dataset["datasetType"] == dataset_type,
            **kwargs,
        )

        return dataset

    def is_current(  # NOSONAR - allow higher complexity
        self,
        old_job: Dict,
        new_job: Dict,
        name_key: Optional[str] = None,
        s3: Optional[S3] = None,
    ):
        if name_key:
            old_job_name = old_job[name_key]
            new_job_name = new_job.get(name_key, "UNKNOWN")

            # this is a current job
            if old_job_name == new_job_name:
                logger.info(f"{new_job_name} may be current")
                return True
        else:
            arn_key = next(iter(key for key in old_job.keys() if key.endswith("Arn")), "UNKNOWN")
            old_job_name = old_job.get(arn_key, "UNKNOWN")

        # check if the job is active/ creating, otherwise filter out
        old_job_status = old_job["status"]
        if old_job_status not in STATUS_CREATING:
            logger.debug(f"{old_job_name} has status {old_job_status} which is not active or creating")
            return False

        # check if the job is within maxAge if provided
        max_age = new_job.get("maxAge", None)
        if max_age and old_job_status == "ACTIVE":
            now_dt = datetime.now(tzlocal())
            job_dt = old_job["lastUpdatedDateTime"]
            job_age = (now_dt - job_dt).total_seconds()

            job_past_max_age = job_age > max_age

            # if we need to compare to an S3 object - include the check
            if s3:
                data_dt = s3.last_modified
                new_data_available = data_dt > job_dt
            else:
                new_data_available = True

            if job_past_max_age and new_data_available:
                logger.debug(f"{old_job_name} is not current")
                return False
            elif job_past_max_age and not new_data_available:
                logger.info(f"{old_job_name} is not current, but no new data is available")
                return True
            elif not job_past_max_age:
                logger.info(f"{old_job_name} remains current ({int(max_age - job_age)}s remaining)")
                return True
        elif max_age and old_job_status != "ACTIVE":
            # this handles the case where we're working with solution version updates (since they do not have a name)
            logger.debug(f"{old_job_name} remains current as it is {old_job_status}")
            return True
        else:
            logger.debug(f"{old_job_name} is active")
            return True

    def describe_dataset_import_job(self, **kwargs):
        """
        Do a list to list all dataset import jobs for a specific dataset and return the latest one
        :param kwargs: the resource keyword arguments
        :return: the response from Amazon Personalize representing the listed dataset import job
        """
        s3_url: str = kwargs["dataSource"]["dataLocation"]

        s3 = S3(url=s3_url)
        contents_exist = s3.exists

        if not contents_exist:
            raise s3.cli.meta.client.exceptions.NoSuchKey(
                {
                    "Code": "NoSuchKey",
                    "Message": f"Could not find csv content at {s3_url}",
                },
                "HeadObject",
            )

        def is_active_import(job: Dict):
            return self.is_current(
                new_job=kwargs,
                old_job=job,
                name_key="jobName",
                s3=s3,
            )

        return self._describe_from_parent(
            resource=DatasetImportJob(),
            parent=Dataset(),
            condition=is_active_import,
            **kwargs,
        )

    def describe_solution_version(self, **kwargs):
        def is_active_solution_version(job: Dict):
            return self.is_current(
                new_job=kwargs,
                old_job=job,
                name_key="solutionVersionArn",
            )

        solution_version = self._describe_from_parent(
            resource=SolutionVersion(),
            parent=Solution(),
            condition=is_active_solution_version,
            **kwargs,
        )

        self._record_offline_metrics(solution_version)
        return solution_version

    def describe_event_tracker(self, **kwargs):
        return self._describe_from_parent(
            resource=EventTracker(),
            parent=DatasetGroup(),
            condition=lambda job: job["status"] in STATUS_CREATING,
            **kwargs,
        )

    def describe_batch_inference_job(self, **kwargs):
        def is_active_batch_inference_job(job: Dict):
            return self.is_current(new_job=kwargs, old_job=job, name_key="jobName")

        return self._describe_from_parent(
            resource=BatchInferenceJob(),
            parent=SolutionVersion(),
            condition=is_active_batch_inference_job,
            **kwargs,
        )

    def describe_batch_segment_job(self, **kwargs):
        def is_active_batch_segment_job(job: Dict):
            return self.is_current(new_job=kwargs, old_job=job, name_key="jobName")

        return self._describe_from_parent(
            resource=BatchSegmentJob(),
            parent=SolutionVersion(),
            condition=is_active_batch_segment_job,
            **kwargs,
        )

    @Notifies("UPDATING")
    def update(self, resource: Resource, **kwargs):
        update_fn_name = f"update_{resource.name.snake}"
        update_fn = getattr(self.cli, update_fn_name)

        # always remove the workflow configuration parameters before update
        kwargs = self._remove_workflow_parameters(resource, kwargs)

        # set up the ARN to update
        kwargs_arn = self.arn(resource, kwargs.pop("name"))
        kwargs.update(kwargs_arn)

        try:
            result = update_fn(**kwargs)
        except self.cli.exceptions.LimitExceededException as exc:
            if resource.has_soft_limit:
                logger.warning(f"soft limit encountered: {exc['Error']['Message']}")
                raise ResourcePending()  # raise ResourcePending to allow the step function to retry later
            else:
                raise  # this is not a retryable service limit
        except self.cli.exceptions.ResourceInUseException:
            raise ResourcePending()

        return result

    @Notifies("CREATING")
    def create(self, resource: Resource, **kwargs):
        create_fn_name = f"create_{resource.name.snake}"
        create_fn = getattr(self.cli, create_fn_name)

        # always remove the workflow configuration parameters before create
        kwargs = self._remove_workflow_parameters(resource, kwargs)

        try:
            result = create_fn(**kwargs)
            self.add_metric(resource)
        except self.cli.exceptions.LimitExceededException as exc:
            if resource.has_soft_limit:
                logger.warning(f"soft limit encountered: {exc['Error']['Message']}")
                raise ResourcePending()  # raise ResourcePending to allow the step function to retry later
            else:
                raise  # this is not a retryable service limit

        # for solution versions, raise an exception to save the version on create
        if resource.name.camel == "solutionVersion":
            raise SolutionVersionPending(f"{result['solutionVersionArn']}")

        return result

    def add_metric(self, resource: Resource):
        metrics.add_metric(
            f"{resource.name.snake.replace('_', ' ').title().replace(' ', '')}Created",
            unit=MetricUnit.Count,
            value=1,
        )

    def _flush_metrics(self):
        """
        Flush the current recorded metrics to stdout (EMF)
        :return: None
        """
        try:
            current_metrics = metrics.serialize_metric_set()
            print(json.dumps(current_metrics))
        except SchemaValidationError as exc:
            logger.info(f"metrics not flushed: {str(exc)}")  # no metrics to serialize or no namespace
        metrics.clear_metrics()

    def _record_offline_metrics(self, solution_version: Dict) -> None:
        """
        Record the solution version offline metrics to CloudWatch
        :param solution_version: The described solution version
        :return: None
        """
        self._flush_metrics()

        # change the metric dimensions for tracking personalize solution metrics
        metrics.add_dimension("service", "SolutionMetrics")
        metrics.add_dimension("solutionArn", solution_version["solutionVersion"]["solutionArn"])
        metrics._metric_units.append("None")

        metrics_response = self.cli.get_solution_metrics(
            solutionVersionArn=solution_version["solutionVersion"]["solutionVersionArn"]
        )
        for name, value in metrics_response["metrics"].items():
            metrics.add_metric(name, "None", float(value))

        # flush the solution offline metrics and reset the metric dimensions
        self._flush_metrics()

    @property
    def exceptions(self):
        return self.cli.exceptions


class InputValidator:
    @classmethod
    def validate(cls, method: str, expected_params: Dict) -> None:
        """
        Validate an Amazon Personalize resource config parameters using the botocore stubber
        :return: None. Raises ParamValidationError if the InputValidator fails to validate
        """
        cli = get_service_client("personalize")
        func = getattr(cli, method)

        with Stubber(cli) as stubber:
            stubber.add_response(method, {}, expected_params)
            func(**expected_params)


class Configuration:
    _schema = [
        {"tags": []},
        {
            "datasetGroup": [
                "serviceConfig",
                {
                    "workflowConfig": [
                        {"schedules": ["import"]},
                        "maxAge",
                    ]
                },
            ]
        },
        {
            "datasets": [
                "serviceConfig",
                {
                    "users": [
                        {"dataset": ["serviceConfig"]},
                        {"schema": ["serviceConfig"]},
                    ]
                },
                {
                    "items": [
                        {"dataset": ["serviceConfig"]},
                        {"schema": ["serviceConfig"]},
                    ]
                },
                {
                    "interactions": [
                        {"dataset": ["serviceConfig"]},
                        {"schema": ["serviceConfig"]},
                    ]
                },
            ]
        },
        {
            "eventTracker": ["serviceConfig"],
        },
        {
            "filters": [
                [
                    "serviceConfig",
                ]
            ]
        },
        {
            "recommenders": [
                [
                    "serviceConfig",
                ]
            ]
        },
        {
            "solutions": [
                [
                    "serviceConfig",
                    {"workflowConfig": {"schedules": ["full", "update", "maxAge"]}},
                    {"campaigns": [["serviceConfig"]]},
                    {
                        "batchInferenceJobs": [
                            [
                                "serviceConfig",
                                {"workflowConfig": ["schedule", "maxAge"]},
                            ]
                        ]
                    },
                    {
                        "batchSegmentJobs": [
                            [
                                "serviceConfig",
                                {"workflowConfig": ["schedule", "maxAge"]},
                            ]
                        ]
                    },
                ]
            ]
        },
    ]

    def __init__(self):
        self._configuration_errors = []
        self.config_dict = {}
        self.dataset_group = "UNKNOWN"
        self.pass_root_tags = False

    def load(self, content: Union[Path, str, dict]):
        if isinstance(content, dict):
            self.config_dict = content
        else:
            if isinstance(content, Path):
                config_str = content.read_text(encoding="utf-8")
            else:
                config_str = content
            self.config_dict = self._decode(config_str)

        self.pass_root_tags = jmespath.search("tags", self.config_dict)

    def validate(self):
        self._validate_not_empty()
        self._validate_keys()
        self._validate_root_tags()
        self._validate_tags(
            "datasetGroup.serviceConfig.tags",
            "datasets.serviceConfig.tags",
            "datasets.interactions.dataset.serviceConfig.tags",
            "datasets.users.dataset.serviceConfig.tags",
            "datasets.items.dataset.serviceConfig.tags",
            "filters[].serviceConfig.tags",
            "eventTracker.serviceConfig.tags",
            "solutions[].serviceConfig.tags",
            "solutions[].serviceConfig.solutionVersion.tags",
            "solutions[].campaigns[].serviceConfig.tags",
            "recommenders[].serviceConfig.tags",
            "solutions[].batchInferenceJobs[].serviceConfig.tags",
            "solutions[].batchSegmentJobs[].serviceConfig.tags",
        )
        self._validate_dataset_group()
        self._validate_schemas()
        self._validate_datasets()
        self._validate_dataset_import_job()
        self._validate_event_tracker()
        self._validate_filters()
        self._validate_solutions()
        self._validate_solution_update()
        self._validate_recommender()
        self._validate_cron_expressions(
            "datasetGroup.workflowConfig.schedules.import",
            "solutions[].workflowConfig.schedules.full",
            "solutions[].workflowConfig.schedules.update",
            "solutions[].batchInferenceJobs[].workflowConfig.schedule",
        )

        self._validate_naming()

        return len(self._configuration_errors) == 0

    def config_dict_wdefaults(self):
        self._validate_not_empty()
        self._validate_dataset_import_job()
        self._validate_solutions()
        self._validate_solution_update()
        return self.config_dict

    @property
    def errors(self) -> List[str]:
        return self._configuration_errors

    def _decode(self, config_str) -> Dict:
        """
        Decoded value the JSON string config_str or return an empty dictionary
        :param config_str: the json string
        :return: dictionary
        """
        try:
            return json.loads(config_str)
        except json.JSONDecodeError as exc:
            self._configuration_errors.append(f"Could not validate JSON: {exc}")
            return {}

    def _validate_resource(self, resource: Resource, expected_params):
        expected_params = expected_params.copy()

        try:
            InputValidator.validate(f"create_{resource.name.snake}", expected_params)

        except botocore.exceptions.ParamValidationError as exc:
            self._configuration_errors.append(str(exc).replace("\n", " "))

    def _validate_dataset_group(self, path="datasetGroup.serviceConfig"):
        dataset_group = jmespath.search(path, self.config_dict)
        if not dataset_group:
            self._configuration_errors.append(f"A datasetGroup must be provided at path datasetGroup")
        else:
            self._validate_resource(DatasetGroup(), dataset_group)
            if isinstance(dataset_group, dict):
                self.dataset_group = dataset_group.get("name", self.dataset_group)
                self._fill_default_vals("datasetGroup", dataset_group)

    def _validate_event_tracker(self, path="eventTracker.serviceConfig"):
        event_tracker = jmespath.search(path, self.config_dict)

        # no event tracker provided - nothing to validate
        if not event_tracker:
            return
        if not isinstance(event_tracker, dict):
            self._configuration_errors.append(f"{path} must be an object")
            return

        event_tracker["datasetGroupArn"] = DatasetGroup().arn("validation")

        self._validate_resource(EventTracker(), event_tracker)
        self._fill_default_vals("eventTracker", event_tracker)

    def _validate_filters(self, path="filters[].serviceConfig"):
        filters = jmespath.search(path, self.config_dict) or {}
        for idx, _filter in enumerate(filters):
            if not self._validate_type(_filter, dict, f"filters[{idx}].serviceConfig must be an object"):
                continue

            _filter["datasetGroupArn"] = DatasetGroup().arn("validation")
            self._validate_resource(Filter(), _filter)
            self._fill_default_vals("filter", _filter)

    def _validate_type(self, var, typ, err: str):
        validates = isinstance(var, typ)
        if not validates:
            self._configuration_errors.append(err)
        return validates

    def _validate_solutions(self, path="solutions[]"):
        solutions = jmespath.search(path, self.config_dict) or {}
        for idx, _solution in enumerate(solutions):
            campaigns = _solution.get("campaigns", [])
            if self._validate_type(campaigns, list, f"solutions[{idx}].campaigns must be a list"):
                self._validate_campaigns(f"solutions[{idx}].campaigns", campaigns)

            batch_inference_jobs = _solution.get("batchInferenceJobs", [])
            if batch_inference_jobs and self._validate_type(
                batch_inference_jobs,
                list,
                f"solutions[{idx}].batchInferenceJobs must be a list",
            ):
                self._validate_batch_inference_jobs(
                    path=f"solutions[{idx}].batchInferenceJobs",
                    solution_name=_solution.get("serviceConfig", {}).get("name", ""),
                    batch_inference_jobs=batch_inference_jobs,
                )

            batch_segment_jobs = _solution.get("batchSegmentJobs", [])
            if batch_segment_jobs and self._validate_type(
                batch_segment_jobs,
                list,
                f"solutions[{idx}].batchSegmentJobs must be a list",
            ):
                self._validate_batch_segment_jobs(
                    path=f"solutions[{idx}].batchSegmentJobs",
                    solution_name=_solution.get("serviceConfig", {}).get("name", ""),
                    batch_segment_jobs=batch_segment_jobs,
                )

            _solution = _solution.get("serviceConfig")

            if not self._validate_type(_solution, dict, f"solutions[{idx}].serviceConfig must be an object"):
                continue

            # `performAutoML` is currently returned from InputValidator.validate() as a valid field
            # Once the botocore Stubber is updated to not have this param anymore in `create_solution` call,
            # this check can be deleted.
            if "performAutoML" in _solution:
                del _solution["performAutoML"]
                logger.error(
                    "performAutoML is not a valid configuration parameter - proceeding to create the "
                    "solution without this feature. For more details, refer to the Maintaining Personalized Experiences "
                    "Github project's README.md file."
                )

            _solution["datasetGroupArn"] = DatasetGroup().arn("validation")
            if "solutionVersion" in _solution:
                # To pass solution through InputValidator
                solution_version_config = _solution["solutionVersion"]
                del _solution["solutionVersion"]
                self._validate_resource(Solution(), _solution)
                _solution["solutionVersion"] = solution_version_config

            else:
                self._validate_resource(Solution(), _solution)

            self._fill_default_vals("solution", _solution)
            self._validate_solution_version(_solution)

    def _validate_solution_version(self, solution_config):
        allowed_sol_version_keys = ["trainingMode", "tags"]

        if "solutionVersion" not in solution_config:
            solution_config["solutionVersion"] = {}
        else:
            keys_not_allowed = set(solution_config["solutionVersion"].keys()) - set(allowed_sol_version_keys)
            if keys_not_allowed != set():
                self._configuration_errors.append(
                    f"Allowed keys for solutionVersion are: {allowed_sol_version_keys}. Unsupported key(s): {list(keys_not_allowed)}"
                )

        self._fill_default_vals("solutionVersion", solution_config["solutionVersion"])

    def _validate_recommender(self, path="recommenders[]"):
        recommenders = jmespath.search(path, self.config_dict) or {}
        for idx, recommender_config in enumerate(recommenders):
            if not self._validate_type(
                recommender_config, dict, f"recommenders[{idx}].serviceConfig must be an object"
            ):
                continue

            _recommender = recommender_config.get("serviceConfig")
            self._fill_default_vals("recommender", _recommender)

    def _validate_solution_update(self):
        invalid = (
            jmespath.search(
                "solutions[].{name: serviceConfig.name, recipe: serviceConfig.recipeArn, update: workflowConfig.schedules.update} | @[?update && (!contains(recipe, `aws-hrnn-coldstart`) && !contains(recipe, `aws-user-personalization`))].name",
                self.config_dict,
            )
            or []
        )
        for solution_name in invalid:
            self._configuration_errors.append(
                f"solution {solution_name} does not support solution version incremental updates - please use `full` instead of `update`."
            )

    def _validate_campaigns(self, path, campaigns: List[Dict]):
        for idx, campaign_config in enumerate(campaigns):
            current_path = f"{path}.campaigns[{idx}]"

            campaign = campaign_config.get("serviceConfig")
            if not self._validate_type(campaign, dict, f"{current_path}.serviceConfig must be an object"):
                continue
            else:
                campaign["solutionVersionArn"] = SolutionVersion().arn("validation")
                self._validate_resource(Campaign(), campaign)

            self._fill_default_vals("campaign", campaign)

    def _validate_batch_inference_jobs(self, path, solution_name, batch_inference_jobs: List[Dict]):
        for idx, batch_job_config in enumerate(batch_inference_jobs):
            current_path = f"{path}.batchInferenceJobs[{idx}]"

            batch_job = batch_job_config.get("serviceConfig")
            if not self._validate_type(batch_job, dict, f"{current_path}.batchInferenceJob must be an object"):
                continue
            else:
                # service does not validate the batch job length client-side
                job_name = f"batch_{solution_name}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
                if len(job_name) > 63:
                    self._configuration_errors.append(
                        f"The generated batch inference job name {job_name} is longer than 63 characters. Use a shorter solution name."
                    )

                # some values are provided by the solution - we introduce placeholders
                batch_job.update(
                    {
                        "solutionVersionArn": SolutionVersion().arn("validation"),
                        "jobName": job_name,
                        "roleArn": "roleArn",
                        "jobInput": {"s3DataSource": {"path": "s3://data-source"}},
                        "jobOutput": {"s3DataDestination": {"path": "s3://data-destination"}},
                    }
                )
                self._validate_resource(BatchInferenceJob(), batch_job)
                self._fill_default_vals("batchJob", batch_job)

    def _validate_batch_segment_jobs(self, path, solution_name, batch_segment_jobs: List[Dict]):
        for idx, batch_job_config in enumerate(batch_segment_jobs):
            current_path = f"{path}.batchSegmentJobs[{idx}]"

            batch_job = batch_job_config.get("serviceConfig")
            if not self._validate_type(batch_job, dict, f"{current_path}.batchSegmentJob must be an object"):
                continue
            else:
                # service does not validate the batch job length client-side
                job_name = f"batch_{solution_name}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}"
                if len(job_name) > 63:
                    self._configuration_errors.append(
                        f"The generated batch segment job name {job_name} is longer than 63 characters. Use a shorter solution name."
                    )

                # some values are provided by the solution - we introduce placeholders
                batch_job.update(
                    {
                        "solutionVersionArn": SolutionVersion().arn("validation"),
                        "jobName": job_name,
                        "roleArn": "roleArn",
                        "jobInput": {"s3DataSource": {"path": "s3://data-source"}},
                        "jobOutput": {"s3DataDestination": {"path": "s3://data-destination"}},
                    }
                )
                self._validate_resource(BatchSegmentJob(), batch_job)
                self._fill_default_vals("segmentJob", batch_job)

    def _validate_rate(self, expression):
        rate_re = re.compile(r"rate\((?P<value>\d+) (?P<unit>(minutes?|hours?|day?s)\))")
        match = rate_re.match(expression)

        if not match:
            self._configuration_errors.append(f"invalid rate ScheduleExpression {expression}")

    def _validate_cron_expressions(self, *paths: List[str]) -> None:  # NOSONAR - allow higher complexity
        """
        Validate all cron expressions found in paths
        :param paths: the list of jmespath paths to validate as cron expressions
        :return: None
        """
        expressions = []
        for path in paths:
            result = jmespath.search(path, self.config_dict)
            if not result:
                logger.debug(f"no schedule found at {path}")
                continue
            if isinstance(result, str):
                expressions.append(result)
            elif isinstance(result, list):
                for item in result:
                    if isinstance(item, str):
                        expressions.append(item)
                    else:
                        self._configuration_errors.append(f"unexpected type at path {path}, expected string")
            else:
                self._configuration_errors.append(f"unexpected type at path {path}, expected string or list")
        for expression in expressions:
            try:
                Schedule(expression=expression)
            except ScheduleError as exc:
                self._configuration_errors.append(str(exc))

    def _validate_not_empty(self):
        if not self.config_dict:
            self._configuration_errors.append("Configuration should not be empty")

    def _validate_datasets(self) -> None:
        """
        Perform a validation of the datasets up front
        :return: None
        """
        datasets = jmespath.search("datasets", self.config_dict)
        if not datasets:
            logger.warning("typical usage includes a dataset declaration")
            return

        datasets = {
            "users": jmespath.search("datasets.users.dataset.serviceConfig", self.config_dict),
            "items": jmespath.search("datasets.items.dataset.serviceConfig", self.config_dict),
            "interactions": jmespath.search("datasets.interactions.dataset.serviceConfig", self.config_dict),
        }

        if not datasets["interactions"]:
            self._configuration_errors.append(
                "You must at minimum create an interactions dataset and declare its schema"
            )

        for dataset_name, dataset in datasets.items():
            if dataset:
                if not self._validate_type(dataset, dict, f"datasets.{dataset_name} must be an object"):
                    return

                # some values are provided by the solution - we introduce placeholders
                dataset.update(
                    {
                        "datasetGroupArn": DatasetGroup().arn("validation"),
                        "schemaArn": Schema().arn("validation"),
                        "datasetType": dataset_name,
                    }
                )
                self._validate_resource(Dataset(), dataset)
                self._fill_default_vals("dataset", dataset)

    def _validate_dataset_import_job(self, path="datasets.serviceConfig") -> None:
        """
        Perform a validation of the dataset import fields to ensure default values are present
        :return: None
        """
        dataset_import = jmespath.search(path, self.config_dict)
        if "datasets" in self.config_dict:
            if not dataset_import:
                self.config_dict["datasets"]["serviceConfig"] = {}
                dataset_import = jmespath.search(path, self.config_dict)

            self._fill_default_vals("datasetImport", dataset_import)

    def _validate_schemas(self) -> None:
        """
        Perform a validation of the schemas up front
        :return: None
        """
        users_schema = jmespath.search("datasets.users.schema.serviceConfig", self.config_dict)
        items_schema = jmespath.search("datasets.items.schema.serviceConfig", self.config_dict)
        interactions_schema = jmespath.search("datasets.interactions.schema.serviceConfig", self.config_dict)

        self._validate_schema("users", users_schema)
        self._validate_schema("items", items_schema)
        self._validate_schema("interactions", interactions_schema)

    def _validate_schema(self, name: str, schema: Optional[Dict]) -> None:
        if not schema:
            return  # nothing to validate - schema wasn't provided

        avro_schema = schema.get("schema", {})
        avro_schema_name = schema.get("name")

        # check for schema name
        if not avro_schema_name:
            self._configuration_errors.append(f"The {name} schema name is missing")

        # check for schema
        if not avro_schema:
            self._configuration_errors.append(f"The {name} schema is missing")
        else:
            try:
                avro.schema.parse(json.dumps(avro_schema))
            except avro.schema.SchemaParseException as exc:
                self._configuration_errors.append(f"The {name} schema is not valid: {exc}")

        self._validate_resource(
            Schema(),
            {
                "schema": json.dumps(avro_schema),
                "name": avro_schema_name,
            },
        )

    def _validate_keys(self, config: Dict = None, schema: List = None, path=""):
        """
        Validate the configuration in config_dict against allowed_keys
        :param config_dict: The dictionary to validate
        :param schema: The allowed keys
        :param path: The path config_dict (used in recursion to identify a jmespath path)
        :return: None
        """
        if not config:
            config = self.config_dict
        if not schema:
            schema = self._schema

        if isinstance(config, list):
            self._validate_list(config, schema, path)
        elif isinstance(config, dict):
            self._validate_dict(config, schema, path)
        else:
            self._configuration_errors.append(f"an unknown validation error occurred at {path}")

    def _validate_tag_types(self, result, path):
        err_msg = f"Invalid type at path {path} for tags, expected list[dict]."
        is_lst = self._validate_type(result, list, err_msg)
        if isinstance(result, list) and isinstance(result[0], list):  # sometimes jmespath returns list of list instead
            result = result[0]

        if is_lst:
            for tag_instance in result:
                is_dict = self._validate_type(tag_instance, dict, err_msg)
                if path == "root":
                    if is_dict and set(tag_instance.keys()) == {"tagKey", "tagValue"}:
                        continue
                    else:
                        self._configuration_errors.append(
                            "Parameter validation failed: Tag keys must be one of: 'tagKey', 'tagValue'"
                        )
                        return False
                else:
                    return is_dict
        return False

    def _validate_root_tags(self):
        if "tags" in self.config_dict:
            self._validate_tag_types(self.config_dict["tags"], "root")

    def _validate_tags(self, *paths: List[str]):
        """
        Validate the configuration in config_dict for all tags provided.
        Ensures that the tags supplied are a list of dict, and only contain the allowed key values.
        :param paths: The paths in config_dict (used in recursion to identify a jmespath path) that may contain tags
        :return: None
        """
        for path in paths:
            result = jmespath.search(path, self.config_dict)
            if result:
                self._validate_tag_types(result, path)

    def _validate_list(self, config: List, schema: List, path=""):
        for idx, item in enumerate(config):
            current_path = f"{path}[{idx}]"
            self._validate_keys(item, schema[0], current_path)

    def _validate_dict(self, config: Dict, schema: List, path=""):
        allowed = [k if isinstance(k, str) else next(iter(k.keys())) if isinstance(k, dict) else k[0] for k in schema]
        sub_validations = [i for i in schema if isinstance(i, dict)]

        for key, value in config.items():
            current_path = [path, key]
            current_path = ".".join([i for i in current_path if i])
            if key not in allowed:
                self._configuration_errors.append(f"key {current_path} is not an allowed key")

            try:
                sub_validation = [v for v in sub_validations if v.get(key)].pop()
                self._validate_keys(value, sub_validation[key], current_path)
            except IndexError:
                pass  # no sub validations

    def _validate_no_duplicates(self, name: str, path: str):
        results = jmespath.search(path, self.config_dict)
        duplicates = get_duplicates(results)
        if duplicates:
            self._configuration_errors.append(
                f"duplicate {name} found: {', '.join(duplicates)}. Do not use the same {name} across solutions"
            )

    def _validate_naming(self):
        """Validate that names of resources don't overlap in ways that might cause issues"""
        self._validate_no_duplicates(name="campaign names", path="solutions[].campaigns[].serviceConfig.name")
        self._validate_no_duplicates(name="solution names", path="solutions[].serviceConfig.name")

    def _fill_default_vals(self, resource_type, resource_dict):
        """Insert default values for tags and other fields whenever not supplied"""

        if (
            resource_type
            in [
                "datasetGroup",
                "datasetImport",
                "dataset",
                "eventTracker",
                "solution",
                "solutionVersion",
                "filter",
                "recommender",
                "campaign",
                "batchJob",
                "segmentJob",
            ]
            and "tags" not in resource_dict
        ):
            if self.pass_root_tags:
                resource_dict["tags"] = self.config_dict["tags"]
            else:
                resource_dict["tags"] = []

        if resource_type == "datasetImport":
            if "importMode" not in resource_dict:
                resource_dict["importMode"] = "FULL"
            if "publishAttributionMetricsToS3" not in resource_dict:
                resource_dict["publishAttributionMetricsToS3"] = False

        if resource_type == "solutionVersion":
            if "tags" not in resource_dict:
                if self.pass_root_tags:
                    resource_dict["tags"] = self.config_dict["tags"]
                else:
                    resource_dict["tags"] = []
            if "trainingMode" not in resource_dict:
                resource_dict["trainingMode"] = "FULL"
