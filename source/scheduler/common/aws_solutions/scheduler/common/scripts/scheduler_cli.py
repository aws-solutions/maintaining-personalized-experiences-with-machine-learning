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
from typing import List, Tuple, Dict, Any

import boto3
import click
from rich import print_json

from aws_solutions.scheduler.common import Scheduler
from aws_solutions.scheduler.common.base import logger

logger.setLevel("ERROR")


def get_stack_output_value(stack, key: str) -> str:
    """
    Get a stack output value
    :param stack: the boto3 stack resource
    :param key: the output key
    :return: str
    """
    results = [i for i in stack.outputs if i["OutputKey"] == key]
    if not results:
        raise ValueError(f"could not find output with key {key} in stack")
    return results[0]["OutputValue"]


def get_stack_tag_value(stack, key: str) -> str:
    """
    Get a stack tag value
    :param stack: the boto3 stack resource
    :param key: the tag key
    :return: str
    """
    results = [i for i in stack.tags if i["Key"] == key]
    if not results:
        raise ValueError(f"could not find tag with key {key} in stack")
    return results[0]["Value"]


def get_stack_metadata_value(stack, key: str) -> str:
    """
    Get a stack template metadata value
    :param stack: the boto3 stack resource
    :param key: the metadata key
    :return: str
    """
    summary = stack.meta.client.get_template_summary(StackName=stack.name)
    metadata = json.loads(summary.get("Metadata", "{}"))
    try:
        return metadata[key]
    except KeyError:
        raise ValueError(f"could not find metadata with key {key} in stack")


def setup_cli_env(stack, region: str) -> None:
    """
    Set the environment variables required by the scheduler
    :param stack: the stack name
    :param region: the AWS region
    :return: None
    """
    os.environ["AWS_REGION"] = region
    os.environ["SOLUTION_ID"] = get_stack_metadata_value(
        stack, "aws:solutions:solution_id"
    )
    os.environ["SOLUTION_VERSION"] = get_stack_metadata_value(
        stack, "aws:solutions:solution_version"
    )


@click.group()
@click.option("-s", "--stack", required=True, envvar="SCHEDULER_STACK")
@click.option("-r", "--region", required=True, envvar="AWS_REGION")
@click.option("--scheduler-table-name-output", default="SchedulerTableName")
@click.option("--scheduler-stepfunction-arn-output", default="SchedulerStepFunctionArn")
@click.pass_context
def cli(
    ctx,
    stack: str,
    region: str,
    scheduler_table_name_output: str,
    scheduler_stepfunction_arn_output: str,
) -> None:
    """
    Scheduler CLI
    \f

    :param ctx: the click context
    :param stack: the AWS CloudFormation stack name
    :param region: the AWS region
    :param scheduler_table_name_output: the scheduler table name
    :param scheduler_stepfunction_arn_output: the scheduler step function ARN
    :return: None
    """
    ctx.ensure_object(dict)

    cloudformation = boto3.resource("cloudformation", region_name=region)
    stack = cloudformation.Stack(stack)
    try:
        setup_cli_env(stack, region)
    except ValueError as exc:
        raise click.ClickException(exc)

    ctx.obj["SCHEDULER"] = Scheduler(
        table_name=get_stack_output_value(stack, scheduler_table_name_output),
        stepfunction=get_stack_output_value(stack, scheduler_stepfunction_arn_output),
    )
    ctx.obj["REGION"] = region
    ctx.obj["STACK"] = stack


@cli.command("list")
@click.pass_context
def list_command(ctx) -> None:
    """
    List all scheduled tasks
    \f

    :param ctx: the click context
    :return: None
    """
    scheduler: Scheduler = ctx.obj["SCHEDULER"]
    tasks = []

    for task in scheduler.list():
        tasks.append(task)
    tasks = sorted(tasks)

    print_json(data={"tasks": tasks})


def _describe(ctx, task: str) -> None:
    """
    Describe a scheduled task
    :param ctx: the click context
    :param task: the task name
    :return: None
    """
    scheduler: Scheduler = ctx.obj["SCHEDULER"]

    tracker = scheduler.read(task, 0)
    latest = scheduler.read(task, int(tracker.latest))

    print_json(
        data={
            "task": {
                "active": scheduler.is_enabled(latest),
                "name": latest.name,
                "schedule": latest.schedule.expression,
                "step_function": latest.state_machine.get("arn"),
                "version": f"v{tracker.latest}",
            }
        }
    )


@cli.command()
@click.option("-t", "--task", required=True)
@click.pass_context
def describe(ctx, task: str) -> Any:
    """
    Describe a scheduled task
    \f

    :param ctx: the click context
    :param task: the task
    :return: ctx click context
    """
    return _describe(ctx, task)


@cli.command()
@click.option("-t", "--task", required=True)
@click.pass_context
def activate(ctx, task: str) -> None:
    """
    Activate a scheduled task
    \f

    :param ctx: the click context
    :param task: the task
    :return: None
    """
    scheduler: Scheduler = ctx.obj["SCHEDULER"]

    tracker = scheduler.read(task, 0)
    latest = scheduler.read(task, int(tracker.latest))

    scheduler.activate(latest)
    _describe(ctx, task)


@cli.command()
@click.option("-t", "--task", required=True)
@click.pass_context
def deactivate(ctx, task) -> None:
    """
    Deactivate a scheduled task
    \f

    :param ctx: the click context
    :param task: the task
    :return: None
    """
    scheduler: Scheduler = ctx.obj["SCHEDULER"]

    tracker = scheduler.read(task, 0)
    if not tracker:
        raise click.ClickException(f"task {task} does not exist")
    latest = scheduler.read(task, int(tracker.latest))

    scheduler.deactivate(latest)
    _describe(ctx=ctx, task=latest)


def _validate_path(ctx, param, value) -> str:
    """
    Callback to validate the path parameter
    :param ctx: the click context
    :param param: the click parameter
    :param value: the click parameter value
    :return: str
    """
    if not value.startswith("train/"):
        raise click.BadParameter("must start with 'train/")
    if not value.endswith(".json"):
        raise click.BadParameter("must end with a suffix of .json")
    return value


def _validate_schedules(ctx, param, value) -> Tuple[Tuple[str, str], ...]:
    """
    Callback to validate the schedule parameters
    :param ctx: the click context
    :param param: the click parameters
    :param value: the click parameter values
    :return: Tuple[Tuple[str, str], ...]
    """
    if len(value) == 0:
        return value

    values = []
    for idx, item in enumerate(value):
        solution, _, schedule = item.partition("@")
        if solution and schedule:
            values.append((solution, schedule))
        else:
            raise click.BadParameter(
                "format must be solution_name@schedule_expression e.g solution@cron(0 */12 * * ? *)"
            )
    return tuple(set(values))


def get_payload(
    dataset_group: str,
    import_schedule: str,
    update_schedule: List[Tuple[str, str]],
    full_schedule: List[Tuple[str, str]],
) -> Dict:
    """
    Gets the AWS Lambda Function payload for setting up schedules/ importing a dataset group into the solution
    :param dataset_group: dataset group name
    :param import_schedule: import schedule (e.g. "cron(* * * * ? *)")
    :param update_schedule: update schedules (eg. ("name","cron(* * * * ? *)))
    :param full_schedule: full schedules (eg. ("name","cron(* * * * ? *)))
    :return: Dict
    """
    payload = {
        "datasetGroupName": dataset_group,
    }
    if import_schedule:
        payload.setdefault("schedules", {})["import"] = import_schedule
    if update_schedule:
        for solution, schedule in update_schedule:
            payload.setdefault("schedules", {}).setdefault("solutions", {})[
                solution
            ] = {"update": schedule}
    if full_schedule:
        for solution, schedule in full_schedule:
            payload.setdefault("schedules", {}).setdefault("solutions", {})[
                solution
            ] = {"full": schedule}
    return payload


@cli.command()
@click.option(
    "-d", "--dataset-group", required=True, help="dataset group name to import"
)
@click.option("-p", "--path", required=True, callback=_validate_path, help="s3 key")
@click.option("-i", "--import-schedule", help="cron schedule for dataset import")
@click.option(
    "-f",
    "--full-schedule",
    multiple=True,
    callback=_validate_schedules,
    help="cron schedules for FULL solution version updates",
)
@click.option(
    "-u",
    "--update-schedule",
    multiple=True,
    callback=_validate_schedules,
    help="cron schedules for UPDATE solution version updates",
)
@click.pass_context
def import_dataset_group(
    ctx, dataset_group, path, import_schedule, full_schedule, update_schedule
):
    """
    Create a new configuration from an existing dataset group in Amazon Personalize and add scheduled tasks
    \f

    :param ctx: the click context
    :param dataset_group: the dataset group name
    :param path: the full s3 key of the configuration file
    :param import_schedule: the import cron schedule
    :param full_schedule: the full schedules
    :param update_schedule:  the update schedules
    """
    region = ctx.obj["REGION"]
    stack = ctx.obj["STACK"]
    cli_lambda = boto3.client("lambda", region_name=region)
    cli_s3 = boto3.client("s3", region_name=region)
    cli_sts = boto3.client("sts", region_name=region)
    config_function = get_stack_output_value(stack, "CreateConfigFunctionArn")
    bucket = get_stack_output_value(stack, "PersonalizeBucketName")
    account = cli_sts.get_caller_identity()["Account"]

    payload = get_payload(
        dataset_group=dataset_group,
        import_schedule=import_schedule,
        update_schedule=update_schedule,
        full_schedule=full_schedule,
    )

    # Run the lambda function to generate the configuration and get the result
    result = cli_lambda.invoke(
        FunctionName=config_function,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    status = result.get("StatusCode")
    if status != 200:
        raise click.ClickException(
            "there was an error generating configuration ({status})"
        )
    if result.get("FunctionError"):
        error_message = json.loads(result.get("Payload").read()).get("errorMessage")
        raise click.ClickException(
            f"Could not generate configuration for {dataset_group}: {error_message}"
        )

    # to trigger the workflow and set up new schedules, upload the returned configuration to S3.
    cli_s3.upload_fileobj(
        Fileobj=result.get("Payload"),
        Bucket=bucket,
        Key=path,
        ExtraArgs={"ExpectedBucketOwner": account},
    )


if __name__ == "__main__":
    cli()
