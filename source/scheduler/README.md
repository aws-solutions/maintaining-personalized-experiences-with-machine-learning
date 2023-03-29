# AWS Solutions Step Functions Scheduler

## Scheduling for AWS Step Functions

This tooling adds scheduling support for AWS Step Functions via a set of libraries and CDK packages.

This README summarizes using the scheduler.

## Prerequisites

Install this package. It requires at least:

- Python 3.9
- AWS CDK version 2.44.0 or higher

To install the packages:

```
pip install <path>/scheduler/cdk    # where <path> is the path to the scheduler namespace package
pip install <path>/scheduler/common # where <path> is the path to the scheduler namespace package
```

## 1. Add the scheduler to your CDK application

```python
from pathlib import Path

from constructs import Construct
from aws_cdk.aws_stepfunctions import StateMachine

from aws_solutions.cdk import CDKSolution
from aws_solutions.cdk.stack import SolutionStack
from aws_solutions.scheduler.cdk.construct import Scheduler


solution = CDKSolution(cdk_json_path=Path(__file__).parent.absolute() / "cdk.json")


class MyStack(SolutionStack):
    def __init__(self, scope: Construct, construct_id: str, description: str, template_filename, state_machine: StateMachine, **kwargs):
        super().__init__(scope, construct_id, description, template_filename, **kwargs)

        scheduler = Scheduler(self, "Scheduler")
        scheduler.grant_invoke(state_machine)
```

## 2. Allow a state machine to create new schedules

You may have an existing `StateMachine` you wish to create schedules with - to do so, use the `SchedulerFragment`.

```python
# creates a scheduled item called "my-schedule-suffix" - typically you will use part of the state input for the suffix.
SchedulerFragment(
    self,
    schedule_for="my schedule",
    schedule_for_suffix="suffix",
    scheduler=scheduler,
    target=state_machine,
    schedule_path="$.path.to.cron.expression",
    schedule_input={
        "static_input": "value",
        "derived_input.$": "$.field_in_state_input",
    },
)
```

# 3. Check the status of schedules using the included CLI

This package also provides a CLI `aws-solutions-scheduler`. This CLI can be used to control the scheduler and establish
schedules for the [Maintaining Personalized Experiences with Machine Learning](https://aws.amazon.com/solutions/implementations/maintaining-personalized-experiences-with-ml/)
solution.

### Installation

It is recommended that you perform the following steps in a dedicated virtual environment:

```shell
cd source
pip install --upgrade pip
pip install cdk_solution_helper_py/helpers_common
pip install scheduler/common
```

### Usage

```shell
Usage: aws-solutions-scheduler [OPTIONS] COMMAND [ARGS]...

  Scheduler CLI

Options:
  -s, --stack TEXT                [required]
  -r, --region TEXT               [required]
  --scheduler-table-name-output TEXT
  --scheduler-stepfunction-arn-output TEXT
  --help                          Show this message and exit.

Commands:
  activate              Activate a scheduled task
  deactivate            Deactivate a scheduled task
  describe              Describe a scheduled task
  import-dataset-group  Create a new configuration from an existing...
  list                  List all scheduled tasks
```

#### Create new schedule(s) for an Amazon Personalize dataset group

Schedules for dataset import, solution version FULL and UPDATE retraining can be established using the CLI for dataset
groups in Amazon Personalize. This example creates a weekly schedule for full dataset import (`-i`) and for full
solution version retraining (-f)

```shell
> aws-solutions-scheduler -s PersonalizeStack -r us-east-1 import-dataset-group -d item-recommender -i "cron(0 0 ? * 1 *)" -f "item-recommender-user-personalization@cron(0 3 ? * 1 *)" -p train/item-recommender/config.json
```

#### Listing Schedules

```shell
> aws-solutions-scheduler -s PersonalizeStack -r us-east-1 list
```

<details>
<summary>See sample result</summary>

```json
{
	"tasks": [
		"personalize-dataset-import-item-recommender",
		"solution-maintenance-full-item-recommender-user-personalization"
	]
}
```

</details>

#### Describing Schedules

```shell
> aws-solutions-scheduler -s PersonalizeStack -r us-east-1 describe --task personalize-dataset-import-item-recommender
```

<details>
<summary>See sample result</summary>

```json
{
	"task": {
		"active": true,
		"name": "personalize-dataset-import-item-recommender",
		"schedule": "cron(*/15 * * * ? *)",
		"step_function": "arn:aws:states:us-east-1:111122223333:stateMachine:personalizestack-periodic-dataset-import-aaaaaaaaaaaa",
		"version": "v1"
	}
}
```

</details>

#### Activating Schedules

Deactivate schedules can be activated

```shell
> aws-solutions-scheduler -s PersonalizeStack -r us-east-1 activate --task personalize-dataset-import-item-recommender
```

<details>
<summary>See sample result</summary>

```json
{
	"task": {
		"active": true,
		"name": "personalize-dataset-import-item-recommender",
		"schedule": "cron(0 0 ? * 1 *)",
		"step_function": "arn:aws:states:us-east-1:111122223333:stateMachine:personalizestack-periodic-dataset-import-aaaaaaaaaaaa",
		"version": "v1"
	}
}
```

</details>

#### Deactivating Schedules

Deactivate schedules can be activated

```shell
> aws-solutions-scheduler -s PersonalizeStack -r us-east-1 deactivate --task personalize-dataset-import-item-recommender
```

<details>
<summary>See sample result</summary>

```json
{
	"task": {
		"active": false,
		"name": "personalize-dataset-import-item-recommender",
		"schedule": "cron(0 0 ? * 1 *)",
		"step_function": "arn:aws:states:us-east-1:111122223333:stateMachine:personalizestack-periodic-dataset-import-aaaaaaaaaaaa",
		"version": "v1"
	}
}
```

</details>

---

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
