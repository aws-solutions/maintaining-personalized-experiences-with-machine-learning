from typing import List

from aws_cdk.aws_stepfunctions import (
    StateMachineFragment,
    Chain,
    Parallel,
    JsonPath,
    State,
    INextable,
)
from aws_cdk.core import Construct

from personalize.aws_lambda.functions import (
    CreateSchema,
    CreateDataset,
    CreateDatasetImportJob,
)
from personalize.step_functions.dataset_import_fragment import DatasetImportFragment


class DatasetImportsFragment(StateMachineFragment):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        create_schema: CreateSchema,
        create_dataset: CreateDataset,
        create_dataset_import_job: CreateDatasetImportJob,
    ):
        super().__init__(scope, construct_id)

        dataset_management_functions = {
            "create_schema": create_schema,
            "create_dataset": create_dataset,
            "create_dataset_import_job": create_dataset_import_job,
        }

        self.chain = Chain.start(
            Parallel(self, "Create and Import Datasets", result_path=JsonPath.DISCARD)
            .branch(
                DatasetImportFragment(
                    self, "Interactions", **dataset_management_functions
                )
            )
            .branch(
                DatasetImportFragment(self, "Users", **dataset_management_functions)
            )
            .branch(
                DatasetImportFragment(self, "Items", **dataset_management_functions)
            )
        )

    @property
    def start_state(self) -> State:
        return self.chain.start_state

    @property
    def end_states(self) -> List[INextable]:
        return self.chain.end_states
