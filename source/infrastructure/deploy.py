#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path

import aws_cdk as cdk
from aws_solutions.cdk import CDKSolution
from cdk_nag import AwsSolutionsChecks, NagPackSuppression, NagSuppressions
from personalize.stack import PersonalizeStack

logger = logging.getLogger("cdk-helper")
solution = CDKSolution(cdk_json_path=Path(__file__).parent.absolute() / "cdk.json")


@solution.context.requires("SOLUTION_NAME")
@solution.context.requires("SOLUTION_ID")
@solution.context.requires("SOLUTION_VERSION")
@solution.context.requires("BUCKET_NAME")
def build_app(context):
    app = cdk.App(context=context)
    stack = PersonalizeStack(
        app,
        "PersonalizeStack",
        description=f"Maintaining Personalized Experiences with Machine Learning",
        template_filename="maintaining-personalized-experiences-with-machine-learning.template",
        synthesizer=solution.synthesizer,
    )
    cdk.Aspects.of(app).add(AwsSolutionsChecks(verbose=True))

    NagSuppressions.add_stack_suppressions(
        stack,
        [
            NagPackSuppression(
                id="AwsSolutions-L1", reason="Python lambda runtime is maintained at version 3.11 as a stable version."
            )
        ],
    )

    return app.synth()


if __name__ == "__main__":
    build_app()
