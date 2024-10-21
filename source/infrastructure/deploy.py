#!/usr/bin/env python3

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

import logging
from pathlib import Path

import aws_cdk as cdk
from aspects.app_registry import AppRegistry
from aws_solutions.cdk import CDKSolution
from personalize.stack import PersonalizeStack
from cdk_nag import AwsSolutionsChecks
from cdk_nag import NagSuppressions
from cdk_nag import NagPackSuppression

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

    cdk.Aspects.of(app).add(AppRegistry(stack, "AppRegistryAspect"))
    cdk.Aspects.of(app).add(AwsSolutionsChecks(verbose=True))

    NagSuppressions.add_stack_suppressions(stack,
                                           [
                                            NagPackSuppression(id='AwsSolutions-L1',
                                                               reason='Python lambda runtime is planned to be '
                                                                      'upgraded in future release.')
                                            ])

    return app.synth()


if __name__ == "__main__":
    build_app()
