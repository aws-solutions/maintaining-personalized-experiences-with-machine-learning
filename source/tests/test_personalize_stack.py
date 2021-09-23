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

import aws_cdk.core as cdk

from infrastructure.personalize.stack import PersonalizeStack


def test_personalize_stack_email(solution):
    app = cdk.App(context=solution.context)
    PersonalizeStack(
        app,
        "PersonalizeStack",
        description="meta-stack",
        template_filename="maintaining-personalized-experiences-with-machine-learning.template",
    )
    synth = app.synth()

    # ensure the email parameter is present
    assert synth.get_stack("PersonalizeStack").template["Parameters"]["Email"]
