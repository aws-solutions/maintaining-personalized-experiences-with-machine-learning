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

import pytest
from aws_cdk import App
from aws_solutions.cdk.synthesizers import SolutionStackSubstitutions
from infrastructure.personalize.stack import PersonalizeStack


@pytest.fixture
def emails_context():
    yield {
        "SOLUTION_NAME": "Maintaining Personalized Experiences with Machine Learning",
        "SOLUTION_ID": "99.99.99",
        "SOLUTION_VERSION": "SO0170test",
        "APP_REGISTRY_NAME": "personalized-experiences-ML",
        "APPLICATION_TYPE": "AWS-Solutions",
        "@aws-cdk/aws-s3:serverAccessLogsUseBucketPolicy": True,
        "BUCKET_NAME": "test-solution-bucket",
    }


def test_personalize_stack_email(solution, emails_context, monkeypatch):
    app = App(context=emails_context)
    
    PersonalizeStack(
        app,
        "PersonalizeStack",
        description="meta-stack",
        template_filename="maintaining-personalized-experiences-with-machine-learning-test.template",
        synthesizer=solution.synthesizer,
    )
    synth = app.synth()

    # ensure the email parameter is present
    assert synth.get_stack_by_name("PersonalizeStack").template["Parameters"]["Email"]
