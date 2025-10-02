# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
