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

extra_context = "EXTRA_CONTEXT"
source_bucket = "SOURCE_BUCKET"


@pytest.fixture
def build_stacks_for_buckets():
    """Ensure parameter ordering is kept"""
    from deploy import build_app
    from deploy import solution as cdk_solution

    cdk_solution.reset()

    synth = build_app({extra_context: extra_context, "BUCKET_NAME": source_bucket})
    stack = synth.get_stack_by_name("PersonalizeStack").template
    yield stack


def test_deploy(solution, build_stacks_for_buckets):
    stack = build_stacks_for_buckets
    assert solution.id in stack["Description"]
    assert source_bucket == stack["Mappings"]["SourceCode"]["General"]["S3Bucket"]
    assert solution.id == stack["Mappings"]["Solution"]["Data"]["ID"]
    assert "Yes" == stack["Mappings"]["Solution"]["Data"]["SendAnonymousUsageData"]
    assert stack["Outputs"]["PersonalizeBucketName"]
    assert stack["Outputs"]["SchedulerTableName"]
    assert stack["Outputs"]["SNSTopicArn"]


def test_parameters(build_stacks_for_buckets):
    stack = build_stacks_for_buckets
    assert (
        stack["Metadata"]["AWS::CloudFormation::Interface"]["ParameterGroups"][0]["Label"]["default"]
        == "Solution Configuration"
    )
    assert stack["Metadata"]["AWS::CloudFormation::Interface"]["ParameterGroups"][0]["Parameters"] == ["Email"]
    assert stack["Metadata"]["AWS::CloudFormation::Interface"]["ParameterLabels"]["Email"]["default"] == "Email"
    assert (
        stack["Metadata"]["AWS::CloudFormation::Interface"]["ParameterLabels"]["PersonalizeKmsKeyArn"]["default"]
        == "(Optional) KMS key ARN used to encrypt Datasets managed by Amazon Personalize"
    )


def test_personalize_bucket(build_stacks_for_buckets):
    stack = build_stacks_for_buckets
    personalize_bucket = stack["Resources"]["PersonalizeBucket"]

    # Personalize bucket
    assert personalize_bucket["Type"] == "AWS::S3::Bucket"
    assert (
        personalize_bucket["Properties"]["LoggingConfiguration"]["DestinationBucketName"]["Ref"] == "AccessLogsBucket"
    )
    assert (
        personalize_bucket["Properties"]["LoggingConfiguration"]["LogFilePrefix"] == "personalize-bucket-access-logs/"
    )
    assert personalize_bucket["Properties"]["BucketEncryption"] == {
        "ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
    }

    assert personalize_bucket["Properties"]["PublicAccessBlockConfiguration"]["BlockPublicAcls"] == True
    assert personalize_bucket["Properties"]["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] == True
    assert personalize_bucket["Properties"]["PublicAccessBlockConfiguration"]["IgnorePublicAcls"] == True
    assert personalize_bucket["Properties"]["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"] == True


def test_access_logs_bucket(build_stacks_for_buckets):
    stack = build_stacks_for_buckets
    access_logs_bucket = stack["Resources"]["AccessLogsBucket"]
    assert access_logs_bucket["Type"] == "AWS::S3::Bucket"
    assert access_logs_bucket["Properties"]["BucketEncryption"] == {
        "ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
    }
    assert access_logs_bucket["Properties"]["PublicAccessBlockConfiguration"]["BlockPublicAcls"] == True
    assert access_logs_bucket["Properties"]["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] == True
    assert access_logs_bucket["Properties"]["PublicAccessBlockConfiguration"]["IgnorePublicAcls"] == True
    assert access_logs_bucket["Properties"]["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"] == True

    bucket_policy = None
    for resource, value in stack["Resources"].items():
        if "AccessLogsBucketPolicy" in resource:
            bucket_policy = value
            break

    assert bucket_policy["Type"] == "AWS::S3::BucketPolicy"

    access_logs_policy_statements = bucket_policy["Properties"]["PolicyDocument"]["Statement"]
    assert len(access_logs_policy_statements) == 2

    for policy in access_logs_policy_statements:
        if "Sid" in policy and policy["Sid"] == "HttpsOnly":
            assert policy["Principal"] == {"AWS": "*"}
            assert policy["Action"] == "*"
            assert policy["Condition"]["Bool"]["aws:SecureTransport"] == False
            assert policy["Effect"] == "Deny"
            assert policy["Resource"] == {"Fn::Join": ["", [{"Fn::GetAtt": ["AccessLogsBucket", "Arn"]}, "/*"]]}

        else:
            assert policy["Principal"] == {"Service": "logging.s3.amazonaws.com"}
            assert policy["Action"] == "s3:PutObject"
            assert policy["Condition"] == {
                "ArnLike": {"aws:SourceArn": {"Fn::GetAtt": ["PersonalizeBucket", "Arn"]}},
                "StringEquals": {"aws:SourceAccount": {"Ref": "AWS::AccountId"}},
            }
            assert policy["Effect"] == "Allow"
            assert policy["Resource"] == {
                "Fn::Join": ["", [{"Fn::GetAtt": ["AccessLogsBucket", "Arn"]}, "/personalize-bucket-access-logs/*"]]
            }
