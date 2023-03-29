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

from datetime import datetime, timedelta

import pytest
from aws_lambda.create_campaign.handler import CONFIG, RESOURCE, STATUS, lambda_handler
from botocore.exceptions import ParamValidationError
from dateutil.parser import isoparse
from dateutil.tz import tzlocal
from moto import mock_sts
from shared.exceptions import ResourcePending
from shared.resource import Campaign, SolutionVersion


def test_create_campaign(validate_handler_config):
    for status in STATUS.split("||"):
        status = status.strip()
        validate_handler_config(RESOURCE, CONFIG, status)
    with pytest.raises(ValueError):
        lambda_handler({}, None)


@mock_sts
def test_describe_campaign_response(personalize_stubber, notifier_stubber):
    campaign_name = "mockCampaign"
    sv_arn = SolutionVersion().arn("unit_test", sv_id="12345678")
    personalize_stubber.add_response(
        method="describe_campaign",
        service_response={
            "campaign": {
                "campaignArn": Campaign().arn(campaign_name),
                "name": campaign_name,
                "solutionVersionArn": sv_arn,
                "minProvisionedTPS": 1,
                "status": "ACTIVE",
                "lastUpdatedDateTime": datetime.now(tzlocal()),
                "creationDateTime": datetime.now(tz=tzlocal()) - timedelta(seconds=100),
            }
        },
        expected_params={"campaignArn": Campaign().arn(campaign_name)},
    )

    result = lambda_handler(
        {
            "serviceConfig": {
                "name": campaign_name,
                "solutionVersionArn": sv_arn,
                "minProvisionedTPS": 1,
                "tags": [{"tagKey": "campaign-1", "tagValue": "campaign-key-1"}],
            },
            "workflowConfig": {
                "maxAge": "365 days",
                "timeStarted": "2021-10-19T15:18:32Z",
            },
        },
        None,
    )

    assert notifier_stubber.has_notified_for_complete
    assert notifier_stubber.latest_notification_status == "ACTIVE"


@mock_sts
def test_create_campaign_response(personalize_stubber, notifier_stubber):
    campaign_name = "mockCampaign"
    sv_arn = SolutionVersion().arn("unit_test", sv_id="12345678")
    personalize_stubber.add_client_error(
        method="describe_campaign",
        service_error_code="ResourceNotFoundException",
        expected_params={"campaignArn": Campaign().arn(campaign_name)},
    )
    personalize_stubber.add_response(
        method="create_campaign",
        expected_params={
            "name": campaign_name,
            "minProvisionedTPS": 1,
            "solutionVersionArn": sv_arn,
        },
        service_response={"campaignArn": Campaign().arn(campaign_name)},
    )

    with pytest.raises(ResourcePending):
        lambda_handler(
            {
                "serviceConfig": {
                    "name": campaign_name,
                    "solutionVersionArn": sv_arn,
                    "minProvisionedTPS": 1,
                },
                "workflowConfig": {
                    "maxAge": "365 days",
                    "timeStarted": "2021-10-19T15:18:32Z",
                },
            },
            None,
        )

    assert notifier_stubber.has_notified_for_creation
    assert notifier_stubber.latest_notification_status == "CREATING"


@mock_sts
def test_update_campaign_start(personalize_stubber, notifier_stubber):
    campaign_name = "mockCampaign"
    sv_arn_old = SolutionVersion().arn("unit_test", sv_id="12345678")
    sv_arn_new = SolutionVersion().arn("unit_test", sv_id="01234567")
    personalize_stubber.add_response(
        method="describe_campaign",
        service_response={
            "campaign": {
                "campaignArn": Campaign().arn(campaign_name),
                "name": campaign_name,
                "solutionVersionArn": sv_arn_old,
                "minProvisionedTPS": 1,
                "status": "ACTIVE",
                "lastUpdatedDateTime": datetime.now(tzlocal()),
                "creationDateTime": datetime.now(tz=tzlocal()) - timedelta(seconds=100),
            }
        },
        expected_params={"campaignArn": Campaign().arn(campaign_name)},
    )
    personalize_stubber.add_response(
        method="update_campaign",
        service_response={
            "campaignArn": Campaign().arn(campaign_name),
        },
        expected_params={
            "campaignArn": Campaign().arn(campaign_name),
            "minProvisionedTPS": 1,
            "solutionVersionArn": sv_arn_new,
        },
    )

    with pytest.raises(ResourcePending):
        lambda_handler(
            {
                "serviceConfig": {"name": campaign_name, "solutionVersionArn": sv_arn_new, "minProvisionedTPS": 1},
                "workflowConfig": {
                    "maxAge": "365 days",
                    "timeStarted": "2021-10-19T15:18:32Z",
                },
            },
            None,
        )

    assert notifier_stubber.has_notified_for_creation
    assert notifier_stubber.latest_notification_status == "UPDATING"


@mock_sts
def test_describe_campaign_response_updating(personalize_stubber, notifier_stubber):
    campaign_name = "mockCampaign"
    sv_arn_old = SolutionVersion().arn("unit_test", sv_id="12345678")
    sv_arn_new = SolutionVersion().arn("unit_test", sv_id="01234567")
    personalize_stubber.add_response(
        method="describe_campaign",
        service_response={
            "campaign": {
                "campaignArn": Campaign().arn(campaign_name),
                "name": campaign_name,
                "solutionVersionArn": sv_arn_old,
                "minProvisionedTPS": 1,
                "status": "ACTIVE",
                "lastUpdatedDateTime": datetime.now(tzlocal()) - timedelta(seconds=1000),
                "creationDateTime": datetime.now(tz=tzlocal()) - timedelta(seconds=1100),
                "latestCampaignUpdate": {
                    "minProvisionedTPS": 1,
                    "solutionVersionArn": sv_arn_new,
                    "creationDateTime": datetime.now(tzlocal()),
                    "lastUpdatedDateTime": datetime.now(tzlocal()),
                    "status": "UPDATE IN_PROGRESS",
                },
            }
        },
        expected_params={"campaignArn": Campaign().arn(campaign_name)},
    )
    personalize_stubber.add_client_error(
        method="update_campaign",
        service_error_code="ResourceInUseException",
    )

    with pytest.raises(ResourcePending):
        lambda_handler(
            {
                "serviceConfig": {"name": campaign_name, "solutionVersionArn": sv_arn_new, "minProvisionedTPS": 1},
                "workflowConfig": {
                    "maxAge": "365 days",
                    "timeStarted": "2021-10-19T15:18:32Z",
                },
            },
            None,
        )

    assert not notifier_stubber.has_notified_for_complete
    assert not notifier_stubber.has_notified_for_creation


@mock_sts
def test_describe_campaign_response_updated(personalize_stubber, notifier_stubber):
    campaign_name = "mockCampaign"
    sv_arn_new = SolutionVersion().arn("unit_test", sv_id="01234567")
    personalize_stubber.add_response(
        method="describe_campaign",
        service_response={
            "campaign": {
                "campaignArn": Campaign().arn(campaign_name),
                "name": campaign_name,
                "solutionVersionArn": sv_arn_new,
                "minProvisionedTPS": 1,
                "status": "ACTIVE",
                "lastUpdatedDateTime": datetime.now(tzlocal()) - timedelta(seconds=1000),
                "creationDateTime": datetime.now(tz=tzlocal()) - timedelta(seconds=1100),
                "latestCampaignUpdate": {
                    "minProvisionedTPS": 1,
                    "solutionVersionArn": sv_arn_new,
                    "creationDateTime": datetime.now(tzlocal()) - timedelta(seconds=100),
                    "lastUpdatedDateTime": datetime.now(tzlocal()),
                    "status": "ACTIVE",
                },
            }
        },
        expected_params={"campaignArn": Campaign().arn(campaign_name)},
    )

    result = lambda_handler(
        {
            "serviceConfig": {
                "name": campaign_name,
                "solutionVersionArn": sv_arn_new,
                "minProvisionedTPS": 1,
                "tags": [{"tagKey": "campaign-1", "tagValue": "campaign-key-1"}],
            },
            "workflowConfig": {
                "maxAge": "365 days",
                "timeStarted": "2021-10-19T15:18:32Z",
            },
        },
        None,
    )

    assert notifier_stubber.has_notified_for_complete
    assert not notifier_stubber.has_notified_for_creation
    assert notifier_stubber.latest_notification_status == "ACTIVE"

    last_updated = isoparse(notifier_stubber.get_resource_last_updated(Campaign(), {"campaign": result}))
    created = isoparse(notifier_stubber.get_resource_created(Campaign(), {"campaign": result}))
    assert (last_updated - created).seconds == 100


@mock_sts
def test_bad_campaign_tags(personalize_stubber, notifier_stubber):
    campaign_name = "mockCampaign"
    sv_arn_new = SolutionVersion().arn("unit_test", sv_id="01234567")
    personalize_stubber.add_response(
        method="describe_campaign",
        service_response={
            "campaign": {
                "campaignArn": Campaign().arn(campaign_name),
                "name": campaign_name,
                "solutionVersionArn": sv_arn_new,
                "minProvisionedTPS": 1,
                "status": "ACTIVE",
                "lastUpdatedDateTime": datetime.now(tzlocal()) - timedelta(seconds=1000),
                "creationDateTime": datetime.now(tz=tzlocal()) - timedelta(seconds=1100),
                "latestCampaignUpdate": {
                    "minProvisionedTPS": 1,
                    "solutionVersionArn": sv_arn_new,
                    "creationDateTime": datetime.now(tzlocal()) - timedelta(seconds=100),
                    "lastUpdatedDateTime": datetime.now(tzlocal()),
                    "status": "ACTIVE",
                },
            }
        },
        expected_params={"campaignArn": Campaign().arn(campaign_name)},
    )

    try:
        lambda_handler(
            {
                "serviceConfig": {
                    "name": campaign_name,
                    "solutionVersionArn": sv_arn_new,
                    "minProvisionedTPS": 1,
                    "tags": "bad data",
                },
                "workflowConfig": {
                    "maxAge": "365 days",
                    "timeStarted": "2021-10-19T15:18:32Z",
                },
            },
            None,
        )
    except ParamValidationError as exp:
        assert (
            exp.kwargs["report"]
            == "Invalid type for parameter tags, value: bad data, type: <class 'str'>, valid types: <class 'list'>, <class 'tuple'>"
        )
