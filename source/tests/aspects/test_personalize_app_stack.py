#!/usr/bin/env python3

# #####################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                 #
#                                                                                                                     #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. You may obtain a copy of the License at                                                          #
#                                                                                                                     #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                        #
#                                                                                                                     #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed   #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for  #
#  the specific language governing permissions and limitations under the License.                                     #
# #####################################################################################################################

import pytest
from pathlib import Path
import aws_cdk as cdk
from aws_cdk.assertions import Template, Capture
from aws_solutions.cdk import CDKSolution

solution = CDKSolution(cdk_json_path=Path(__file__).parent.parent.absolute() / "infrastructure" / "cdk.json")

from infrastructure.personalize.stack import PersonalizeStack
from infrastructure.aspects.app_registry import AppRegistry


@pytest.fixture(scope="module")
def synth_template(cdk_json):
    app = cdk.App(context=cdk_json["context"])

    stack = PersonalizeStack(
        app,
        "PersonalizeStack",
        description=f"Deploy, deliver and maintain personalized experiences with Amazon Personalize",
        template_filename="maintaining-personalized-experiences-with-machine-learning.template",
        synthesizer=solution.synthesizer,
    )
    cdk.Aspects.of(app).add(AppRegistry(stack, "AppRegistryAspect"))
    template = Template.from_stack(stack)
    yield template, app


app_registry_capture = Capture()


def test_service_catalog_registry_application(synth_template):
    template, app = synth_template
    template.resource_count_is("AWS::ServiceCatalogAppRegistry::Application", 1)
    template.has_resource_properties(
        "AWS::ServiceCatalogAppRegistry::Application",
        {
            "Name": {
                "Fn::Join": [
                    "-",
                    [
                        {"Ref": "AWS::StackName"},
                        app.node.try_get_context("APP_REGISTRY_NAME"),
                        {"Ref": "AWS::Region"},
                        {"Ref": "AWS::AccountId"},
                    ],
                ]
            },
            "Description": "Service Catalog application to track and manage all your resources for the solution Maintaining Personalized Experiences with Machine Learning",
            "Tags": {
                "SOLUTION_ID": "SO0170",
                "SOLUTION_NAME": "Maintaining Personalized Experiences with Machine Learning",
                "SOLUTION_VERSION": "v1.3.0",
                "Solutions:ApplicationType": "AWS-Solutions",
                "Solutions:SolutionID": "SO0170",
                "Solutions:SolutionName": "Maintaining Personalized Experiences with Machine Learning",
                "Solutions:SolutionVersion": "v1.3.0",
            },
        },
    )


def test_resource_association_nested_stacks(synth_template):
    template, app = synth_template
    template.resource_count_is("AWS::ServiceCatalogAppRegistry::ResourceAssociation", 1)
    template.has_resource_properties(
        "AWS::ServiceCatalogAppRegistry::ResourceAssociation",
        {
            "Application": {"Fn::GetAtt": [app_registry_capture, "Id"]},
            "Resource": {"Ref": "AWS::StackId"},
            "ResourceType": "CFN_STACK",
        },
    )


def test_attr_grp_association(synth_template):
    attr_grp_capture = Capture()
    template, app = synth_template
    template.resource_count_is("AWS::ServiceCatalogAppRegistry::AttributeGroupAssociation", 1)
    template.has_resource_properties(
        "AWS::ServiceCatalogAppRegistry::AttributeGroupAssociation",
        {
            "Application": {"Fn::GetAtt": [app_registry_capture.as_string(), "Id"]},
            "AttributeGroup": {"Fn::GetAtt": [attr_grp_capture, "Id"]},
        },
    )

    assert (
        template.to_json()["Resources"][attr_grp_capture.as_string()]["Type"]
        == "AWS::ServiceCatalogAppRegistry::AttributeGroup"
    )
