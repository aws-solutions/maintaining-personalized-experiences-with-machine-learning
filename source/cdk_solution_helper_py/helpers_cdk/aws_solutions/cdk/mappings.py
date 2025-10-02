# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from aws_cdk import CfnMapping
from constructs import Construct


class Mappings:
    def __init__(
        self,
        parent: Construct,
        solution_id: str,
        send_anonymous_usage_data: bool = True,
        quicksight_template_arn: bool = False,
    ):
        self.parent = parent

        # Track the solution mapping (ID, version, anonymous usage data)
        self.solution_mapping = CfnMapping(
            parent,
            "Solution",
            mapping={
                "Data": {
                    "ID": solution_id,
                    "Version": "%%SOLUTION_VERSION%%",
                    "SendAnonymousUsageData": "Yes" if send_anonymous_usage_data else "No",
                    "SolutionName": "%%SOLUTION_NAME%%",
                    "ApplicationType": "AWS-Solutions",
                }
            },
            lazy=False,
        )

        # track the s3 bucket, key prefix and (optional) quicksight template source
        general = {"S3Bucket": "%%BUCKET_NAME%%", "KeyPrefix": "%%SOLUTION_NAME%%/%%SOLUTION_VERSION%%"}
        if quicksight_template_arn:
            general["QuickSightSourceTemplateArn"] = "%%QUICKSIGHT_SOURCE%%"

        self.source_mapping = CfnMapping(
            parent,
            "SourceCode",
            mapping={"General": general},
            lazy=False,
        )
