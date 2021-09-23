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
from aws_cdk.core import App, Stack

from aws_solutions.cdk.mappings import Mappings


@pytest.mark.parametrize("send_data,result", [(True, "Yes"), (False, "No")])
def test_mappings(send_data, result):
    solution_id = "SO001"
    app = App()
    stack = Stack(app)
    Mappings(stack, solution_id=solution_id, send_anonymous_usage_data=send_data)

    template = app.synth().stacks[0].template

    assert template["Mappings"]["Solution"]["Data"]["ID"] == solution_id
    assert template["Mappings"]["Solution"]["Data"]["Version"] == "%%SOLUTION_VERSION%%"
    assert template["Mappings"]["Solution"]["Data"]["SendAnonymousUsageData"] == result

    assert (
        template["Mappings"]["SourceCode"]["General"]["S3Bucket"] == "%%BUCKET_NAME%%"
    )
    assert (
        template["Mappings"]["SourceCode"]["General"]["KeyPrefix"]
        == "%%SOLUTION_NAME%%/%%SOLUTION_VERSION%%"
    )
