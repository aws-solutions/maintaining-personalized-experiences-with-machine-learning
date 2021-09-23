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

from pathlib import Path

import pytest

from aws_solutions.cdk.helpers import copytree


@pytest.fixture(scope="function")
def dir_to_copy(tmp_path):
    Path(tmp_path / "exists" / "sub1" / "sub2").mkdir(parents=True)
    Path(tmp_path / "exists" / "sub1" / "sub1_f").touch()
    Path(tmp_path / "exists" / "sub1" / "sub2")
    Path(tmp_path / "exists" / "sub1" / "sub2", "sub2_f").touch()
    Path(tmp_path / "exists" / "subroot_f").touch()
    Path(tmp_path / "other" / "sub3").mkdir(parents=True)
    Path(tmp_path / "other" / "sub3" / "sub3_f").touch()

    yield tmp_path


def test_copytree_dir_exists(dir_to_copy):
    Path(dir_to_copy / "new").mkdir()
    copytree(src=dir_to_copy / "exists", dst=dir_to_copy / "new")

    assert Path(dir_to_copy / "new" / "sub1" / "sub1_f").exists()
    assert Path(dir_to_copy / "new" / "sub1" / "sub2" / "sub2_f").exists()
    assert Path(dir_to_copy / "new" / "subroot_f").exists()


def test_copytree_dir_does_not_exist(dir_to_copy):
    copytree(src=dir_to_copy / "exists", dst=dir_to_copy / "new")
    copytree(src=dir_to_copy / "other", dst=dir_to_copy / "new")

    assert Path(dir_to_copy / "new" / "sub1" / "sub1_f").exists()
    assert Path(dir_to_copy / "new" / "sub1" / "sub2" / "sub2_f").exists()
    assert Path(dir_to_copy / "new" / "subroot_f").exists()
    assert Path(dir_to_copy / "new" / "sub3" / "sub3_f").exists()


def test_copytree_globs(dir_to_copy):
    copytree(
        src=dir_to_copy / "exists",
        dst=dir_to_copy / "new",
        ignore=["**/sub2/*", "subroot_f"],
    )

    assert not (Path(dir_to_copy) / "new" / "subroot_f").exists()
    assert (Path(dir_to_copy) / "new" / "sub1").exists()
    assert (Path(dir_to_copy) / "new" / "sub1" / "sub1_f").exists()
    assert not (Path(dir_to_copy) / "new" / "sub1" / "sub2").exists()
