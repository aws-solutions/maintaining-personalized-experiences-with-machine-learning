# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
