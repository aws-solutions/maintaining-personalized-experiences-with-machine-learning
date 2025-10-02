# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest

from aws_solutions.cdk.tools import Cleaner
from aws_solutions.cdk.tools.cleaner import Cleanable


@pytest.fixture
def directory_to_clean(tmp_path):
    build_s3_assets = tmp_path / "build-s3-assets"
    global_s3_assets = tmp_path / "global-s3-assets"
    regional_s3_assets = tmp_path / "regional-s3-assets"

    build_s3_assets.mkdir()
    global_s3_assets.mkdir()
    regional_s3_assets.mkdir()

    build_asset = build_s3_assets / "build_asset"
    global_asset = global_s3_assets / "global_asset"
    regional_asset = regional_s3_assets / "regional_asset"

    build_asset.touch()
    regional_asset.touch()
    global_asset.touch()

    yield build_s3_assets, global_s3_assets, regional_s3_assets


@pytest.mark.parametrize(
    "to_delete, is_file",
    [
        ("python_bytecode_1.pyc", True),
        ("python_bytecode_2.pyo", True),
        ("python_bytecode_3.pyd", True),
        (".coverage", True),
        ("cdk.out", False),
        ("some.egg-info", False),
        ("__pycache__", False),
    ],
    ids=["pyc", "pyo", "pyd", "coverage", "cdk", "egg_info", "pycache"],
)
def test_cleanup_source(directory_to_clean, to_delete, is_file):
    build_s3_assets, _, _ = directory_to_clean

    if is_file:
        Path(build_s3_assets / to_delete).touch()
    else:
        Path(build_s3_assets / to_delete).mkdir()

    # cleaner should recurse into the directory and clean up the file(s)/ dirs
    Cleaner.cleanup_source(build_s3_assets.parent)

    assert not Path(build_s3_assets / to_delete).exists()


def test_clean_dirs(directory_to_clean):
    build_s3_assets, global_s3_assets, regional_s3_assets = directory_to_clean
    assert build_s3_assets.is_dir()
    assert global_s3_assets.is_dir()
    assert regional_s3_assets.is_dir()

    Cleaner.clean_dirs(build_s3_assets, global_s3_assets, regional_s3_assets)

    assert build_s3_assets.is_dir()
    assert global_s3_assets.is_dir()
    assert regional_s3_assets.is_dir()

    assert not (build_s3_assets / "build_asset").exists()
    assert not (global_s3_assets / "global_asset").exists()
    assert not (regional_s3_assets / "regional_asset").exists()


def test_cleanble(tmp_path):
    f1 = tmp_path / "test1-abc"
    f2 = tmp_path / "test2-abc"

    d1 = tmp_path / "dir"
    f3 = d1 / "test1-abc"

    f1.touch()
    f2.touch()
    d1.mkdir()
    f3.touch()

    assert all([f1.exists(), f2.exists(), f3.exists()])

    to_clean = Cleanable(name="test1", file_type="f", pattern="test1*")
    to_clean.delete(tmp_path)

    assert not f1.exists()
    assert f2.exists()
    assert not f3.exists()
    assert d1.exists()

    to_clean = Cleanable(name="dir", file_type="d", pattern="dir")
    to_clean.delete(tmp_path)

    assert not d1.exists()
