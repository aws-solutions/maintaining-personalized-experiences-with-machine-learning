# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger("cdk-helper")


@dataclass
class Cleanable:
    """Encapsulates something that can be cleaned by the cleaner"""

    name: str
    file_type: str
    pattern: str

    def __post_init__(self):
        if self.file_type not in ("d", "f"):
            raise ValueError("only directories and files are allowed ('d' or 'f')")

    def delete(self, source_dir):
        source_path = Path(source_dir)

        for path in source_path.rglob(self.pattern):
            if "aws_solutions" not in str(path.name):  # prevent the module from being unlinked in a dev environment
                if self.file_type == "d" and path.is_dir():
                    logger.info(f"deleting {self.name} directory {path}")
                    shutil.rmtree(path, ignore_errors=True)
                if self.file_type == "f" and path.is_file():
                    logger.info(f"deleting {self.name} file {path}")
                    try:
                        path.unlink()
                    except FileNotFoundError:
                        pass


class Cleaner:
    """Encapsulates functions that help clean up the build environment."""

    TO_CLEAN = [
        Cleanable("Python bytecode", "f", "*.py[cod]"),
        Cleanable("Python Coverage databases", "f", ".coverage"),
        Cleanable("CDK Cloud Assemblies", "d", "cdk.out"),
        Cleanable("Python egg", "d", "*.egg-info"),
        Cleanable("Python bytecode cache", "d", "__pycache__"),
        Cleanable("Python test cache", "d", ".pytest_cache"),
    ]

    @staticmethod
    def clean_dirs(*args):
        """Recursively remove each of its arguments, then recreate the directory"""
        for dir_to_remove in args:
            logger.info("cleaning %s" % dir_to_remove)
            shutil.rmtree(dir_to_remove, ignore_errors=True)
            os.makedirs(dir_to_remove)

    @staticmethod
    def cleanup_source(source_dir):
        """Cleans up all items found in TO_CLEAN"""
        for item in Cleaner.TO_CLEAN:
            item.delete(source_dir)
