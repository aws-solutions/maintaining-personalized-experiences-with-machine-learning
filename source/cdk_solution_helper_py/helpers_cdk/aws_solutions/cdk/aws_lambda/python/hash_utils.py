# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib
import os
from pathlib import Path


class DirectoryHash:
    # fmt: off
    _hash = hashlib.sha1()  # nosec NOSONAR - safe to hash; side-effect of collision is to create new bundle
    # fmt: on

    @classmethod
    def hash(cls, *directories: Path):
        DirectoryHash._hash = hashlib.sha1()  # nosec NOSONAR - safe to hash; see above
        if isinstance(directories, Path):
            directories = [directories]
        for directory in sorted(directories):
            DirectoryHash._hash_dir(str(directory.absolute()))
        return DirectoryHash._hash.hexdigest()

    @classmethod
    def _hash_dir(cls, directory: Path):
        for path, dirs, files in os.walk(directory):
            for file in sorted(files):
                DirectoryHash._hash_file(Path(path) / file)
            for directory in sorted(dirs):
                DirectoryHash._hash_dir(str((Path(path) / directory).absolute()))
            break

    @classmethod
    def _hash_file(cls, file: Path):
        with file.open("rb") as f:
            while True:
                block = f.read(2**10)
                if not block:
                    break
                DirectoryHash._hash.update(block)


class LayerHash:
    @classmethod
    def hash(cls, requirements: Path):
        if not requirements.exists():
            raise ValueError("requirements directory must exist")
        if not requirements.is_dir():
            raise ValueError("requirements must be a directory")

        requirements_txt = requirements / "requirements.txt"
        if not requirements_txt.exists() or not requirements_txt.is_file():
            raise ValueError("requirements.txt file must exist")

        # build the directories to check
        directories = [requirements]

        with open(requirements_txt, "r") as f:
            line = f.readline().strip()
            if line.startswith("-e"):
                raise ValueError(f"editable requirements are not allowed, so {line} is not allowed")

            if line and (line.startswith(".") or "/" in line):
                directories.append(requirements / line)

        return DirectoryHash.hash(*directories)
