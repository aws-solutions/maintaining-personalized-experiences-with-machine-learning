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

from functools import lru_cache
from urllib.parse import urlparse

import botocore.exceptions

from aws_solutions.core import get_service_resource


class S3:
    cli = get_service_resource("s3")

    def __init__(self, url, expected_suffix=".csv"):
        self.cli = get_service_resource("s3")
        self.expected_suffix = expected_suffix
        self.url = url
        self._last_modified = None
        self.bucket, self.key = self._urlparse()

    def _urlparse(self):
        parsed = urlparse(self.url, allow_fragments=False)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        return bucket, key

    @property
    @lru_cache()
    def exists(self):
        if self.url.endswith(self.expected_suffix):
            return self._exists_one()
        else:
            return self._exists_any()

    @property
    def last_modified(self):
        if self.exists:
            return self._last_modified

    def _exists_one(self):
        try:
            metadata = self.cli.Object(self.bucket, self.key)
            metadata.load()
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return False

        self._last_modified = metadata.last_modified
        return True

    def _exists_any(self):
        try:
            bucket = self.cli.Bucket(self.bucket)
            objects = [
                o
                for o in bucket.objects.filter(Prefix=self.key + "/", Delimiter="/")
                if o.key.endswith(self.expected_suffix)
            ]
            latest = next(
                iter(sorted(objects, key=lambda k: k.last_modified, reverse=True)), None
            )
        except botocore.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return False

        if latest:
            self._last_modified = latest.last_modified
            return True
        else:
            return False
