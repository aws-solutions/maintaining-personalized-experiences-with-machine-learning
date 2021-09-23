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

import logging

from aws_solutions.cdk.helpers.logger import Logger


def test_logger(caplog):
    logger = Logger.get_logger("test-logger")
    logger.propagate = True  # for test

    assert logger.level == logging.INFO

    with caplog.at_level(logging.INFO):
        logger.critical("CRITICAL")
        logger.error("ERROR")
        logger.warning("WARNING")
        logger.info("INFO")
        logging.debug("DEBUG")

    for level in "CRITICAL ERROR WARNING INFO".split(" "):
        assert level in caplog.text
