# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
