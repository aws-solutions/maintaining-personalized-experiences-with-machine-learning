# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging


class Logger:
    """Set up a logger fo this package"""

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Gets the current logger for this package
        :param name: the name of the logger
        :return: the logger
        """
        logger = logging.getLogger(name)
        if not len(logger.handlers):
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter("[%(levelname)s]\t%(name)s\t%(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False
        return logger
