# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime

import parsedatetime as pdt
from aws_lambda_powertools import Logger

logger = Logger()


def parse_datetime(tm: str) -> int:
    if "month" in tm:
        logger.warning("while months are supported, they are based off of the calendar of the start of year 1 CE")
    if "year" in tm:
        logger.warning("while years are supported, they are based off of the calendar of the start of year 1 CE")

    start_of_time = datetime.datetime.min
    cal = pdt.Calendar(version=pdt.VERSION_CONTEXT_STYLE)
    timedelta = cal.parseDT(tm, sourceTime=start_of_time)[0] - start_of_time
    return int(timedelta.total_seconds())
