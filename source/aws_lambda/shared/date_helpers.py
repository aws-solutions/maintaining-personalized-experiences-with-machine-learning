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
