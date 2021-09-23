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

import re
from dataclasses import dataclass, field

import cronex

from shared.scheduler import CRON_ANY_WILDCARD, CRON_MIN_MAX_YEAR


class ScheduleError(ValueError):
    pass  # NOSONAR (python:S1186) - is a sort of ValueError


@dataclass
class Schedule:
    """Represents and validates a scheduled expression compatible with Scheduler"""

    expression: str = field(repr=True, compare=True)
    _configuration_errors: list = field(repr=False, init=False, default_factory=list)

    def __post_init__(self):
        self.validate()

    def validate(self):
        """
        Validate the schedule expression, raising ScheduleError for invalid expressions
        :return: None
        """
        # no schedule provided - nothing to validate
        if not self.expression:
            raise ScheduleError("Task is missing a schedule")

        # schedule provided - validate it
        if self.expression.startswith("cron(") and self.expression.endswith(")"):
            self.expression = self._validate_cron()
        elif self.expression == "delete":
            pass  # allow delete
        else:
            raise ScheduleError(
                f'invalid schedule {self.expression}. Use a cron() schedule or "delete" to remove a schedule that already exists'
            )

        if self._configuration_errors:
            raise ScheduleError(".".join(self._configuration_errors))

    def _validate_cron(self) -> str:
        """
        Perform a partial validation of the cron expression
        :param expression: the cron expression e.g. cron(* * * * ? *)
        :return: the expression e.g. * * * ? *
        """
        schedule = self.expression
        # fmt: off
        cron_re = re.compile(
            r"^cron\((?P<minutes>[^ ]+) (?P<hours>[^ ]+) (?P<day_of_month>[^ ]+) (?P<month>[^ ]+) (?P<day_of_week>[^ ]+) (?P<year>[^ ]+)\)$"
        )
        # fmt: on
        match = cron_re.match(schedule)

        if not match:
            self._configuration_errors.append(
                f"invalid cron ScheduleExpression {schedule}. Should have 6 fields"
            )
        else:
            minutes = match.group("minutes")
            hours = match.group("hours")
            day_of_month = match.group("day_of_month")
            month = match.group("month")
            day_of_week = match.group("day_of_week")
            year = match.group("year")

            if day_of_month != CRON_ANY_WILDCARD and day_of_week != CRON_ANY_WILDCARD:
                self._configuration_errors.append(
                    f"invalid cron ScheduleExpression {schedule}. Do not specify day-of-month and day-of week in the same cron expression"
                )

            # validate the majority of the ScheduleExpression
            try:
                cronex.CronExpression(
                    f"{minutes} {hours} {day_of_month} {month} {day_of_week}"
                )
            except ValueError as exc:
                self._configuration_errors.append(
                    f"invalid cron ScheduleExpression: {exc}"
                )

            # cronex does not validate the year - validate separately
            try:
                cronex.parse_atom(year, CRON_MIN_MAX_YEAR)
            except ValueError as exc:
                self._configuration_errors.append(
                    f"invalid cron ScheduleExpression year: {exc}"
                )

            return (
                f"cron({minutes} {hours} {day_of_month} {month} {day_of_week} {year})"
            )
