/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the License.
 */

package com.amazonaws.solutions.schedule_sfn_task;

public class ScheduleEvent {
    private String schedule;
    private String next;

    public String getNext() {
        return next;
    }

    public String setNext(String next) {
        this.next = next;
        return next;
    }

    public void setSchedule(String schedule) {
        /*
        cron schedules have 7 fields (seconds, minutes, hours, day-of-month month day-of-week and year), we use only the
        last 6 fields (omitting seconds). To do this, we always set seconds to 0, and keep the remainder of the provided
        schedule. When generating a next scheduled time, we use a random number of seconds in the minute to avoid hot
        spots at the start of each minute. An example string schedule provided might look like * * * * ? * (e.g. every
        minute)
        */
        schedule = validateSchedule(schedule);
        this.schedule = "0 " + schedule;
    }

    public String getSchedule() {
        return schedule;
    }

    private String validateSchedule(String schedule) {
        schedule = schedule
                .replace("cron(", "")
                .replace(")", "");

        String[] fields = schedule.split("\\s+");

        if(fields.length != 6) {
            throw new ScheduleException("schedule " + schedule + " is not a valid schedule (requires 6 fields)");
        }
        return schedule;
    }
}
