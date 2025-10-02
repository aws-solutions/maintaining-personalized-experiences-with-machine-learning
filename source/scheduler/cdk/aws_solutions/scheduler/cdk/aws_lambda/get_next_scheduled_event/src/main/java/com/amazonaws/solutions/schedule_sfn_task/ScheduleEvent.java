// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

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
