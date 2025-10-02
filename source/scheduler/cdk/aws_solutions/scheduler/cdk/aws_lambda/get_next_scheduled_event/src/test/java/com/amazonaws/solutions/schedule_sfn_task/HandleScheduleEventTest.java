// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.solutions.schedule_sfn_task;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.text.DateFormat;
import java.text.SimpleDateFormat;


class HandleScheduleEventTest {
    private ScheduleEvent event;
    private HandleScheduleEvent handler;

    @BeforeEach
    public void setUp() {
        event = new ScheduleEvent();
        handler = new HandleScheduleEvent();
    }

    @Test
    @DisplayName("returns ISO 8601 in UTC with seconds")
    public void testScheduleEventOutput() {
        this.event.setSchedule("cron(* * * * ? *)");
        String result = handler.handleRequest(this.event, null);

        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Assertions.assertDoesNotThrow(() -> {
            sdf.parse(result);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"cron(1)", "* * * * * *", "* * *", "* * * * *"})
    @DisplayName("com.amazonaws.solutions.schedule_sfn_task.ScheduleEvent invalid representation raises com.amazonaws.solutions.schedule_sfn_task.ScheduleException")
    public void testScheduleEventInvalid(String schedule) {
        Assertions.assertThrows(ScheduleException.class, () -> {
            this.event.setSchedule(schedule);
            handler.handleRequest(this.event, null);
        });
    }
}