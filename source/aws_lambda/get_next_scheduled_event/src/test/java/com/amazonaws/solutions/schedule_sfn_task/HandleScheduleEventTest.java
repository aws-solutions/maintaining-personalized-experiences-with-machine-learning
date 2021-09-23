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