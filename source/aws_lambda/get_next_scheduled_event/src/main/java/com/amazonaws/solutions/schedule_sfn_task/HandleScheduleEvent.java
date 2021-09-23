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

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.quartz.CronExpression;

import java.security.SecureRandom;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.TimeZone;

public class HandleScheduleEvent implements RequestHandler<ScheduleEvent, String> {
    @Override
    public String handleRequest(ScheduleEvent event, Context context) {
        try {
            setNextSchedule(event);
        } catch (ParseException e) {
            throw new ScheduleException(e.getMessage());
        }
        return event.getNext();
    }

    private ScheduleEvent setNextSchedule(ScheduleEvent event) throws ParseException {
        String schedule = event.getSchedule();
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        // create the expression (this throws a ParseException on failure)
        CronExpression expression = new CronExpression(schedule);

        // set up the next date as a string
        int seconds = getRandomSeconds();
        Date dt = Date.from(expression.getNextValidTimeAfter(Date.from(Instant.now())).toInstant().plusSeconds(seconds));
        String dtText = dateFormatter.format(dt);
        event.setNext(event.setNext(dtText));

        return event;
    }

    private int getRandomSeconds() {
        SecureRandom random = new SecureRandom();
        return random.nextInt(60);
    }
}
