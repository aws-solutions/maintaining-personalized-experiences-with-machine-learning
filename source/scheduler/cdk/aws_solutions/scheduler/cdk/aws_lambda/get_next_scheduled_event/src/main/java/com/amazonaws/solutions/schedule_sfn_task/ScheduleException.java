// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazonaws.solutions.schedule_sfn_task;

public class ScheduleException extends RuntimeException {
    public ScheduleException(String message) {
        super(message);
    }
}
