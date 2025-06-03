/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler.utils;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.config.ScheduledScalingOptions;
import org.apache.flink.configuration.Configuration;

import org.quartz.impl.calendar.CronCalendar;
import org.quartz.impl.calendar.DailyCalendar;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/** Calendar utilities. */
public class CalendarUtils {

    /** Quartz doesn't have the invertTimeRange flag so rewrite this method. */
    static boolean isTimeIncluded(CronCalendar cron, long timeInMillis) {
        if (cron.getBaseCalendar() != null
                && !cron.getBaseCalendar().isTimeIncluded(timeInMillis)) {
            return false;
        } else {
            return cron.getCronExpression().isSatisfiedBy(new Date(timeInMillis));
        }
    }

    static Optional<DailyCalendar> interpretAsDaily(String subExpression) {
        String[] splits = subExpression.split("-");
        if (splits.length != 2) {
            return Optional.empty();
        }
        try {
            DailyCalendar daily = new DailyCalendar(splits[0], splits[1]);
            daily.setInvertTimeRange(true);
            return Optional.of(daily);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    static Optional<CronCalendar> interpretAsCron(String subExpression) {
        try {
            return Optional.of(new CronCalendar(subExpression));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    static Optional<String> validatePeriodExpression(String expression, String config) {
        String[] subExpressions = expression.split("&&");
        Optional<DailyCalendar> dailyCalendar = Optional.empty();
        Optional<CronCalendar> cronCalendar = Optional.empty();
        if (subExpressions.length > 2) {
            return Optional.of(
                    String.format(
                            "Invalid value %s in the autoscaler config %s", expression, config));
        }

        for (String subExpression : subExpressions) {
            subExpression = subExpression.strip();
            Optional<DailyCalendar> daily = interpretAsDaily(subExpression);
            dailyCalendar = daily.isPresent() ? daily : dailyCalendar;
            Optional<CronCalendar> cron = interpretAsCron(subExpression);
            cronCalendar = cron.isPresent() ? cron : cronCalendar;

            if (daily.isEmpty() && cron.isEmpty()) {
                return Optional.of(
                        String.format(
                                "Invalid value %s in the autoscaler config %s, the value is neither a valid daily expression nor a valid cron expression",
                                expression, config));
            }
        }

        if (subExpressions.length == 2 && (dailyCalendar.isEmpty() || cronCalendar.isEmpty())) {
            return Optional.of(
                    String.format(
                            "Invalid value %s in the autoscaler config %s, the value can not be configured as dailyExpression && dailyExpression or cronExpression && cronExpression",
                            expression, config));
        }
        return Optional.empty();
    }

    static boolean inPeriod(String expression, Instant instant) {
        String[] subExpressions = expression.split("&&");
        boolean result = true;
        for (String subExpression : subExpressions) {
            subExpression = subExpression.strip();
            Optional<DailyCalendar> daily = interpretAsDaily(subExpression);
            if (daily.isPresent()) {
                result = result && daily.get().isTimeIncluded(instant.toEpochMilli());
            } else {
                Optional<CronCalendar> cron = interpretAsCron(subExpression);
                result = result && isTimeIncluded(cron.get(), instant.toEpochMilli());
            }
        }
        return result;
    }

    public static boolean inExcludedPeriods(Configuration conf, Instant instant) {
        List<String> excludedExpressions = conf.get(AutoScalerOptions.EXCLUDED_PERIODS);
        for (String expression : excludedExpressions) {
            if (inPeriod(expression, instant)) {
                return true;
            }
        }
        return false;
    }

    public static boolean inBaselineWindowPeriods(Configuration conf, Instant instant) {
        List<String> baselineWindowPeriodExpressions = conf.get(AutoScalerOptions.BASELINE_WINDOW);
        for (String expression : baselineWindowPeriodExpressions) {
            if (inPeriod(expression, instant)) {
                return true;
            }
        }
        return false;
    }

    public static boolean inScheduledScalingPeriod(Configuration conf, Instant instant) {
        var schedules = conf.get(AutoScalerOptions.SCHEDULED_SCALING_SCHEDULES);
        if (schedules == null || schedules.isEmpty()) {
            return false;
        }
        for (String schedule : schedules) {
            var scheduleConfig = ScheduledScalingOptions.forScheduledScaling(conf, schedule);
            var scheduleExpressions =
                    scheduleConfig.get(ScheduledScalingOptions.SCHEDULED_SCALING_PERIOD);
            var leadTime = scheduleConfig.get(ScheduledScalingOptions.SCHEDULED_SCALING_LEAD_TIME);

            Instant instantWithLead = instant.plus(leadTime);

            for (String expression : scheduleExpressions) {
                if (inPeriod(expression, instant) || inPeriod(expression, instantWithLead)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static Optional<Double> getMaxScalingMultiplier(Configuration conf, Instant instant) {
        var schedules = conf.get(AutoScalerOptions.SCHEDULED_SCALING_SCHEDULES);
        if (schedules == null || schedules.isEmpty()) {
            return Optional.empty();
        }

        double maxMultiplier = Double.MIN_VALUE;
        boolean foundMatch = false;

        for (String schedule : schedules) {
            var scheduleConfig = ScheduledScalingOptions.forScheduledScaling(conf, schedule);
            var scheduleExpressions =
                    scheduleConfig.get(ScheduledScalingOptions.SCHEDULED_SCALING_PERIOD);
            var leadTime = scheduleConfig.get(ScheduledScalingOptions.SCHEDULED_SCALING_LEAD_TIME);
            var multiplier =
                    scheduleConfig.get(ScheduledScalingOptions.SCHEDULED_SCALING_MULTIPLIER);

            Instant instantWithLead = instant.plus(leadTime);

            for (String expression : scheduleExpressions) {
                if (inPeriod(expression, instant) || inPeriod(expression, instantWithLead)) {
                    if (multiplier != null) {
                        maxMultiplier = Math.max(maxMultiplier, multiplier);
                        foundMatch = true;
                    }
                    break;
                }
            }
        }

        return foundMatch ? Optional.of(maxMultiplier) : Optional.empty();
    }

    public static Optional<String> validateExcludedPeriods(Configuration conf) {
        List<String> excludedExpressions = conf.get(AutoScalerOptions.EXCLUDED_PERIODS);
        for (String expression : excludedExpressions) {
            Optional<String> errorMsg =
                    validatePeriodExpression(expression, AutoScalerOptions.EXCLUDED_PERIODS.key());
            if (errorMsg.isPresent()) {
                return errorMsg;
            }
        }
        return Optional.empty();
    }

    public static Optional<String> validateBaselineWindowPeriods(Configuration conf) {
        List<String> baselineWindowExpressions = conf.get(AutoScalerOptions.BASELINE_WINDOW);
        for (String expression : baselineWindowExpressions) {
            Optional<String> errorMsg =
                    validatePeriodExpression(expression, AutoScalerOptions.BASELINE_WINDOW.key());
            if (errorMsg.isPresent()) {
                return errorMsg;
            }
        }
        return Optional.empty();
    }
}
