package org.apache.flink.autoscaler.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.AUTOSCALER_CONF_PREFIX;
import static org.apache.flink.autoscaler.config.AutoScalerOptions.SCHEDULED_SCALING_CONF_PREFIX;
import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Configuration options for scheduled scaling within the Flink Autoscaler.
 *
 * <p>Scheduled scaling allows users to define time-based windows during which the autoscaler
 * adjusts target processing capacity using a predefined multiplier over the computed baseline.
 */
public class ScheduledScalingOptions {
    // add docs for the config
    public static final ConfigOption<List<String>> SCHEDULED_SCALING_PERIOD =
            key("periods")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the periods during which the specified schedule should be active, the expression consist of two optional subexpressions concatenated with &&, "
                                    + "one is cron expression in Quartz format (6 or 7 positions), "
                                    + "for example, * * 9-11,14-16 * * ? means exclude from 9:00:00am to 11:59:59am and from 2:00:00pm to 4:59:59pm every day, * * * ? * 2-6 means exclude every weekday, etc."
                                    + "see http://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html for the usage of cron expression."
                                    + "Caution: in most case cron expression is enough, we introduce the other subexpression: daily expression, because cron can only represent integer hour period without minutes and "
                                    + "seconds suffix, daily expression's formation is startTime-endTime, such as 9:30:30-10:50:20, when exclude from 9:30:30-10:50:20 in Monday and Thursday "
                                    + "we can express it as 9:30:30-10:50:20 && * * * ? * 2,5");

    public static final ConfigOption<Integer> SCHEDULED_SCALING_MULTIPLIER =
            key("scaling-multiplier")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Multiplier applied to the baseline processing rate during the defined schedule. This sets the target processing capacity as multiplier × baseline rate.");

    public static final ConfigOption<Duration> SCHEDULED_SCALING_LEAD_TIME =
            key("lead-time")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Time before the start of the scheduled window during which the autoscaler should proactively scale up. This ensures the system is ready before the actual workload spike. ");

    public static Configuration forScheduledScaling(
            Configuration configuration, String scheduleName) {
        // add support for fallBackKey with DelegatingConfiguration.
        return new DelegatingConfiguration(
                configuration,
                AUTOSCALER_CONF_PREFIX + SCHEDULED_SCALING_CONF_PREFIX + scheduleName + ".");
    }
}
