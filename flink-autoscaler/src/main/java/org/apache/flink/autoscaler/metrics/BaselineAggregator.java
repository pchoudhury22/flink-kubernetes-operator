package org.apache.flink.autoscaler.metrics;

import java.util.Collection;

/**
 * Aggregation strategies for computing a baseline value from a collection of samples.
 *
 * <p>This enum defines different methods for aggregating a set of baseline metrics. It supports
 * minimum, maximum, and average aggregations.
 */
public enum BaselineAggregator {
    MIN {
        @Override
        public double aggregate(Collection<Double> values) {
            return values.stream().min(Double::compare).orElse(Double.NaN);
        }
    },
    MAX {
        @Override
        public double aggregate(Collection<Double> values) {
            return values.stream().max(Double::compare).orElse(Double.NaN);
        }
    },
    AVG {
        @Override
        public double aggregate(Collection<Double> values) {
            return values.stream().mapToDouble(d -> d).average().orElse(Double.NaN);
        }
    };

    public abstract double aggregate(Collection<Double> values);
}
