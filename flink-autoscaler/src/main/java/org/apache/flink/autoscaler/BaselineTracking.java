/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.BaselineAggregator;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** Tracks and maintains baseline processing rate samples per job vertex over time. */
@Experimental
@Data
@NoArgsConstructor
@Builder
public class BaselineTracking {
    private static final Logger LOG = LoggerFactory.getLogger(BaselineTracking.class);

    private final Map<JobVertexID, SortedMap<Instant, BaselineRecord>> baselineTracking =
            new ConcurrentHashMap<>();

    public void recordNewBaselineSample(
            Configuration conf, EvaluatedMetrics evaluatedMetrics, Instant now) {
        // implement fixed sampling strategy.
        int maxSamples = conf.get(AutoScalerOptions.BASELINE_MAX_SAMPLES);

        for (Map.Entry<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> entry :
                evaluatedMetrics.getVertexMetrics().entrySet()) {
            JobVertexID vertexId = entry.getKey();
            var trueProcessingRate =
                    entry.getValue().get(ScalingMetric.TRUE_PROCESSING_RATE).getAverage();

            if (!Double.isFinite(trueProcessingRate)) {
                continue;
            }

            BaselineRecord record = new BaselineRecord();
            record.setBaselineMetrics(
                    Map.of(
                            ScalingMetric.TRUE_PROCESSING_RATE,
                            EvaluatedScalingMetric.avg(trueProcessingRate)));

            SortedMap<Instant, BaselineRecord> baselineRecords =
                    baselineTracking.computeIfAbsent(vertexId, k -> new TreeMap<>());

            if (baselineRecords.size() >= maxSamples) {
                baselineRecords.remove(baselineRecords.firstKey());
            }
            baselineRecords.put(now, record);
        }
    }

    public Optional<Double> computeBaselineProcessingRate(
            Configuration conf, JobVertexID jobVertexID) {
        BaselineAggregator aggregator = conf.get(AutoScalerOptions.BASELINE_AGGREGATOR);
        int minSamples = conf.get(AutoScalerOptions.BASELINE_MIN_SAMPLES);

        SortedMap<Instant, BaselineRecord> records = baselineTracking.get(jobVertexID);
        if (records == null || records.isEmpty()) {
            return Optional.empty();
        }

        List<Double> processingRates =
                records.values().stream()
                        .map(
                                record ->
                                        record.getBaselineMetrics()
                                                .get(ScalingMetric.TRUE_PROCESSING_RATE))
                        .filter(Objects::nonNull)
                        .map(EvaluatedScalingMetric::getAverage)
                        .filter(Double::isFinite)
                        .collect(Collectors.toList());

        if (processingRates.size() < minSamples) {
            return Optional.empty();
        }

        return Optional.of(aggregator.aggregate(processingRates));
    }

    /**
     * A data structure that stores the evaluated scaling metrics at a specific point in time during
     * baseline collection.
     *
     * <p>Currently, this holds only {@code TRUE_PROCESSING_RATE}, but can be extended to include
     * additional metrics in the future.
     */
    @Data
    public static class BaselineRecord {
        Map<ScalingMetric, EvaluatedScalingMetric> baselineMetrics;
    }
}
