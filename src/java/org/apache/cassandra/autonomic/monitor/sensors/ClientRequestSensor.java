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
package org.apache.cassandra.autonomic.monitor.sensors;

import java.util.Set;

import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;

import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.emit;
import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.perSec;

/** Node-wide coordinator request rates, errors and latency for reads, writes and CAS, from {@link ClientRequestsMetricsHolder}. */
public class ClientRequestSensor implements Sensor
{
    private static final Set<SignalId> SIGNALS = Set.of(
        Signals.CLIENT_READ_OPS_PER_SEC, Signals.CLIENT_READ_LATENCY_MEAN_MICROS, Signals.CLIENT_READ_LATENCY_P99_MICROS,
        Signals.CLIENT_READ_TIMEOUTS_PER_SEC, Signals.CLIENT_READ_UNAVAILABLES_PER_SEC, Signals.CLIENT_READ_FAILURES_PER_SEC,
        Signals.CLIENT_WRITE_OPS_PER_SEC, Signals.CLIENT_WRITE_LATENCY_MEAN_MICROS, Signals.CLIENT_WRITE_LATENCY_P99_MICROS,
        Signals.CLIENT_WRITE_TIMEOUTS_PER_SEC, Signals.CLIENT_WRITE_UNAVAILABLES_PER_SEC, Signals.CLIENT_WRITE_FAILURES_PER_SEC,
        Signals.CLIENT_CAS_READ_OPS_PER_SEC, Signals.CLIENT_CAS_READ_LATENCY_MEAN_MICROS, Signals.CLIENT_CAS_READ_LATENCY_P99_MICROS,
        Signals.CLIENT_CAS_READ_TIMEOUTS_PER_SEC, Signals.CLIENT_CAS_READ_UNAVAILABLES_PER_SEC, Signals.CLIENT_CAS_READ_FAILURES_PER_SEC,
        Signals.CLIENT_CAS_WRITE_OPS_PER_SEC, Signals.CLIENT_CAS_WRITE_LATENCY_MEAN_MICROS, Signals.CLIENT_CAS_WRITE_LATENCY_P99_MICROS,
        Signals.CLIENT_CAS_WRITE_TIMEOUTS_PER_SEC, Signals.CLIENT_CAS_WRITE_UNAVAILABLES_PER_SEC, Signals.CLIENT_CAS_WRITE_FAILURES_PER_SEC);

    private final SensorSupport.Deltas deltas = new SensorSupport.Deltas();
    private long lastCollect = -1;

    @Override
    public String name()
    {
        return "client_request";
    }

    @Override
    public Set<SignalId> signals()
    {
        return SIGNALS;
    }

    @Override
    public long minIntervalMillis()
    {
        return 5_000;
    }

    @Override
    public void collect(ObservationSink sink, long now)
    {
        long elapsed = lastCollect < 0 ? -1 : now - lastCollect;
        lastCollect = now;
        op(sink, now, elapsed, "read", ClientRequestsMetricsHolder.readMetrics,
           Signals.CLIENT_READ_OPS_PER_SEC, Signals.CLIENT_READ_LATENCY_MEAN_MICROS, Signals.CLIENT_READ_LATENCY_P99_MICROS,
           Signals.CLIENT_READ_TIMEOUTS_PER_SEC, Signals.CLIENT_READ_UNAVAILABLES_PER_SEC, Signals.CLIENT_READ_FAILURES_PER_SEC);
        op(sink, now, elapsed, "write", ClientRequestsMetricsHolder.writeMetrics,
           Signals.CLIENT_WRITE_OPS_PER_SEC, Signals.CLIENT_WRITE_LATENCY_MEAN_MICROS, Signals.CLIENT_WRITE_LATENCY_P99_MICROS,
           Signals.CLIENT_WRITE_TIMEOUTS_PER_SEC, Signals.CLIENT_WRITE_UNAVAILABLES_PER_SEC, Signals.CLIENT_WRITE_FAILURES_PER_SEC);
        op(sink, now, elapsed, "cas_read", ClientRequestsMetricsHolder.casReadMetrics,
           Signals.CLIENT_CAS_READ_OPS_PER_SEC, Signals.CLIENT_CAS_READ_LATENCY_MEAN_MICROS, Signals.CLIENT_CAS_READ_LATENCY_P99_MICROS,
           Signals.CLIENT_CAS_READ_TIMEOUTS_PER_SEC, Signals.CLIENT_CAS_READ_UNAVAILABLES_PER_SEC, Signals.CLIENT_CAS_READ_FAILURES_PER_SEC);
        op(sink, now, elapsed, "cas_write", ClientRequestsMetricsHolder.casWriteMetrics,
           Signals.CLIENT_CAS_WRITE_OPS_PER_SEC, Signals.CLIENT_CAS_WRITE_LATENCY_MEAN_MICROS, Signals.CLIENT_CAS_WRITE_LATENCY_P99_MICROS,
           Signals.CLIENT_CAS_WRITE_TIMEOUTS_PER_SEC, Signals.CLIENT_CAS_WRITE_UNAVAILABLES_PER_SEC, Signals.CLIENT_CAS_WRITE_FAILURES_PER_SEC);
    }

    private void op(ObservationSink sink, long now, long elapsed, String op, ClientRequestMetrics m,
                    SignalId ops, SignalId mean, SignalId p99, SignalId timeouts, SignalId unavailables, SignalId failures)
    {
        Subject node = Subject.node();
        long dCount = deltas.delta(op + ".n", m.latency.getCount());
        long dMicros = deltas.delta(op + ".us", m.totalLatency.getCount());
        emit(sink, ops, node, now, perSec(dCount, elapsed));
        if (dCount > 0 && dMicros >= 0)
            emit(sink, mean, node, now, (double) dMicros / dCount);
        emit(sink, p99, node, now, m.latency.getSnapshot().get99thPercentile());
        emit(sink, timeouts, node, now, perSec(deltas.delta(op + ".to", m.timeouts.getCount()), elapsed));
        emit(sink, unavailables, node, now, perSec(deltas.delta(op + ".un", m.unavailables.getCount()), elapsed));
        emit(sink, failures, node, now, perSec(deltas.delta(op + ".fa", m.failures.getCount()), elapsed));
    }
}
