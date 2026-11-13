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
package org.apache.cassandra.autonomic.monitor;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Test-only sensor that lets a test push observations for any signal. Pushed values are delivered to the store on the
 * next {@link #collect}. A push without a timestamp is stamped with the collect time.
 */
public class InjectedSensor implements Sensor
{
    private static final class Pending
    {
        final SignalId signal;
        final Subject subject;
        final double value;
        final Long atMillis;

        Pending(SignalId signal, Subject subject, double value, Long atMillis)
        {
            this.signal = signal;
            this.subject = subject;
            this.value = value;
            this.atMillis = atMillis;
        }
    }

    private final Queue<Pending> pending = new ConcurrentLinkedQueue<>();
    private final Set<SignalId> signals = ConcurrentHashMap.newKeySet();

    public InjectedSensor push(SignalId signal, Subject subject, double value)
    {
        signals.add(signal);
        pending.add(new Pending(signal, subject, value, null));
        return this;
    }

    public InjectedSensor push(SignalId signal, Subject subject, long atMillis, double value)
    {
        signals.add(signal);
        pending.add(new Pending(signal, subject, value, atMillis));
        return this;
    }

    @Override
    public String name()
    {
        return "injected";
    }

    @Override
    public Set<SignalId> signals()
    {
        return Set.copyOf(signals);
    }

    @Override
    public long minIntervalMillis()
    {
        return 0;
    }

    @Override
    public void collect(ObservationSink sink, long nowMillis)
    {
        Pending p;
        while ((p = pending.poll()) != null)
            sink.accept(new Observation(p.signal, p.subject, p.atMillis != null ? p.atMillis : nowMillis, p.value));
    }
}
