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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;

/** Collects observations for assertions. */
class RecordingSink implements ObservationSink
{
    final List<Observation> observations = new ArrayList<>();

    @Override
    public void accept(Observation o)
    {
        observations.add(o);
    }

    void clear()
    {
        observations.clear();
    }

    /** The last value emitted for the signal and subject, or null. */
    Double value(SignalId signal, Subject subject)
    {
        Double result = null;
        for (Observation o : observations)
            if (o.signal.equals(signal) && o.subject.equals(subject))
                result = o.value;
        return result;
    }

    Set<SignalId> signals()
    {
        Set<SignalId> s = new HashSet<>();
        for (Observation o : observations)
            s.add(o.signal);
        return s;
    }

    List<Observation> of(SignalId signal)
    {
        List<Observation> result = new ArrayList<>();
        for (Observation o : observations)
            if (o.signal.equals(signal))
                result.add(o);
        return result;
    }

    List<Observation> on(Subject subject)
    {
        List<Observation> result = new ArrayList<>();
        for (Observation o : observations)
            if (o.subject.equals(subject))
                result.add(o);
        return result;
    }
}
