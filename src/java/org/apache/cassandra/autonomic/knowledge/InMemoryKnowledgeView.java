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
package org.apache.cassandra.autonomic.knowledge;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;
import java.util.function.LongSupplier;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;

/** {@link KnowledgeView} over an {@link ObservationStore}; active diagnoses are held in memory. */
public class InMemoryKnowledgeView implements KnowledgeView
{
    private final ObservationStore store;
    private final LongSupplier clock;
    private volatile List<Diagnosis> activeDiagnoses = Collections.emptyList();

    public InMemoryKnowledgeView(ObservationStore store, LongSupplier clock)
    {
        this.store = store;
        this.clock = clock;
    }

    public ObservationStore store()
    {
        return store;
    }

    /** Replaces the active set; the list is copied. */
    public void setActiveDiagnoses(Collection<Diagnosis> diagnoses)
    {
        activeDiagnoses = List.copyOf(diagnoses);
    }

    @Override
    public OptionalDouble latest(SignalId signal, Subject subject)
    {
        return store.latest(signal, subject);
    }

    @Override
    public Series series(SignalId signal, Subject subject, long lookbackMillis)
    {
        return store.series(signal, subject, nowMillis() - lookbackMillis);
    }

    @Override
    public Collection<Subject> subjects(SignalId signal)
    {
        return store.subjects(signal);
    }

    @Override
    public Collection<Diagnosis> activeDiagnoses()
    {
        return activeDiagnoses;
    }

    @Override
    public Collection<ActionRecord> recentActions(long lookbackMillis)
    {
        // TODO T2.6: local actions from the executor / KnowledgeStore
        return Collections.emptyList();
    }

    @Override
    public long nowMillis()
    {
        return clock.getAsLong();
    }
}
