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
package org.apache.cassandra.autonomic.analyze.detectors;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.actuators.CompactionThroughputActuator;
import org.apache.cassandra.autonomic.execute.actuators.FakeHandle;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BaselineDriftDetectorTest
{
    private final FakeHandle handle = new FakeHandle(64);
    private final KnobActuator knob = new CompactionThroughputActuator(handle, Map.of());
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(8, 8), () -> 5_000L);
    private final BaselineDriftDetector detector = new BaselineDriftDetector(() -> List.of(knob), 3_600_000);

    @Test
    public void reportsADriftedKnobWithAStableIdUntilItIsReverted() throws Exception
    {
        assertTrue(detector.analyze(view).isEmpty());
        handle.value = 80;
        Diagnosis d = detector.analyze(view).get(0);
        assertEquals("autonomic.baseline_drift", d.type);
        assertEquals(Severity.INFO, d.severity);
        assertEquals(Subject.node(), d.subject);
        assertEquals("80 (configured 64)", d.evidence.get("compaction_throughput"));
        assertEquals(d.id, detector.analyze(view).get(0).id);

        knob.revert();
        assertTrue(detector.analyze(view).isEmpty());
        handle.value = 80;
        assertTrue(!d.id.equals(detector.analyze(view).get(0).id));
    }

    @Test
    public void ignoresAKnobWithAnActionInFlight()
    {
        handle.value = 80;
        UUID id = TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L, 1L);
        Action a = new Action(id, knob.name(), ActionScope.NODE, Subject.node(), Map.of(), null);
        view.setActionSource(lookback -> List.of(new ActionRecord(a, ActionState.VERIFYING, 4_000L)));
        assertTrue(detector.analyze(view).isEmpty());
        view.setActionSource(lookback -> List.of(new ActionRecord(a, ActionState.COMMITTED, 4_000L)));
        assertEquals(1, detector.analyze(view).size());
    }
}
