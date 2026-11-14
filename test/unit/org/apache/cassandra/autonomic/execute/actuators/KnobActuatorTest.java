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
package org.apache.cassandra.autonomic.execute.actuators;

import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.Applied;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator.Direction;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Envelope, step clamp, undo, override, direction lock and revert of {@link KnobActuator}. */
public class KnobActuatorTest
{
    private static final ActionContext CTX = new ActionContext()
    {
        public long stepDeadlineMillis() { return Long.MAX_VALUE; }
        public long nowMillis() { return 0; }
        public boolean killed() { return false; }
        public void recordUndo(Map<String, String> undo) {}
    };

    private final KnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(8, 8), () -> 0L);
    private final FakeHandle handle = new FakeHandle(64);
    // envelope [16, 256]
    private final KnobActuator knob = new CompactionThroughputActuator(handle, Map.of());
    private long ids;

    private Action action(KnobActuator k, double target, String diagnosis)
    {
        return new Action(TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L + ids++, 1L), k.name(), ActionScope.NODE, Subject.node(),
                          KnobActuator.params(target, diagnosis), null);
    }

    private Optional<String> validate(double target)
    {
        return knob.validate(action(knob, target, "d1"), view);
    }

    @Test
    public void validateRejectsTargetsOutsideTheEnvelope()
    {
        assertTrue(validate(15).get().contains("outside the envelope"));
        assertTrue(validate(257).get().contains("outside the envelope"));
        assertFalse(validate(16).isPresent());
        assertFalse(validate(256).isPresent());
        assertTrue(validate(64).get().contains("current value"));
    }

    @Test
    public void validateRejectsMissingOrFractionalTarget()
    {
        Action none = new Action(TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L, 1L), knob.name(), ActionScope.NODE, Subject.node(), Map.of(), null);
        assertTrue(knob.validate(none, view).isPresent());
        assertTrue(validate(70.5).get().contains("not an integer"));
    }

    @Test
    public void blastRadiusClampsTheStepToAFractionOfTheCurrentValue()
    {
        assertFalse(knob.blastRadius(action(knob, 80, null), view, 0.25).isPresent());
        assertTrue(knob.blastRadius(action(knob, 81, null), view, 0.25).isPresent());
        assertFalse(knob.blastRadius(action(knob, 48, null), view, 0.25).isPresent());
        assertTrue(knob.blastRadius(action(knob, 47, null), view, 0.25).isPresent());
        // inside the step but outside the envelope
        handle.value = 250;
        assertTrue(knob.blastRadius(action(knob, 257, null), view, 0.25).isPresent());
    }

    @Test
    public void smallIntegerKnobsMayStepByOne()
    {
        FakeHandle h = new FakeHandle(1);
        KnobActuator one = new ConcurrentCompactorsActuator(h, 4);
        assertFalse(one.blastRadius(action(one, 2, null), view, 0.25).isPresent());
        assertTrue(one.blastRadius(action(one, 3, null), view, 0.25).isPresent());
        assertEquals(Optional.of(2.0), one.nextTarget(Direction.UP, 0.25, null));
    }

    @Test
    public void nextTargetStepsAndStopsAtTheEnvelopeLimit()
    {
        assertEquals(Optional.of(80.0), knob.nextTarget(Direction.UP, 0.25, "d1"));
        assertEquals(Optional.of(48.0), knob.nextTarget(Direction.DOWN, 0.25, "d1"));
        handle.value = 250;
        assertEquals(Optional.of(256.0), knob.nextTarget(Direction.UP, 0.25, "d1"));
        handle.value = 256;
        assertEquals(Optional.empty(), knob.nextTarget(Direction.UP, 0.25, "d1"));
        handle.value = 17;
        assertEquals(Optional.of(16.0), knob.nextTarget(Direction.DOWN, 0.25, "d1"));
        handle.value = 16;
        assertEquals(Optional.empty(), knob.nextTarget(Direction.DOWN, 0.25, "d1"));
    }

    @Test
    public void applyRecordsUndoAndRollbackRestores() throws Exception
    {
        Action a = action(knob, 80, "d1");
        Applied applied = knob.apply(a, CTX);
        assertEquals(Map.of("knob", "compaction_throughput", "previous", "64", "set", "80"), applied.undo);
        assertEquals(80, handle.value, 0);
        assertEquals(1, handle.writes);
        knob.apply(a, CTX);
        assertEquals(1, handle.writes);
        knob.rollback(a, applied, CTX);
        assertEquals(64, handle.value, 0);
    }

    @Test
    public void operatorChangeIsNotFought() throws Exception
    {
        Action a = action(knob, 80, "d1");
        Applied applied = knob.apply(a, CTX);
        handle.value = 100;
        assertTrue(knob.overridden(a, applied));
        knob.rollback(a, applied, CTX);
        assertEquals(100, handle.value, 0);
        // and nothing is proposed or accepted until the operator's value is reverted
        assertTrue(knob.operatorOverride());
        assertEquals(Optional.empty(), knob.nextTarget(Direction.UP, 0.25, "d2"));
        assertTrue(knob.validate(action(knob, 120, "d2"), view).get().contains("operator override"));
        assertTrue(knob.revert());
        assertEquals(64, handle.value, 0);
        assertFalse(knob.validate(action(knob, 80, "d2"), view).isPresent());
    }

    @Test
    public void operatorChangeBeforeTheFirstWriteIsAccepted() throws Exception
    {
        assertFalse(knob.operatorOverride());
        handle.value = 70;
        // never written by this actuator, so a policy may act on the new value
        assertEquals(Optional.of(88.0), knob.nextTarget(Direction.UP, 0.25, "d1"));
    }

    @Test
    public void rolledBackIncreaseLocksTheDirectionUntilANewDiagnosis() throws Exception
    {
        Action up = action(knob, 80, "d1");
        knob.rollback(up, knob.apply(up, CTX), CTX);
        assertEquals(64, handle.value, 0);

        assertEquals(Optional.empty(), knob.nextTarget(Direction.UP, 0.25, "d1"));
        assertTrue(knob.validate(action(knob, 80, "d1"), view).get().contains("direction lock"));
        // decreasing is not locked
        assertEquals(Optional.of(48.0), knob.nextTarget(Direction.DOWN, 0.25, "d1"));
        // the diagnosis cleared and a new one opened
        assertEquals(Optional.of(80.0), knob.nextTarget(Direction.UP, 0.25, "d2"));
        assertEquals(Optional.of(80.0), knob.nextTarget(Direction.UP, 0.25, "d1"));
    }

    @Test
    public void rolledBackDecreaseDoesNotLock() throws Exception
    {
        Action down = action(knob, 48, "d1");
        knob.rollback(down, knob.apply(down, CTX), CTX);
        assertEquals(Optional.of(80.0), knob.nextTarget(Direction.UP, 0.25, "d1"));
    }

    @Test
    public void revertRestoresConfiguredValueAndReportsDrift() throws Exception
    {
        assertFalse(knob.drifted());
        assertFalse(knob.revert());
        knob.apply(action(knob, 80, "d1"), CTX);
        assertTrue(knob.drifted());
        assertTrue(knob.revert());
        assertEquals(64, handle.value, 0);
        assertFalse(knob.drifted());
    }

    @Test
    public void unmanagedKnobProposesAndAcceptsNothing()
    {
        KnobActuator unlimited = new CompactionThroughputActuator(new FakeHandle(0), Map.of());
        assertEquals(Optional.empty(), unlimited.nextTarget(Direction.UP, 0.25, "d1"));
        assertTrue(unlimited.validate(action(unlimited, 5, "d1"), view).get().contains("not managed"));
    }
}
