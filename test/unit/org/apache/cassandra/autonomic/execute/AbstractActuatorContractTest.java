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
package org.apache.cassandra.autonomic.execute;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * The verify/rollback contract of overview 7.3, rules 1 to 4. The unit test of every concrete {@link Actuator}
 * extends this class. Each test method gets a fresh instance, so {@link #actuator()} may return a new actuator per
 * test as long as it returns the same one within a test.
 */
public abstract class AbstractActuatorContractTest
{
    /** Upper bound for a node-scoped rollback, which must be local and non-blocking (rule 3). */
    protected static final long ROLLBACK_BUDGET_MILLIS = 5_000;

    /** The actuator under test; the same instance for the duration of one test. */
    protected abstract Actuator actuator();

    /** A valid action that changes at least one value from the current state; a new id on every call. */
    protected abstract Action newAction();

    /** Every value the actuator can change, as an object with value equality. */
    protected abstract Object snapshot();

    /** Changes a value that the last {@code apply} set, the way an operator would (nodetool, JMX, a setter). */
    protected abstract void operatorOverride();

    protected ActionContext context()
    {
        return new ActionContext()
        {
            private final long deadline = System.currentTimeMillis() + 30_000;

            public long stepDeadlineMillis() { return deadline; }
            public long nowMillis() { return System.currentTimeMillis(); }
            public boolean killed() { return false; }
            public void recordUndo(Map<String, String> undo) {}
        };
    }

    @Test
    public void rule1_applyIsIdempotentPerActionId() throws Exception
    {
        Object before = snapshot();
        Action action = newAction();
        Applied first = actuator().apply(action, context());
        Object afterFirst = snapshot();
        assertNotEquals("apply must change something", before, afterFirst);
        assertFalse("apply must return what rollback needs", first.undo.isEmpty());

        Applied second = actuator().apply(action, context());
        assertEquals("a second apply of the same action must change nothing", afterFirst, snapshot());
        assertEquals("a second apply of the same action must return the same undo", first, second);

        // the undo still describes the state before the first apply
        actuator().rollback(action, second, context());
        assertEquals(before, snapshot());
    }

    @Test
    public void rule2_rollbackRestoresExactlyAndTwiceIsHarmless() throws Exception
    {
        Assume.assumeTrue(actuator().reversible());
        Object before = snapshot();
        Action action = newAction();
        Applied applied = actuator().apply(action, context());

        // rule 1: the undo alone is sufficient, as it is after being read back from the store
        Applied fromStore = new Applied(new LinkedHashMap<>(applied.undo));
        actuator().rollback(action, fromStore, context());
        assertEquals("rollback must restore every value apply changed", before, snapshot());

        actuator().rollback(action, fromStore, context());
        assertEquals("a second rollback must be harmless", before, snapshot());
    }

    @Test
    public void rule3_nodeScopedRollbackIsLocalAndDoesNotBlock() throws Exception
    {
        Assume.assumeTrue(actuator().reversible() && actuator().scope() == ActionScope.NODE);
        Object before = snapshot();
        Action action = newAction();
        Applied applied = actuator().apply(action, context());
        // on a plain thread, with nothing of the autonomic layer available to it
        CompletableFuture<Void> done = CompletableFuture.runAsync(() -> {
            try
            {
                actuator().rollback(action, applied, context());
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
        done.get(ROLLBACK_BUDGET_MILLIS, TimeUnit.MILLISECONDS);
        assertEquals(before, snapshot());
    }

    @Test
    public void rule4_operatorOverrideIsDetectedAndLeftAlone() throws Exception
    {
        Action action = newAction();
        Applied applied = actuator().apply(action, context());
        assertFalse("not overridden right after apply", actuator().overridden(action, applied));

        operatorOverride();
        assertTrue("an operator's change must be detected", actuator().overridden(action, applied));

        // the executor does not call rollback once overridden; an actuator must still never undo an operator's change
        Object operatorState = snapshot();
        if (actuator().reversible())
            actuator().rollback(action, applied, context());
        assertEquals("rollback must not touch a value an operator changed", operatorState, snapshot());
    }
}
