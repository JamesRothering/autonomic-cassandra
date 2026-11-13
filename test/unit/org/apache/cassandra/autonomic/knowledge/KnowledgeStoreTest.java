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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KnowledgeStoreTest extends CQLTester
{
    private static final int TTL = 3600;
    private static final String KS = SchemaConstants.AUTONOMIC_KEYSPACE_NAME;
    private KnowledgeStore store;
    private long now;

    @Before
    public void setupStore()
    {
        store = new KnowledgeStore(() -> TTL);
        now = System.currentTimeMillis();
        execute("TRUNCATE " + KS + ".diagnoses");
        execute("TRUNCATE " + KS + ".actions");
        execute("TRUNCATE " + KS + ".action_events");
        execute("TRUNCATE " + KS + ".local_state");
    }

    private Diagnosis diagnosis(Severity severity)
    {
        return new Diagnosis(UUID.randomUUID(), "hot_partition", Subject.partition("ks", "t", "a:b"), severity, 0.75,
                             now - 1000, now, "sum", Map.of("p99", "12ms"), UUID.randomUUID());
    }

    private ActionRecord action(ActionState state)
    {
        Action a = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), "cache_resize", ActionScope.NODE, Subject.table("ks", "t"), Map.of("size", "64"), null);
        return new ActionRecord(a, state, now, UUID.randomUUID(), "pol", true, "why", Map.of("previous", "32"),
                                Map.of("lat", 1.5), Map.of("lat", 0.5), Verdict.IMPROVED, now - 500);
    }

    @Test
    public void keyspaceAndTablesExist()
    {
        assertNotNull(Schema.instance.getKeyspaceMetadata(KS));
        assertTrue(SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES.contains(KS));
        for (String t : new String[]{ "diagnoses", "actions", "action_events", "local_state" })
            assertNotNull(t, Schema.instance.getTableMetadata(KS, t));
        assertEquals(4, Schema.instance.getKeyspaceMetadata(KS).tables.size());
        assertTrue(Schema.instance.getKeyspaceMetadata(KS).params.replication.klass.getSimpleName().equals("LocalStrategy"));
    }

    @Test
    public void diagnosisRoundTripAndClear()
    {
        Diagnosis d = diagnosis(Severity.WARN);
        assertTrue(store.upsertDiagnosis(d));
        List<KnowledgeStore.StoredDiagnosis> read = store.recentDiagnoses(now - 10_000, now, 10);
        assertEquals(1, read.size());
        assertEquals(d, read.get(0).diagnosis);
        assertFalse(read.get(0).cleared);

        assertTrue(store.clearDiagnosis(d, now + 5));
        read = store.recentDiagnoses(now - 10_000, now, 10);
        assertEquals(1, read.size());
        assertTrue(read.get(0).cleared);
        assertEquals(now + 5, read.get(0).diagnosis.lastSeenMillis);
    }

    @Test
    public void actionAndEventRoundTrip()
    {
        ActionRecord r = action(ActionState.VERIFYING);
        assertTrue(store.recordTransition(r, ActionState.APPLYING, "applied"));
        assertTrue(store.appendEvent(r.id(), ActionState.VERIFYING, ActionState.COMMITTED, null));
        assertEquals(r, store.getAction(r.id(), r.proposedMillis).get());
        List<ActionRecord> recent = store.recentActions(now - 10_000, now, 10);
        assertEquals(List.of(r), recent);

        ActionRecord next = r.withState(ActionState.COMMITTED, "ok", now + 1);
        assertTrue(store.saveAction(next));
        assertEquals(next, store.getAction(r.id(), r.proposedMillis).get());

        List<KnowledgeStore.ActionEvent> events = store.actionEvents(r.id());
        assertEquals(2, events.size());
        assertEquals("APPLYING", events.get(0).fromState);
        assertEquals("VERIFYING", events.get(0).toState);
        assertEquals("applied", events.get(0).detail);
        assertEquals("COMMITTED", events.get(1).toState);
    }

    @Test
    public void localStateRoundTripPrefixScanAndDelete()
    {
        assertEquals(Optional.empty(), store.getState("paused"));
        assertTrue(store.putState("paused", "OPERATOR", now));
        assertTrue(store.putState("inflight/b", "2", now));
        assertTrue(store.putState("inflight/a", "1", now));
        assertTrue(store.putState("cooldown/x", "3", now));
        assertEquals(Optional.of("OPERATOR"), store.getState("paused"));
        assertEquals(Map.of("inflight/a", "1", "inflight/b", "2"), store.scanState("inflight/"));
        assertEquals(List.of("inflight/a", "inflight/b"), List.copyOf(store.scanState("inflight/").keySet()));
        assertTrue(store.deleteState("inflight/a"));
        assertEquals(Map.of("inflight/b", "2"), store.scanState("inflight/"));
        assertEquals(Optional.empty(), store.getState("inflight/a"));
    }

    private int ttlOf(String cql)
    {
        UntypedResultSet rs = execute(cql);
        return rs.one().getInt("ttl_v");
    }

    @Test
    public void historyRowsCarryTtlButLocalStateDoesNot() throws Throwable
    {
        Diagnosis d = diagnosis(Severity.INFO);
        ActionRecord r = action(ActionState.PROPOSED);
        store.upsertDiagnosis(d);
        store.saveAction(r);
        store.appendEvent(r.id(), null, ActionState.PROPOSED, "x");
        store.putState("k", "v", now);
        for (String q : new String[]{ "SELECT TTL(type) AS ttl_v FROM " + KS + ".diagnoses",
                                      "SELECT TTL(state) AS ttl_v FROM " + KS + ".actions",
                                      "SELECT TTL(to_state) AS ttl_v FROM " + KS + ".action_events" })
        {
            int ttl = ttlOf(q);
            assertTrue(q + " ttl=" + ttl, ttl > 0 && ttl <= TTL);
        }
        assertFalse(execute("SELECT TTL(value) AS ttl_v FROM " + KS + ".local_state").one().has("ttl_v"));
    }

    @Test
    public void writeFailureIsCountedAndDoesNotThrow()
    {
        assertTrue(store.isAvailable());
        store.setRunner((cql, args) -> { throw new IllegalStateException("boom"); });
        assertFalse(store.upsertDiagnosis(diagnosis(Severity.WARN)));
        assertFalse(store.putState("k", "v", now));
        assertFalse(store.recordTransition(action(ActionState.APPLYING), ActionState.APPROVED, null));
        assertEquals(4, store.writeFailures());
        assertFalse(store.isAvailable());
        assertTrue(store.scanState("").isEmpty());
        assertEquals(1, store.readFailures());

        store.setRunner(org.apache.cassandra.cql3.QueryProcessor::executeInternal);
        assertTrue(store.putState("k", "v", now));
        assertTrue(store.isAvailable());
        assertEquals(4, store.writeFailures());
    }

    @Test
    public void subjectsRoundTrip()
    {
        for (Subject s : new Subject[]{ Subject.node(), Subject.cluster(), Subject.keyspace("k"), Subject.table("k", "t"),
                                         Subject.partition("k", "t", "a:b"), Subject.threadPool("ReadStage"), Subject.peer("h") })
            assertEquals(s, KnowledgeStore.parseSubject(s.asString()));
    }
}
