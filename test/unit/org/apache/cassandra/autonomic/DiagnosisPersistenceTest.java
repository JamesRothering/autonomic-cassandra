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
package org.apache.cassandra.autonomic;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.knowledge.KnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** A diagnosis produced by a detector reaches system_autonomic.diagnoses and is marked cleared afterwards. */
public class DiagnosisPersistenceTest extends CQLTester
{
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final UUID id = UUID.randomUUID();
    private AutonomicService service;
    private DurationSpec.IntMillisecondsBound savedGrace;

    @Before
    public void setupService() throws Exception
    {
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        savedGrace = c.startup_grace;
        c.enabled = true;
        c.startup_grace = new DurationSpec.IntMillisecondsBound("0ms");
        execute("TRUNCATE system_autonomic.diagnoses");
    }

    @After
    public void teardownService() throws Exception
    {
        if (service != null)
            service.shutdownBlocking();
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        c.enabled = false;
        c.startup_grace = savedGrace;
    }

    private UntypedResultSet rows()
    {
        return org.apache.cassandra.cql3.QueryProcessor.executeInternal("SELECT id, state, severity, type, subject FROM system_autonomic.diagnoses");
    }

    private void awaitRows(int expected, String state) throws Exception
    {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline)
        {
            UntypedResultSet rs = rows();
            if (rs.size() == expected && (state == null || state.equals(rs.one().getString("state"))))
                return;
            Thread.sleep(20);
        }
        assertEquals(expected, rows().size());
        assertEquals(state, rows().one().getString("state"));
    }

    @Test
    public void detectedDiagnosisIsPersistedAndMarkedClearedWhenItGoes() throws Exception
    {
        AtomicLong now = new AtomicLong(System.currentTimeMillis());
        Detector detector = new Detector()
        {
            public String name() { return "persist_fake"; }
            public Set<SignalId> requires() { return Set.of(); }
            public List<Diagnosis> analyze(KnowledgeView k)
            {
                return active.get() ? List.of(new Diagnosis(id, "fake_type", Subject.table("ks", "t"), Severity.WARN, 0.9,
                                                            now.get(), now.get(), "fake", Map.of("a", "b"), null))
                                    : List.of();
            }
        };
        KnowledgeStore store = new KnowledgeStore();
        service = new AutonomicService(now::get, () -> true, store);
        service.registerDetector(detector);
        service.start();
        service.controlTick();
        awaitRows(1, "OPEN");
        UntypedResultSet.Row row = rows().one();
        assertEquals(id, row.getUUID("id"));
        assertEquals("fake_type", row.getString("type"));
        assertEquals("TABLE:ks.t", row.getString("subject"));

        // unchanged ticks do not rewrite; condition clears
        service.controlTick();
        active.set(false);
        service.controlTick();
        awaitRows(1, "CLEARED");
        assertTrue(store.isAvailable());
        assertEquals(0, store.writeFailures());
    }
}
