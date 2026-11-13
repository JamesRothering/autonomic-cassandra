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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AutonomicVirtualTablesTest extends CQLTester
{
    private static final String SIGNALS_KS = "vts_autonomic";
    private static final ObservationStore signalStore = new ObservationStore(16, 16);

    @BeforeClass
    public static void registerSignalsKeyspace()
    {
        addVirtualKeyspace();
        // the signals table is tested against a store we construct, since enabling the layer starts its threads
        List<VirtualTable> tables = Collections.singletonList(AutonomicVirtualTables.signals(SIGNALS_KS, () -> signalStore));
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(SIGNALS_KS, tables));
    }

    private static final String[] TABLES = { "autonomic_status", "autonomic_components", "autonomic_signals",
                                             "autonomic_diagnoses", "autonomic_actions", "autonomic_cluster_ledger" };

    @Test
    public void everyTableIsSelectableWhenDisabled() throws Throwable
    {
        assertTrue("the layer is disabled by default", AutonomicService.instance.state() == AutonomicService.State.DISABLED);
        for (String table : TABLES)
            execute("SELECT * FROM system_views." + table);
        assertRows(execute("SELECT value FROM system_views.autonomic_status WHERE name = 'state'"), row("DISABLED"));
        assertEmpty(execute("SELECT * FROM system_views.autonomic_components"));
        assertEmpty(execute("SELECT * FROM system_views.autonomic_signals"));
        assertEmpty(execute("SELECT * FROM system_views.autonomic_cluster_ledger"));
    }

    @Test
    public void diagnosesAndActionsFromTheStore() throws Throwable
    {
        KnowledgeStore store = AutonomicService.instance.knowledgeStore();
        long now = Clock.Global.currentTimeMillis();

        UUID openId = UUID.randomUUID();
        Diagnosis open = new Diagnosis(openId, "vt_test_open", Subject.node(), Severity.WARN, 0.5, now, now,
                                       "open summary", Map.of("k", "v"), null);
        assertTrue(store.upsertDiagnosis(open));
        UUID clearedId = UUID.randomUUID();
        Diagnosis cleared = new Diagnosis(clearedId, "vt_test_cleared", Subject.node(), Severity.INFO, 0.25, now, now,
                                          "cleared summary", Map.of(), null);
        assertTrue(store.clearDiagnosis(cleared, now));

        // stored OPEN and not active in memory (the layer is disabled), so it is shown as stale
        assertRows(execute("SELECT type, state, active, severity, summary FROM system_views.autonomic_diagnoses WHERE id = ?", openId),
                   row("vt_test_open", "STALE", false, "WARN", "open summary"));
        assertRows(execute("SELECT state, active FROM system_views.autonomic_diagnoses WHERE id = ?", clearedId),
                   row("CLEARED", false));

        UUID actionId = TimeUUID.Generator.nextTimeUUID().asUUID();
        Action action = new Action(actionId, "vt_test_actuator", ActionScope.NODE, Subject.node(),
                                   Map.of("knob", "value"), null);
        ActionRecord record = new ActionRecord(action, ActionState.PROPOSED, now, openId, "vt_policy", true,
                                               "reason", Map.of("undo", "x"), Map.of("p99", 1.5), Map.of(), null, now);
        assertTrue(store.saveAction(record));

        assertRows(execute("SELECT actuator, scope, target, policy, dry_run, state, state_reason, diagnosis_id"
                           + " FROM system_views.autonomic_actions WHERE id = ?", actionId),
                   row("vt_test_actuator", "NODE", Subject.node().asString(), "vt_policy", true, "PROPOSED", "reason", openId));
        // the undo map is deliberately not exposed
        assertInvalidMessage("undo", "SELECT undo FROM system_views.autonomic_actions");
    }

    @Test
    public void signalsShowTheNewestPoint() throws Throwable
    {
        long now = Clock.Global.currentTimeMillis();
        signalStore.accept(new Observation(SignalId.of("vt.test.signal"), Subject.node(), now - 1000, 1.0));
        signalStore.accept(new Observation(SignalId.of("vt.test.signal"), Subject.node(), now, 2.5));

        assertRows(execute("SELECT value, samples FROM " + SIGNALS_KS + ".autonomic_signals WHERE signal = ?", "vt.test.signal"),
                   row(2.5, 2));
        assertEquals(1, signalStore.summaries().stream().filter(s -> s.signal.name.equals("vt.test.signal")).count());
    }
}
