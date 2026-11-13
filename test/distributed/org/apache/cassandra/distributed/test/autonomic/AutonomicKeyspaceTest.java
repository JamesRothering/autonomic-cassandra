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
package org.apache.cassandra.distributed.test.autonomic;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AutonomicKeyspaceTest extends TestBaseImpl
{
    @Test
    public void keyspaceExistsWithLayerDisabledAndSurvivesRestart() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            // the keyspace is local, so the coordinator reads the node's own rows
            Object[][] none = cluster.coordinator(1).execute("SELECT key FROM system_autonomic.local_state", ConsistencyLevel.ONE);
            assertEquals(0, none.length);
            for (String t : new String[]{ "diagnoses", "actions", "action_events", "local_state" })
                assertEquals(0, cluster.coordinator(1).execute("SELECT * FROM system_autonomic." + t + " LIMIT 1", ConsistencyLevel.ONE).length);
            assertNotNull(cluster.get(1).callOnInstance(() -> org.apache.cassandra.schema.Schema.instance.getKeyspaceMetadata("system_autonomic")));

            // write through the DAO, which needs no running loops
            cluster.get(1).runOnInstance(() -> {
                boolean ok = new org.apache.cassandra.autonomic.knowledge.KnowledgeStore()
                             .putState("paused", "OPERATOR", System.currentTimeMillis());
                if (!ok)
                    throw new AssertionError("write failed");
            });

            cluster.get(1).shutdown().get(1, TimeUnit.MINUTES);
            cluster.get(1).startup();

            Object[][] rows = cluster.coordinator(1).execute("SELECT value FROM system_autonomic.local_state WHERE key = 'paused'", ConsistencyLevel.ONE);
            assertEquals(1, rows.length);
            assertEquals("OPERATOR", rows[0][0]);
            String read = cluster.get(1).callOnInstance(() -> new org.apache.cassandra.autonomic.knowledge.KnowledgeStore()
                                                               .getState("paused").orElse(null));
            assertEquals("OPERATOR", read);
        }
    }
}
