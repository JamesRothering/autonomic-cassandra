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

import org.junit.Test;

import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.knowledge.Series;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ObservationStoreTest
{
    private static final SignalId SIG = SignalId.of("test.signal");
    private static final Subject T1 = Subject.table("ks", "t1");
    private static final Subject T2 = Subject.table("ks", "t2");
    private static final Subject T3 = Subject.table("ks", "t3");

    private static void put(ObservationStore s, Subject subject, long at, double v)
    {
        s.accept(new Observation(SIG, subject, at, v));
    }

    @Test
    public void ringKeepsNewestCapacityPoints()
    {
        ObservationStore s = new ObservationStore(3, 10);
        for (int i = 1; i <= 5; i++)
            put(s, T1, i * 10L, i);
        Series all = s.series(SIG, T1, 0);
        assertArrayEquals(new long[]{ 30, 40, 50 }, all.times);
        assertArrayEquals(new double[]{ 3, 4, 5 }, all.values, 0.0);
        assertEquals(5.0, s.latest(SIG, T1).getAsDouble(), 0.0);
    }

    @Test
    public void lookbackSelectsWindow()
    {
        ObservationStore s = new ObservationStore(10, 10);
        long[] now = { 100 };
        InMemoryKnowledgeView view = new InMemoryKnowledgeView(s, () -> now[0]);
        for (int i = 0; i < 5; i++)
            put(s, T1, i * 20L, i); // 0,20,40,60,80
        Series w = view.series(SIG, T1, 50); // time >= 50
        assertArrayEquals(new long[]{ 60, 80 }, w.times);
        assertArrayEquals(new double[]{ 3, 4 }, w.values, 0.0);
        assertTrue(view.series(SIG, T2, 50).isEmpty());
        assertFalse(view.latest(SIG, T2).isPresent());
        assertEquals(100, view.nowMillis());
        assertTrue(view.recentActions(1000).isEmpty());
    }

    @Test
    public void evictsLeastRecentlyUpdatedSeries()
    {
        ObservationStore s = new ObservationStore(3, 2);
        put(s, T1, 1, 1);
        put(s, T2, 2, 2);
        put(s, T1, 3, 3); // T1 now most recently updated
        assertEquals(0, s.evictions());
        put(s, T3, 4, 4); // evicts T2
        assertEquals(1, s.evictions());
        assertEquals(2, s.seriesCount());
        assertFalse(s.latest(SIG, T2).isPresent());
        assertTrue(s.latest(SIG, T1).isPresent());
        assertTrue(s.latest(SIG, T3).isPresent());
        assertEquals(2, s.subjects(SIG).size());
    }

    @Test
    public void subjectsAndKeys()
    {
        assertEquals("TABLE:ks.t1", T1.asString());
        assertEquals("PARTITION:ks.t1:k", Subject.partition("ks", "t1", "k").asString());
        assertEquals("NODE", Subject.node().asString());
        assertEquals(Subject.table("ks", "t1"), T1);
        assertEquals(Subject.table("ks", "t1").hashCode(), T1.hashCode());
    }
}
