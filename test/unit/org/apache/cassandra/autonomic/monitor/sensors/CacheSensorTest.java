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
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.autonomic.monitor.Subject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CacheSensorTest
{
    private static final Subject NODE = Subject.node();

    private long keyCapacity = 1000;
    private long keyHits;
    private long keyRequests;
    private long chunkCapacity = 1000;
    private long chunkHits;
    private long chunkRequests;

    private CacheSensor sensor(double floor)
    {
        return new CacheSensor(() -> {
            List<CacheSensor.Reading> l = new ArrayList<>();
            l.add(new CacheSensor.Reading("key", keyCapacity, keyHits, keyRequests, Signals.CACHE_KEY_HIT_RATE, Signals.CACHE_KEY_REQUESTS_PER_SEC));
            l.add(new CacheSensor.Reading("chunk", chunkCapacity, chunkHits, chunkRequests, Signals.CACHE_CHUNK_HIT_RATE, Signals.CACHE_CHUNK_REQUESTS_PER_SEC));
            return l;
        }, () -> floor);
    }

    @Test
    public void emitsTheHitRateOfTheIntervalAndTheRequestRate()
    {
        CacheSensor s = sensor(10);
        RecordingSink sink = new RecordingSink();
        keyHits = 900;
        keyRequests = 1000;
        s.collect(sink, 1_000);
        assertTrue("first collection only sets the baseline", sink.observations.isEmpty());

        // lifetime rate would be 0.9 + ...; this interval: 100 hits of 500 requests in 5 s
        keyHits += 100;
        keyRequests += 500;
        s.collect(sink, 6_000);
        assertEquals(0.2, sink.value(Signals.CACHE_KEY_HIT_RATE, NODE), 1e-9);
        assertEquals(100.0, sink.value(Signals.CACHE_KEY_REQUESTS_PER_SEC, NODE), 1e-9);
        assertEquals(0.0, sink.value(Signals.CACHE_CHUNK_REQUESTS_PER_SEC, NODE), 1e-9);
        assertNull("no requests, no rate", sink.value(Signals.CACHE_CHUNK_HIT_RATE, NODE));
    }

    @Test
    public void noHitRateBelowTheRequestFloorButTheRequestRateIsStillEmitted()
    {
        CacheSensor s = sensor(10);
        RecordingSink sink = new RecordingSink();
        s.collect(sink, 1_000);
        keyHits += 20;
        keyRequests += 40;   // 8 per second
        s.collect(sink, 6_000);
        assertNull(sink.value(Signals.CACHE_KEY_HIT_RATE, NODE));
        assertEquals(8.0, sink.value(Signals.CACHE_KEY_REQUESTS_PER_SEC, NODE), 1e-9);
        keyHits += 25;
        keyRequests += 50;   // exactly the floor
        s.collect(sink, 11_000);
        assertEquals(0.5, sink.value(Signals.CACHE_KEY_HIT_RATE, NODE), 1e-9);
    }

    @Test
    public void skipsACacheWithZeroCapacity()
    {
        keyCapacity = 0;
        CacheSensor s = sensor(10);
        RecordingSink sink = new RecordingSink();
        s.collect(sink, 1_000);
        keyHits += 50;
        keyRequests += 100;
        chunkHits += 50;
        chunkRequests += 100;
        s.collect(sink, 6_000);
        assertNull(sink.value(Signals.CACHE_KEY_HIT_RATE, NODE));
        assertNull(sink.value(Signals.CACHE_KEY_REQUESTS_PER_SEC, NODE));
        assertEquals(0.5, sink.value(Signals.CACHE_CHUNK_HIT_RATE, NODE), 1e-9);
    }

    @Test
    public void aCounterThatGoesBackwardsIsANewBaseline()
    {
        CacheSensor s = sensor(10);
        RecordingSink sink = new RecordingSink();
        keyHits = 500;
        keyRequests = 1000;
        s.collect(sink, 1_000);
        keyHits = 10;      // the counters were reset (AbstractCacheMetrics.reset; InstrumentingCache.clear does not reset them)
        keyRequests = 20;
        s.collect(sink, 6_000);
        assertNull(sink.value(Signals.CACHE_KEY_HIT_RATE, NODE));
        keyHits += 50;
        keyRequests += 100;
        s.collect(sink, 11_000);
        assertEquals(0.5, sink.value(Signals.CACHE_KEY_HIT_RATE, NODE), 1e-9);
    }
}
