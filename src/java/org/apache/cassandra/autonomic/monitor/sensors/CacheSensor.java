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
import java.util.Set;
import java.util.function.Supplier;

import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.service.CacheService;

import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.emit;
import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.perSec;

/**
 * Hit rate and request rate of the key, row and chunk caches (ADR-046), from the deltas of Cassandra's hit and
 * request counters between collections: the lifetime {@code hitRate} gauge hides a recent drop. A cache with zero
 * capacity (disabled) is skipped, and the hit rate is left out of an interval with fewer requests than
 * {@code cache.min_requests_per_sec}.
 *
 * <p>Requests and hits are marked per lookup by {@code InstrumentingCache.get} (through {@code KeyCache.get} for the
 * key cache, from {@code SinglePartitionReadCommand.getThroughCache} for the row cache), so hits over requests is the
 * hit rate per lookup. The row cache defaults to {@code row_cache_size: 0MiB} and is skipped by capacity.
 * {@code ChunkCache.instance} is null when the chunk cache is disabled. The key cache is only used by the BIG
 * sstable format: BTI readers do not implement {@code KeyCacheSupport}, so on BTI the key cache has capacity but no
 * requests, and emits a requests rate of about 0 and no hit rate.
 */
public class CacheSensor implements Sensor
{
    private static final Set<SignalId> SIGNALS = Set.of(
        Signals.CACHE_KEY_HIT_RATE, Signals.CACHE_ROW_HIT_RATE, Signals.CACHE_CHUNK_HIT_RATE,
        Signals.CACHE_KEY_REQUESTS_PER_SEC, Signals.CACHE_ROW_REQUESTS_PER_SEC, Signals.CACHE_CHUNK_REQUESTS_PER_SEC);

    /** One cache's counters at a point in time. */
    static final class Reading
    {
        final String name;
        final long capacity;
        final long hits;
        final long requests;
        final SignalId hitRateSignal;
        final SignalId requestsSignal;

        Reading(String name, long capacity, long hits, long requests, SignalId hitRateSignal, SignalId requestsSignal)
        {
            this.name = name;
            this.capacity = capacity;
            this.hits = hits;
            this.requests = requests;
            this.hitRateSignal = hitRateSignal;
            this.requestsSignal = requestsSignal;
        }
    }

    private final Supplier<List<Reading>> source;
    private final Supplier<Double> minRequestsPerSec;
    private final SensorSupport.Deltas hitDeltas = new SensorSupport.Deltas();
    private final SensorSupport.Deltas requestDeltas = new SensorSupport.Deltas();
    private long lastCollect = -1;

    public CacheSensor()
    {
        this(CacheSensor::liveCaches, () -> DatabaseDescriptor.getAutonomicConfig().cache.min_requests_per_sec);
    }

    CacheSensor(Supplier<List<Reading>> source, Supplier<Double> minRequestsPerSec)
    {
        this.source = source;
        this.minRequestsPerSec = minRequestsPerSec;
    }

    private static List<Reading> liveCaches()
    {
        List<Reading> out = new ArrayList<>(3);
        add(out, "key", CacheService.instance.keyCache.getMetrics(), Signals.CACHE_KEY_HIT_RATE, Signals.CACHE_KEY_REQUESTS_PER_SEC);
        add(out, "row", CacheService.instance.rowCache.getMetrics(), Signals.CACHE_ROW_HIT_RATE, Signals.CACHE_ROW_REQUESTS_PER_SEC);
        ChunkCache chunk = ChunkCache.instance;
        if (chunk != null)
            add(out, "chunk", chunk.metrics, Signals.CACHE_CHUNK_HIT_RATE, Signals.CACHE_CHUNK_REQUESTS_PER_SEC);
        return out;
    }

    private static void add(List<Reading> out, String name, CacheMetrics m, SignalId hitRate, SignalId requests)
    {
        out.add(new Reading(name, m.capacity.getValue(), m.hits.getCount(), m.requests.getCount(), hitRate, requests));
    }

    @Override
    public String name()
    {
        return "cache";
    }

    @Override
    public Set<SignalId> signals()
    {
        return SIGNALS;
    }

    @Override
    public long minIntervalMillis()
    {
        return 5_000;
    }

    @Override
    public void collect(ObservationSink sink, long now)
    {
        Subject node = Subject.node();
        long elapsed = now - lastCollect;
        double floor = minRequestsPerSec.get();
        for (Reading r : source.get())
        {
            if (r.capacity <= 0)
            {
                hitDeltas.retainWhere(k -> !k.equals(r.name));
                requestDeltas.retainWhere(k -> !k.equals(r.name));
                continue;
            }
            long hits = hitDeltas.delta(r.name, r.hits);
            long requests = requestDeltas.delta(r.name, r.requests);
            if (lastCollect < 0 || hits < 0 || requests < 0)
                continue;
            double perSec = perSec(requests, elapsed);
            emit(sink, r.requestsSignal, node, now, perSec);
            if (requests > 0 && perSec >= floor)
                emit(sink, r.hitRateSignal, node, now, Math.min(1.0, (double) hits / requests));
        }
        lastCollect = now;
    }
}
