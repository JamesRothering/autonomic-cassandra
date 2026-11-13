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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;

/**
 * Test-only {@link KnowledgeStore} held in memory, with switches to make writes or the {@code local_state} scan fail.
 * Survives "restarts" of the layer when the same instance is given to a new service or executor.
 */
public class InMemoryKnowledgeStore extends KnowledgeStore
{
    public final Map<UUID, ActionRecord> actions = new LinkedHashMap<>();
    public final List<ActionEvent> events = new ArrayList<>();
    public final TreeMap<String, String> state = new TreeMap<>();
    public final Map<UUID, StoredDiagnosis> diagnoses = new LinkedHashMap<>();
    public volatile boolean failWrites;
    public volatile boolean failScan;
    private volatile boolean lastWriteFailed;
    private long failedWrites;
    private long failedReads;

    public InMemoryKnowledgeStore()
    {
        super(() -> 3600);
    }

    private synchronized boolean write(Runnable r)
    {
        if (failWrites)
        {
            lastWriteFailed = true;
            failedWrites++;
            return false;
        }
        r.run();
        lastWriteFailed = false;
        return true;
    }

    @Override
    public boolean isAvailable()
    {
        return !lastWriteFailed;
    }

    @Override
    public synchronized long writeFailures()
    {
        return failedWrites;
    }

    @Override
    public synchronized long readFailures()
    {
        return failedReads;
    }

    @Override
    public boolean upsertDiagnosis(Diagnosis d)
    {
        return write(() -> diagnoses.put(d.id, new StoredDiagnosis(d, false)));
    }

    @Override
    public boolean clearDiagnosis(Diagnosis d, long clearedAtMillis)
    {
        return write(() -> diagnoses.put(d.id, new StoredDiagnosis(d, true)));
    }

    @Override
    public synchronized List<StoredDiagnosis> recentDiagnoses(long sinceMillis, long nowMillis, int limit)
    {
        return new ArrayList<>(diagnoses.values());
    }

    @Override
    public boolean saveAction(ActionRecord r)
    {
        return write(() -> actions.put(r.id(), r));
    }

    @Override
    public boolean appendEvent(UUID actionId, ActionState from, ActionState to, String detail)
    {
        return write(() -> events.add(new ActionEvent(actionId, org.apache.cassandra.utils.TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L + events.size(), 1L), from == null ? null : from.name(), to.name(), detail)));
    }

    @Override
    public synchronized List<ActionRecord> recentActions(long sinceMillis, long nowMillis, int limit)
    {
        List<ActionRecord> out = new ArrayList<>();
        for (ActionRecord r : actions.values())
            if (r.proposedMillis >= sinceMillis && out.size() < limit)
                out.add(0, r);
        return out;
    }

    @Override
    public synchronized Optional<ActionRecord> getAction(UUID id, long proposedMillis)
    {
        return Optional.ofNullable(actions.get(id));
    }

    @Override
    public synchronized List<ActionEvent> actionEvents(UUID actionId)
    {
        List<ActionEvent> out = new ArrayList<>();
        for (ActionEvent e : events)
            if (e.actionId.equals(actionId))
                out.add(e);
        return out;
    }

    /** {@code from>to} of every event of the action, in order. */
    public List<String> transitions(UUID actionId)
    {
        List<String> out = new ArrayList<>();
        for (ActionEvent e : actionEvents(actionId))
            out.add((e.fromState == null ? "-" : e.fromState) + '>' + e.toState);
        return out;
    }

    @Override
    public boolean putState(String key, String value, long nowMillis)
    {
        return write(() -> state.put(key, value));
    }

    @Override
    public synchronized Optional<String> getState(String key)
    {
        return Optional.ofNullable(state.get(key));
    }

    @Override
    public boolean deleteState(String key)
    {
        return write(() -> state.remove(key));
    }

    @Override
    public synchronized Optional<Map<String, String>> tryScanState(String prefix)
    {
        if (failScan)
        {
            failedReads++;
            return Optional.empty();
        }
        Map<String, String> out = new LinkedHashMap<>();
        state.forEach((k, v) -> {
            if (k.startsWith(prefix))
                out.put(k, v);
        });
        return Optional.of(out);
    }
}
