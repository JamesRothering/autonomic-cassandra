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
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.serializers.SimpleDateSerializer;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.schema.SchemaConstants.AUTONOMIC_KEYSPACE_NAME;

/**
 * DAO over the node-local {@code system_autonomic} tables (overview 5.1), using internal queries.
 *
 * <p>It has no dependency on the running loops, so it works when the layer is disabled. Every method is synchronous
 * and runs on the caller's thread: the caller chooses the thread, and uses the same methods for the synchronous
 * write-ahead cases of overview 7.2 ({@link #recordTransition}, {@link #putState}). Write methods never throw: a failure
 * is logged, counted in {@link #writeFailures()}, and reported by the {@code false} return value and by
 * {@link #isAvailable()}. Read methods never throw either; on failure they return an empty result and count a
 * read failure.
 */
public class KnowledgeStore
{
    private static final Logger logger = LoggerFactory.getLogger(KnowledgeStore.class);

    public static final String STATE_OPEN = "OPEN";
    public static final String STATE_CLEARED = "CLEARED";

    /** Executes one internal query; replaceable in tests to inject failures. */
    @FunctionalInterface
    interface QueryRunner
    {
        UntypedResultSet run(String cql, Object... args);
    }

    /** A diagnosis row read back from the store. */
    public static final class StoredDiagnosis
    {
        public final Diagnosis diagnosis;
        public final boolean cleared;

        StoredDiagnosis(Diagnosis diagnosis, boolean cleared)
        {
            this.diagnosis = diagnosis;
            this.cleared = cleared;
        }
    }

    /** A row of {@code action_events}. */
    public static final class ActionEvent
    {
        public final UUID actionId;
        public final UUID at;
        public final String fromState;
        public final String toState;
        public final String detail;

        ActionEvent(UUID actionId, UUID at, String fromState, String toState, String detail)
        {
            this.actionId = actionId;
            this.at = at;
            this.fromState = fromState;
            this.toState = toState;
            this.detail = detail;
        }
    }

    private final IntSupplier ttlSeconds;
    private volatile QueryRunner runner = QueryProcessor::executeInternal;
    private final AtomicLong writeFailures = new AtomicLong();
    private final AtomicLong readFailures = new AtomicLong();
    private volatile boolean lastWriteFailed;

    /** A store whose TTL follows {@code autonomic.history_ttl}. */
    public KnowledgeStore()
    {
        this(() -> (int) DatabaseDescriptor.getAutonomicConfig().history_ttl.toSeconds());
    }

    public KnowledgeStore(IntSupplier ttlSeconds)
    {
        this.ttlSeconds = ttlSeconds;
    }

    @VisibleForTesting
    void setRunner(QueryRunner runner)
    {
        this.runner = runner;
    }

    public long writeFailures()
    {
        return writeFailures.get();
    }

    public long readFailures()
    {
        return readFailures.get();
    }

    /**
     * False when the most recent write failed. The governor uses this for its {@code STORE_UNAVAILABLE} check;
     * it becomes true again after the next successful write.
     */
    public boolean isAvailable()
    {
        return !lastWriteFailed;
    }

    private static String table(String name)
    {
        return AUTONOMIC_KEYSPACE_NAME + '.' + name;
    }

    private static int day(long millis)
    {
        return SimpleDateSerializer.timeInMillisToDay(millis);
    }

    private boolean write(String what, String cql, Object... args)
    {
        try
        {
            runner.run(cql, args);
            lastWriteFailed = false;
            return true;
        }
        catch (Throwable t)
        {
            lastWriteFailed = true;
            writeFailures.incrementAndGet();
            logger.warn("Autonomic knowledge store write failed ({}): {}", what, t.toString());
            return false;
        }
    }

    private UntypedResultSet read(String what, String cql, Object... args)
    {
        try
        {
            return runner.run(cql, args);
        }
        catch (Throwable t)
        {
            readFailures.incrementAndGet();
            logger.warn("Autonomic knowledge store read failed ({}): {}", what, t.toString());
            return null;
        }
    }

    // ---- diagnoses ----

    /** Writes (inserts or replaces) the row of an open diagnosis. */
    public boolean upsertDiagnosis(Diagnosis d)
    {
        return writeDiagnosis(d, STATE_OPEN, d.lastSeenMillis);
    }

    /** Marks a diagnosis cleared at {@code clearedAtMillis}; the row stays until its TTL expires. */
    public boolean clearDiagnosis(Diagnosis d, long clearedAtMillis)
    {
        return writeDiagnosis(d, STATE_CLEARED, Math.max(clearedAtMillis, d.lastSeenMillis));
    }

    private boolean writeDiagnosis(Diagnosis d, String state, long lastSeen)
    {
        return write("diagnosis " + d.id,
                     "INSERT INTO " + table(AutonomicKeyspace.DIAGNOSES)
                     + " (day, id, type, subject, severity, confidence, state, first_seen, last_seen, summary, evidence, root_cause)"
                     + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
                     day(d.firstSeenMillis), d.id, d.type, d.subject.asString(), d.severity.name(), d.confidence, state,
                     new Date(d.firstSeenMillis), new Date(lastSeen), d.summary, d.evidence, d.rootCause, ttlSeconds.getAsInt());
    }

    /** Diagnoses whose last activity is at or after {@code sinceMillis}, newest partition first, at most {@code limit}. */
    public List<StoredDiagnosis> recentDiagnoses(long sinceMillis, long nowMillis, int limit)
    {
        List<StoredDiagnosis> out = new ArrayList<>();
        for (int day = day(nowMillis); day >= day(sinceMillis) && out.size() < limit; day--)
        {
            UntypedResultSet rows = read("diagnoses", "SELECT * FROM " + table(AutonomicKeyspace.DIAGNOSES) + " WHERE day = ? LIMIT ?", day, limit);
            if (rows == null)
                break;
            for (UntypedResultSet.Row row : rows)
            {
                long lastSeen = row.getTimestamp("last_seen").getTime();
                if (lastSeen < sinceMillis || out.size() >= limit)
                    continue;
                Map<String, String> evidence = row.has("evidence") ? row.getMap("evidence", UTF8Type.instance, UTF8Type.instance) : Collections.emptyMap();
                Diagnosis d = new Diagnosis(row.getUUID("id"), row.getString("type"), parseSubject(row.getString("subject")),
                                            Severity.valueOf(row.getString("severity")), row.getDouble("confidence"),
                                            row.getTimestamp("first_seen").getTime(), lastSeen, row.getString("summary"),
                                            evidence, row.has("root_cause") ? row.getUUID("root_cause") : null);
                out.add(new StoredDiagnosis(d, STATE_CLEARED.equals(row.getString("state"))));
            }
        }
        return out;
    }

    // ---- actions and events ----

    /** Writes (inserts or replaces) the full row of an action record. */
    public boolean saveAction(ActionRecord r)
    {
        Action a = r.action;
        return write("action " + a.id,
                     "INSERT INTO " + table(AutonomicKeyspace.ACTIONS)
                     + " (day, id, diagnosis_id, policy, actuator, scope, target, params, dry_run, state, state_reason, undo,"
                     + " baseline, result, verdict, proposed_at, updated_at)"
                     + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ?",
                     day(r.proposedMillis), TimeUUID.fromUuid(a.id), r.diagnosisId, r.policy, a.actuator, a.scope.name(),
                     a.target.asString(), a.params, r.dryRun, r.state.name(), r.stateReason, r.undo, r.baseline, r.result,
                     r.verdict == null ? null : r.verdict.name(), new Date(r.proposedMillis), new Date(r.updatedMillis),
                     ttlSeconds.getAsInt());
    }

    /** Appends one row to the audit trail. {@code from} is null for the initial state. */
    public boolean appendEvent(UUID actionId, ActionState from, ActionState to, String detail)
    {
        return write("action event " + actionId,
                     "INSERT INTO " + table(AutonomicKeyspace.ACTION_EVENTS)
                     + " (action_id, at, from_state, to_state, detail) VALUES (?, ?, ?, ?, ?) USING TTL ?",
                     TimeUUID.fromUuid(actionId), TimeUUID.Generator.nextTimeUUID(), from == null ? null : from.name(), to.name(),
                     detail, ttlSeconds.getAsInt());
    }

    /**
     * Synchronously persists a state transition: the action row, then the audit event. Used for the write-ahead
     * transitions of overview 7.2. Returns true only if both writes succeeded.
     */
    public boolean recordTransition(ActionRecord record, ActionState from, String detail)
    {
        boolean saved = saveAction(record);
        boolean logged = appendEvent(record.id(), from, record.state, detail);
        return saved && logged;
    }

    /** Action records proposed at or after {@code sinceMillis}, newest first, at most {@code limit}. */
    public List<ActionRecord> recentActions(long sinceMillis, long nowMillis, int limit)
    {
        List<ActionRecord> out = new ArrayList<>();
        for (int day = day(nowMillis); day >= day(sinceMillis) && out.size() < limit; day--)
        {
            UntypedResultSet rows = read("actions", "SELECT * FROM " + table(AutonomicKeyspace.ACTIONS) + " WHERE day = ? LIMIT ?", day, limit);
            if (rows == null)
                break;
            for (UntypedResultSet.Row row : rows)
            {
                long proposed = row.getTimestamp("proposed_at").getTime();
                if (proposed >= sinceMillis && out.size() < limit)
                    out.add(parseAction(row));
            }
        }
        return out;
    }

    public Optional<ActionRecord> getAction(UUID id, long proposedMillis)
    {
        UntypedResultSet rows = read("action", "SELECT * FROM " + table(AutonomicKeyspace.ACTIONS) + " WHERE day = ? AND id = ?",
                                     day(proposedMillis), TimeUUID.fromUuid(id));
        if (rows == null || rows.isEmpty())
            return Optional.empty();
        return Optional.of(parseAction(rows.one()));
    }

    public List<ActionEvent> actionEvents(UUID actionId)
    {
        List<ActionEvent> out = new ArrayList<>();
        UntypedResultSet rows = read("action events", "SELECT * FROM " + table(AutonomicKeyspace.ACTION_EVENTS) + " WHERE action_id = ?",
                                     TimeUUID.fromUuid(actionId));
        if (rows == null)
            return out;
        for (UntypedResultSet.Row row : rows)
            out.add(new ActionEvent(actionId, row.getTimeUUID("at").asUUID(), row.has("from_state") ? row.getString("from_state") : null,
                                    row.getString("to_state"), row.has("detail") ? row.getString("detail") : null));
        return out;
    }

    private static ActionRecord parseAction(UntypedResultSet.Row row)
    {
        UUID id = row.getTimeUUID("id").asUUID();
        Map<String, String> params = row.has("params") ? row.getMap("params", UTF8Type.instance, UTF8Type.instance) : Collections.emptyMap();
        Action action = new Action(id, row.getString("actuator"), ActionScope.valueOf(row.getString("scope")),
                                   parseSubject(row.getString("target")), params, null);
        return new ActionRecord(action, ActionState.valueOf(row.getString("state")), row.getTimestamp("updated_at").getTime(),
                                row.has("diagnosis_id") ? row.getUUID("diagnosis_id") : null,
                                row.has("policy") ? row.getString("policy") : null,
                                row.has("dry_run") && row.getBoolean("dry_run"),
                                row.has("state_reason") ? row.getString("state_reason") : null,
                                row.has("undo") ? row.getMap("undo", UTF8Type.instance, UTF8Type.instance) : Collections.emptyMap(),
                                row.has("baseline") ? row.getMap("baseline", UTF8Type.instance, DoubleType.instance) : Collections.emptyMap(),
                                row.has("result") ? row.getMap("result", UTF8Type.instance, DoubleType.instance) : Collections.emptyMap(),
                                row.has("verdict") ? Verdict.valueOf(row.getString("verdict")) : null,
                                row.getTimestamp("proposed_at").getTime());
    }

    // ---- local_state ----

    /** Synchronously writes a durable flag; no TTL. */
    public boolean putState(String key, String value, long nowMillis)
    {
        return write("state " + key, "INSERT INTO " + table(AutonomicKeyspace.LOCAL_STATE) + " (key, value, updated_at) VALUES (?, ?, ?)",
                     key, value, new Date(nowMillis));
    }

    public Optional<String> getState(String key)
    {
        UntypedResultSet rows = read("state " + key, "SELECT value FROM " + table(AutonomicKeyspace.LOCAL_STATE) + " WHERE key = ?", key);
        if (rows == null || rows.isEmpty())
            return Optional.empty();
        UntypedResultSet.Row row = rows.one();
        return row.has("value") ? Optional.of(row.getString("value")) : Optional.empty();
    }

    public boolean deleteState(String key)
    {
        return write("delete state " + key, "DELETE FROM " + table(AutonomicKeyspace.LOCAL_STATE) + " WHERE key = ?", key);
    }

    /**
     * All entries whose key starts with {@code prefix}, in key order. The table is small, so this scans it and
     * filters in memory.
     */
    public Map<String, String> scanState(String prefix)
    {
        Map<String, String> out = new LinkedHashMap<>();
        UntypedResultSet rows = read("scan state " + prefix, "SELECT key, value FROM " + table(AutonomicKeyspace.LOCAL_STATE));
        if (rows == null)
            return out;
        java.util.TreeMap<String, String> sorted = new java.util.TreeMap<>();
        for (UntypedResultSet.Row row : rows)
        {
            String key = row.getString("key");
            if (key.startsWith(prefix))
                sorted.put(key, row.has("value") ? row.getString("value") : null);
        }
        out.putAll(sorted);
        return out;
    }

    // ---- helpers ----

    /** Inverse of {@link Subject#asString()}. */
    static Subject parseSubject(String s)
    {
        int colon = s.indexOf(':');
        String kind = colon < 0 ? s : s.substring(0, colon);
        String rest = colon < 0 ? null : s.substring(colon + 1);
        switch (Subject.Kind.valueOf(kind))
        {
            case KEYSPACE:
                return Subject.keyspace(rest);
            case TABLE:
            {
                int dot = rest.indexOf('.');
                return Subject.table(rest.substring(0, dot), rest.substring(dot + 1));
            }
            case PARTITION:
            {
                int dot = rest.indexOf('.');
                int sep = rest.indexOf(':', dot);
                return Subject.partition(rest.substring(0, dot), rest.substring(dot + 1, sep), rest.substring(sep + 1));
            }
            case THREAD_POOL:
                return Subject.threadPool(rest);
            case PEER:
                return Subject.peer(rest);
            case CLUSTER:
                return Subject.cluster();
            default:
                return Subject.node();
        }
    }
}
