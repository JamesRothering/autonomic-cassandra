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
package org.apache.cassandra.autonomic.execute.actuators;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.Applied;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Garbage-collecting compaction of one table ({@code StorageService.garbageCollect}, one job), the irreversible
 * actuator of overview 7.3 rule 10. Cost is bounded by construction: a single user table and a single job. It is
 * started on its own thread so that the actuator thread does not wait for it, and it is verified for reporting only.
 * The governor admits it only at severity CRITICAL and once per target per day; this actuator refuses a second run
 * on the same table within the day as well. Params: {@code tombstone_option} ({@code ROW}, default, or {@code CELL}).
 */
public class GarbageCollectActuator implements Actuator
{
    public static final String NAME = "garbage_collect";
    public static final String PARAM_OPTION = "tombstone_option";
    public static final long INTERVAL_MILLIS = TimeUnit.DAYS.toMillis(1);

    private static final Logger logger = LoggerFactory.getLogger(GarbageCollectActuator.class);

    /** The compaction itself. */
    public interface Compactor
    {
        boolean tableExists(String keyspace, String table);

        void garbageCollect(String keyspace, String table, String tombstoneOption) throws Exception;
    }

    private final Compactor compactor;
    private final Consumer<Runnable> runner;
    private final Map<UUID, Applied> applied = new ConcurrentHashMap<>();
    private final Map<String, Long> lastRun = new ConcurrentHashMap<>();

    public GarbageCollectActuator(Compactor compactor, Consumer<Runnable> runner)
    {
        this.compactor = compactor;
        this.runner = runner;
    }

    public static GarbageCollectActuator create()
    {
        return new GarbageCollectActuator(new Compactor()
        {
            public boolean tableExists(String keyspace, String table)
            {
                return Schema.instance.getTableMetadata(keyspace, table) != null;
            }

            public void garbageCollect(String keyspace, String table, String option) throws Exception
            {
                StorageService.instance.garbageCollect(option, 1, keyspace, table);
            }
        }, task -> executorFactory().startThread("AutonomicGarbageCollect", task));
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public ActionScope scope()
    {
        return ActionScope.NODE;
    }

    @Override
    public boolean reversible()
    {
        return false;
    }

    private static String option(Action action)
    {
        return action.params.getOrDefault(PARAM_OPTION, "ROW");
    }

    @Override
    public Optional<String> validate(Action action, KnowledgeView knowledge)
    {
        Subject t = action.target;
        if (t.kind != Subject.Kind.TABLE || t.keyspace == null || t.table == null)
            return Optional.of("target must be a table");
        if (SchemaConstants.isSystemKeyspace(t.keyspace))
            return Optional.of("system tables are not compacted");
        if (!option(action).equals("ROW") && !option(action).equals("CELL"))
            return Optional.of("tombstone_option must be ROW or CELL");
        if (!compactor.tableExists(t.keyspace, t.table))
            return Optional.of("no table " + t.keyspace + '.' + t.table);
        Long last = lastRun.get(t.asString());
        if (last != null && knowledge.nowMillis() - last < INTERVAL_MILLIS)
            return Optional.of("garbage collection of " + t.asString() + " already ran within the last day");
        return Optional.empty();
    }

    @Override
    public Applied apply(Action action, ActionContext ctx)
    {
        Applied done = applied.get(action.id);
        if (done != null)
            return done;
        Subject t = action.target;
        Map<String, String> undo = new LinkedHashMap<>();
        undo.put("target", t.asString());
        undo.put(PARAM_OPTION, option(action));
        undo.put("irreversible", "true");
        Applied result = new Applied(undo);
        applied.put(action.id, result);
        lastRun.put(t.asString(), ctx.nowMillis());
        String option = option(action);
        runner.accept(() -> {
            try
            {
                compactor.garbageCollect(t.keyspace, t.table, option);
            }
            catch (Throwable e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                logger.warn("Autonomic garbage collection of {} failed", t.asString(), e);
            }
        });
        return result;
    }

    /** Nothing to restore; the executor does not call this for an irreversible actuator. */
    @Override
    public void rollback(Action action, Applied applied, ActionContext ctx)
    {
    }

    @Override
    public boolean overridden(Action action, Applied applied)
    {
        return false;
    }
}
