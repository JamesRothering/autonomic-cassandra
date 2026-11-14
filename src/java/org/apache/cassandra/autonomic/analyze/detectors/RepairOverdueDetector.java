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
package org.apache.cassandra.autonomic.analyze.detectors;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils;
import org.apache.cassandra.service.AutoRepairService;

/**
 * A host whose last successful auto-repair finished more than {@code interval_multiple} (default 2.0) repair
 * intervals ago, where the interval is {@code min_repair_interval} of the configured auto-repair. A host with no
 * repair on record counts as repaired at time 0. CRITICAL from {@code critical_multiple} (default 4.0) intervals.
 * Subject is the host ({@code Subject.peer}). Only {@code repair_type} (default FULL) is watched.
 *
 * <p>With auto-repair disabled in configuration the detector still reports, at INFO, with evidence
 * {@code auto_repair_enabled=false}; the policy takes no action then.
 *
 * <p>The measurement is read from {@code system_distributed} through {@link AutoRepairUtils}, not from the signal
 * store, so it is cached for {@code refresh_ms} (default 30000) and the control thread pays one history read at
 * most, plus one per host while some host is not known to be fresh. A failed read leaves the measurement missing.
 */
public class RepairOverdueDetector extends ThresholdDetector
{
    public static final String NAME = "repair_overdue";
    public static final String MULTIPLE_KEY = "interval_multiple";
    public static final String CRITICAL_KEY = "critical_multiple";
    public static final String TYPE_KEY = "repair_type";
    public static final String REFRESH_KEY = "refresh_ms";
    public static final double DEFAULT_MULTIPLE = 2.0;
    public static final double DEFAULT_CRITICAL_MULTIPLE = 4.0;
    public static final long DEFAULT_REFRESH_MILLIS = 30_000;
    public static final long DEFAULT_INTERVAL_MILLIS = 24L * 3_600_000;
    /** Unit-less ratio of the time since the last repair to the repair interval; derived, no sensor emits it. */
    public static final SignalId OVERDUE_RATIO = SignalId.of("repair.overdue_ratio");
    /** Evidence keys the policy reads. */
    public static final String EVIDENCE_TYPE = "repair_type";
    public static final String EVIDENCE_ENABLED = "auto_repair_enabled";

    private static final Logger logger = LoggerFactory.getLogger(RepairOverdueDetector.class);

    /** The auto-repair state this detector reads. */
    public interface History
    {
        /** Auto-repair scheduling of this type is enabled in configuration. */
        boolean enabled(RepairType type);

        /** The configured repair interval; not positive if unknown. */
        long intervalMillis(RepairType type);

        /**
         * Host id to the finish time (epoch millis, 0 for none) of its last repair, for every host in the ring. Hosts
         * known to have finished after {@code freshAfterMillis} may share one lower bound. Throws if unreadable.
         */
        Map<UUID, Long> lastFinishMillis(RepairType type, long freshAfterMillis) throws Exception;
    }

    /** Reads {@link AutoRepairUtils}; when auto-repair is disabled its statements are not prepared, so no history is known. */
    public static final class Production implements History
    {
        @Override
        public boolean enabled(RepairType type)
        {
            return AutoRepairService.instance.getAutoRepairConfig() != null
                   && AutoRepairService.instance.getAutoRepairConfig().isAutoRepairEnabled(type);
        }

        @Override
        public long intervalMillis(RepairType type)
        {
            if (AutoRepairService.instance.getAutoRepairConfig() == null)
                return DEFAULT_INTERVAL_MILLIS;
            return AutoRepairService.instance.getAutoRepairConfig().getRepairMinInterval(type).toMilliseconds();
        }

        @Override
        public Map<UUID, Long> lastFinishMillis(RepairType type, long freshAfterMillis) throws Exception
        {
            boolean enabled = enabled(type);
            Set<UUID> hosts = AutoRepairUtils.getHostIdsInCurrentRing(type);
            Map<UUID, Long> out = new HashMap<>();
            if (enabled)
            {
                List<AutoRepairUtils.AutoRepairHistory> history = AutoRepairUtils.getAutoRepairHistory(type);
                if (history != null && history.size() >= hosts.size())
                {
                    long oldest = Long.MAX_VALUE;
                    for (AutoRepairUtils.AutoRepairHistory h : history)
                        oldest = Math.min(oldest, h.getLastRepairFinishTime());
                    if (oldest > freshAfterMillis)
                    {
                        for (UUID host : hosts)
                            out.put(host, oldest);
                        return out;
                    }
                }
            }
            for (UUID host : hosts)
            {
                long finished = 0;
                try
                {
                    finished = AutoRepairUtils.getLastRepairFinishTimeForNode(type, host);
                }
                catch (Exception | LinkageError e)
                {
                    // disabled: the scheduler never prepared its statements, so there is no history; enabled: unreadable
                    if (enabled)
                        throw e;
                }
                out.put(host, finished);
            }
            return out;
        }
    }

    private final History history;
    private final RepairType type;
    private final double criticalMultiple;
    private final long refreshMillis;

    private long refreshedAtMillis = Long.MIN_VALUE;
    private boolean enabled;
    private long intervalMillis = DEFAULT_INTERVAL_MILLIS;
    private Map<UUID, Long> lastFinish = Collections.emptyMap();

    public RepairOverdueDetector(Settings settings, History history, RepairType type, double criticalMultiple, long refreshMillis)
    {
        super(NAME, NAME, OVERDUE_RATIO, settings);
        this.history = history;
        this.type = type;
        this.criticalMultiple = criticalMultiple;
        this.refreshMillis = refreshMillis;
    }

    public static RepairOverdueDetector fromConfig(Map<String, Object> options)
    {
        return fromConfig(options, new Production());
    }

    public static RepairOverdueDetector fromConfig(Map<String, Object> options, History history)
    {
        Settings settings = Settings.fromConfig(options, MULTIPLE_KEY, DEFAULT_MULTIPLE);
        RepairType type = RepairType.FULL;
        double critical = DEFAULT_CRITICAL_MULTIPLE;
        long refresh = DEFAULT_REFRESH_MILLIS;
        try
        {
            Object t = options.get(TYPE_KEY);
            if (t != null)
                type = RepairType.parse(t.toString());
            Object c = options.get(CRITICAL_KEY);
            if (c != null)
                critical = Double.parseDouble(c.toString().trim());
            Object r = options.get(REFRESH_KEY);
            if (r != null)
                refresh = Long.parseLong(r.toString().trim());
        }
        catch (RuntimeException e)
        {
            logger.warn("Invalid {} options {}: {}; using defaults", NAME, options, e.getMessage());
        }
        return new RepairOverdueDetector(settings, history, type, Math.max(critical, settings.raise), Math.max(0, refresh));
    }

    @Override
    public Set<SignalId> requires()
    {
        return Collections.emptySet();
    }

    @Override
    protected void beginEvaluation(KnowledgeView knowledge)
    {
        long now = knowledge.nowMillis();
        if (refreshedAtMillis != Long.MIN_VALUE && now - refreshedAtMillis < refreshMillis)
            return;
        refreshedAtMillis = now;
        try
        {
            enabled = history.enabled(type);
            intervalMillis = history.intervalMillis(type);
            if (intervalMillis <= 0)
                intervalMillis = DEFAULT_INTERVAL_MILLIS;
            lastFinish = history.lastFinishMillis(type, now - (long) (settings().raise * intervalMillis));
        }
        catch (Exception | LinkageError e)
        {
            logger.debug("Detector {} could not read the {} repair history", NAME, type, e);
            lastFinish = Collections.emptyMap();
        }
    }

    @Override
    protected Collection<Subject> subjects(KnowledgeView knowledge)
    {
        Set<Subject> out = new LinkedHashSet<>();
        for (UUID host : lastFinish.keySet())
            out.add(Subject.peer(host.toString()));
        return out;
    }

    @Override
    protected OptionalDouble measure(Subject subject, KnowledgeView knowledge)
    {
        Long finished = lastFinish.get(UUID.fromString(subject.detail));
        if (finished == null)
            return OptionalDouble.empty();
        return OptionalDouble.of(Math.max(0, knowledge.nowMillis() - finished) / (double) intervalMillis);
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        if (!enabled)
            return Severity.INFO;
        return value >= criticalMultiple ? Severity.CRITICAL : Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        return String.format(Locale.ROOT, "Host %s last finished a %s repair %.1f repair intervals ago%s", subject.detail, type, value,
                             enabled ? "" : "; auto-repair is disabled");
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        evidence.put(OVERDUE_RATIO.name, format(value) + " (raise " + format(settings().raise) + ", clear " + format(settings().clear) + ')');
        evidence.put(EVIDENCE_TYPE, type.name());
        evidence.put(EVIDENCE_ENABLED, Boolean.toString(enabled));
        evidence.put("repair_interval_ms", Long.toString(intervalMillis));
        Long finished = lastFinish.get(UUID.fromString(subject.detail));
        evidence.put("last_repair_finish_ms", finished == null || finished == 0 ? "never" : Long.toString(finished));
    }
}
