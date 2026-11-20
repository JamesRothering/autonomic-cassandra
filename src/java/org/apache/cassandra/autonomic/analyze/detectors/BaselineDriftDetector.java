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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Reports knobs whose live value differs from {@code cassandra.yaml}: a committed change is an in-memory override
 * (overview 7.3 rule 11) and an operator's change is one too. {@code nodetool autonomic revert} clears it. One INFO
 * diagnosis on the node, with the id kept while any knob drifts. A knob with an action in flight is not reported.
 */
public class BaselineDriftDetector implements Detector
{
    public static final String NAME = "baseline_drift";
    public static final String TYPE = "autonomic.baseline_drift";
    /** Longer than any observation window the policies use by default. */
    public static final long LOOKBACK_MILLIS = 3_600_000;

    private final Supplier<Collection<KnobActuator>> knobs;
    private final long lookbackMillis;
    private UUID id;
    private long firstSeenMillis;

    /** @param lookbackMillis how far back to look for actions in flight, at least the longest observation window */
    public BaselineDriftDetector(Supplier<Collection<KnobActuator>> knobs, long lookbackMillis)
    {
        this.knobs = knobs;
        this.lookbackMillis = lookbackMillis;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<SignalId> requires()
    {
        return Collections.emptySet();
    }

    @Override
    public List<Diagnosis> analyze(KnowledgeView knowledge)
    {
        Set<String> busy = new HashSet<>();
        for (ActionRecord r : knowledge.recentActions(lookbackMillis))
            if (r.state == ActionState.APPROVED || r.state == ActionState.APPLYING || r.state == ActionState.VERIFYING
                || r.state == ActionState.ROLLING_BACK)
                busy.add(r.action.actuator);
        Map<String, String> evidence = new LinkedHashMap<>();
        for (KnobActuator knob : knobs.get())
            if (!busy.contains(knob.name()) && knob.drifted())
                evidence.put(knob.name(), KnobActuator.format(knob.current()) + " (configured " + KnobActuator.format(knob.configured()) + ')');
        if (evidence.isEmpty())
        {
            id = null;
            return Collections.emptyList();
        }
        long now = knowledge.nowMillis();
        if (id == null)
        {
            id = TimeUUID.Generator.nextTimeUUID().asUUID();
            firstSeenMillis = now;
        }
        return List.of(new Diagnosis(id, TYPE, Subject.node(), Severity.INFO, 1.0, firstSeenMillis, now,
                                     evidence.size() + " knob(s) differ from cassandra.yaml: " + describe(evidence),
                                     evidence, null));
    }

    /** {@code knob=current (configured baseline)} for each drifted knob. */
    private static String describe(Map<String, String> evidence)
    {
        List<String> parts = new java.util.ArrayList<>();
        for (Map.Entry<String, String> e : evidence.entrySet())
            parts.add(e.getKey() + '=' + e.getValue());
        return String.join(", ", parts);
    }
}
