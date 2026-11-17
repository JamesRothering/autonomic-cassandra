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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.execute.actuators.PartitionSplitActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.layout.LayoutEvents;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;

/**
 * The outcomes of a partition split that an operator should look at (adaptive-schema design 7.3), as WARN diagnoses of
 * the key's partition subject that stay open for {@link #OPEN_MILLIS} after the outcome. Nothing is proposed for
 * them: reverse migration (T6.17) is not built.
 */
public final class SplitOutcomeDetectors
{
    public static final String VERIFY_FAILED = "layout.verify_failed";
    public static final String SPLIT_NOT_EFFECTIVE = "layout.split_not_effective";
    public static final long OPEN_MILLIS = 24L * 3_600_000;

    private SplitOutcomeDetectors()
    {
    }

    private static UUID id(String type, UUID actionId)
    {
        return UUID.nameUUIDFromBytes((type + ':' + actionId).getBytes(StandardCharsets.UTF_8));
    }

    /** The equivalence verification of a migration failed beyond its retries and the migration was aborted. */
    public static final class VerifyFailed implements Detector
    {
        @Override
        public String name()
        {
            return VERIFY_FAILED;
        }

        @Override
        public Set<SignalId> requires()
        {
            return Collections.emptySet();
        }

        @Override
        public List<Diagnosis> analyze(KnowledgeView knowledge)
        {
            long now = knowledge.nowMillis();
            List<Diagnosis> out = new ArrayList<>();
            for (LayoutEvents.VerifyFailure f : LayoutEvents.verifyFailures())
            {
                if (now - f.atMillis > OPEN_MILLIS)
                    continue;
                Map<String, String> evidence = new LinkedHashMap<>();
                evidence.put("key_hex", f.keyHex);
                evidence.put("table", f.keyspace + '.' + f.table);
                evidence.put("action", f.actionId.toString());
                evidence.put("verification", f.detail);
                out.add(new Diagnosis(id(VERIFY_FAILED, f.actionId), VERIFY_FAILED, Subject.partition(f.keyspace, f.table, f.keyHex), Severity.WARN, 1.0,
                                      f.atMillis, now, "The split of partition 0x" + f.keyHex + " of " + f.keyspace + '.' + f.table
                                                       + " was aborted: its overflow copy did not verify equal to the base copy", evidence, null));
            }
            return out;
        }
    }

    /** Stage B of a split ended NEUTRAL or REGRESSED: the key stays split. */
    public static final class SplitNotEffective implements Detector
    {
        @Override
        public String name()
        {
            return SPLIT_NOT_EFFECTIVE;
        }

        @Override
        public Set<SignalId> requires()
        {
            return Collections.emptySet();
        }

        @Override
        public List<Diagnosis> analyze(KnowledgeView knowledge)
        {
            long now = knowledge.nowMillis();
            List<Diagnosis> out = new ArrayList<>();
            for (ActionRecord r : knowledge.recentActions(OPEN_MILLIS))
            {
                if (!r.action.actuator.equals(PartitionSplitActuator.NAME) || r.state != ActionState.COMMITTED
                    || (r.verdict != Verdict.NEUTRAL && r.verdict != Verdict.REGRESSED))
                    continue;
                Subject target = r.action.target;
                Map<String, String> evidence = new LinkedHashMap<>();
                evidence.put("key_hex", String.valueOf(target.detail));
                evidence.put("table", target.keyspace + '.' + target.table);
                evidence.put("action", r.action.id.toString());
                evidence.put("verdict", r.verdict.name());
                out.add(new Diagnosis(id(SPLIT_NOT_EFFECTIVE, r.action.id), SPLIT_NOT_EFFECTIVE, target, Severity.WARN, 1.0,
                                      r.updatedMillis, now, "The split of partition 0x" + target.detail + " of " + target.keyspace + '.' + target.table
                                                            + " ended " + r.verdict + ": it stays split; a reverse migration is not available yet",
                                      evidence, null));
            }
            return out;
        }
    }
}
