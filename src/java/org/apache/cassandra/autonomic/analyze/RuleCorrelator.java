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
package org.apache.cassandra.autonomic.analyze;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.Subject;

/**
 * Links symptoms to causes with a fixed table of cause/effect rules.
 *
 * <p>An effect that has an active cause under some rule is returned with {@code rootCause} set to the ultimate root
 * (a cause that is not itself an effect of another active diagnosis) and with its summary extended by the reason.
 * Nothing is dropped: effects stay in the result, so they are recorded and shown, and {@code AutonomicService} hands
 * only diagnoses without a root cause to policies. The result is a pure function of the diagnoses given, so an
 * effect is released on the first evaluation after its cause is gone. The rule table must be acyclic on diagnosis
 * types, which the constructor checks.
 */
public class RuleCorrelator implements Correlator
{
    /** Where an effect must be relative to its cause. */
    public enum Scope
    {
        /** The effect is on the same keyspace and table as the cause. */
        SAME_TABLE,
        /** Any effect of the type, as every detector looks at this node. */
        LOCAL
    }

    /** {@code cause} makes {@code effect} likely, for the reason {@code because}. */
    public static final class Rule
    {
        public final String cause;
        public final String effect;
        public final Scope scope;
        /** Restricts the effect's subject kind; null for any. */
        public final Subject.Kind effectKind;
        public final String because;

        public Rule(String cause, String effect, Scope scope, Subject.Kind effectKind, String because)
        {
            this.cause = Objects.requireNonNull(cause);
            this.effect = Objects.requireNonNull(effect);
            this.scope = Objects.requireNonNull(scope);
            this.effectKind = effectKind;
            this.because = Objects.requireNonNull(because);
        }

        boolean applies(Diagnosis c, Diagnosis e)
        {
            if (!c.type.equals(cause) || !e.type.equals(effect) || (effectKind != null && e.subject.kind != effectKind))
                return false;
            if (scope == Scope.LOCAL)
                return true;
            Subject cs = c.subject;
            Subject es = e.subject;
            return cs.keyspace != null && cs.table != null && cs.keyspace.equals(es.keyspace) && cs.table.equals(es.table);
        }
    }

    private static final List<Rule> DEFAULT_RULES = List.of(
        new Rule("disk_pressure", "compaction_backlog", Scope.LOCAL, null,
                 "a nearly full disk leaves compaction without room to merge sstables"),
        new Rule("compaction_backlog", "latency_slo_breach", Scope.LOCAL, null,
                 "pending compactions leave more sstables per read and compete for disk and CPU"),
        new Rule("compaction_backlog", "degraded_self", Scope.LOCAL, null,
                 "pending compactions slow this node's own reads"),
        new Rule("gc_pressure", "latency_slo_breach", Scope.LOCAL, null,
                 "garbage collection pauses stall requests node-wide"),
        new Rule("gc_pressure", "pool_saturation", Scope.LOCAL, null,
                 "garbage collection pauses stop stage threads and let queues grow"),
        new Rule("gc_pressure", "degraded_self", Scope.LOCAL, null,
                 "garbage collection pauses slow this node's own requests"),
        new Rule("pool_saturation", "latency_slo_breach", Scope.LOCAL, null,
                 "requests wait in a saturated thread pool queue"),
        new Rule("pool_saturation", "degraded_self", Scope.LOCAL, null,
                 "requests wait in a saturated thread pool queue"),
        new Rule("hot_partition", "latency_slo_breach", Scope.SAME_TABLE, null,
                 "one partition takes a large share of the table's operations"),
        new Rule("tombstone_overload", "latency_slo_breach", Scope.SAME_TABLE, null,
                 "reads scan many tombstones"),
        new Rule("degraded_peer", "latency_slo_breach", Scope.LOCAL, Subject.Kind.NODE,
                 "coordinator requests wait on a slow replica"),
        new Rule("degraded_peer", "degraded_self", Scope.LOCAL, null,
                 "coordinator requests wait on a slow replica, not on this node"));

    /** The built-in rule table. */
    public static List<Rule> defaultRules()
    {
        return DEFAULT_RULES;
    }

    public static RuleCorrelator defaults()
    {
        return new RuleCorrelator(DEFAULT_RULES);
    }

    private final List<Rule> rules;
    private final Map<String, List<Rule>> byEffect = new HashMap<>();

    /** @throws IllegalArgumentException if the rules contain a cycle on diagnosis types */
    public RuleCorrelator(List<Rule> rules)
    {
        this.rules = List.copyOf(rules);
        for (Rule r : this.rules)
            byEffect.computeIfAbsent(r.effect, k -> new ArrayList<>()).add(r);
        checkAcyclic(this.rules);
    }

    static void checkAcyclic(List<Rule> rules)
    {
        Map<String, Set<String>> effects = new HashMap<>();
        for (Rule r : rules)
            effects.computeIfAbsent(r.cause, k -> new HashSet<>()).add(r.effect);
        Set<String> done = new HashSet<>();
        for (String type : effects.keySet())
            visit(type, effects, done, new HashSet<>());
    }

    private static void visit(String type, Map<String, Set<String>> effects, Set<String> done, Set<String> path)
    {
        if (done.contains(type))
            return;
        if (!path.add(type))
            throw new IllegalArgumentException("cycle in correlation rules at " + type);
        for (String next : effects.getOrDefault(type, Collections.emptySet()))
            visit(next, effects, done, path);
        path.remove(type);
        done.add(type);
    }

    @Override
    public List<Diagnosis> correlate(List<Diagnosis> raw, KnowledgeView knowledge)
    {
        Map<UUID, Link> links = new HashMap<>();
        for (Diagnosis e : raw)
        {
            Link best = null;
            for (Rule rule : byEffect.getOrDefault(e.type, Collections.emptyList()))
                for (Diagnosis c : raw)
                    if (c != e && rule.applies(c, e) && (best == null || BETTER.compare(c, best.cause) < 0))
                        best = new Link(c, rule);
            if (best != null)
                links.put(e.id, best);
        }
        List<Diagnosis> out = new ArrayList<>(raw.size());
        for (Diagnosis d : raw)
        {
            Link link = links.get(d.id);
            if (link == null)
            {
                out.add(d);
                continue;
            }
            Diagnosis root = d;
            for (Link l = link; l != null; l = links.get(root.id))
                root = l.cause;
            String summary = d.summary + " [likely caused by " + link.cause.type + " on " + link.cause.subject.asString()
                             + ": " + link.rule.because + ']';
            out.add(new Diagnosis(d.id, d.type, d.subject, d.severity, d.confidence, d.firstSeenMillis, d.lastSeenMillis,
                                  summary, d.evidence, root.id));
        }
        return out;
    }

    /** Most severe first, then the one seen first, then by id, so the choice does not depend on list order. */
    private static final Comparator<Diagnosis> BETTER = Comparator
        .<Diagnosis, Severity>comparing(d -> d.severity, Comparator.reverseOrder())
        .thenComparingLong(d -> d.firstSeenMillis)
        .thenComparing(d -> d.id);

    private static final class Link
    {
        final Diagnosis cause;
        final Rule rule;

        Link(Diagnosis cause, Rule rule)
        {
            this.cause = cause;
            this.rule = rule;
        }
    }
}
