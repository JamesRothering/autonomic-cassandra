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
package org.apache.cassandra.autonomic.safety;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.AutonomicConfig.SafetyConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionOutcome;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;

/**
 * The thirteen checks of overview 7.1 and the state behind them.
 *
 * <p>Threading: called from the control thread ({@code evaluate}), JMX threads ({@code approve}, {@code reset}) and
 * the actuator thread ({@code recordOutcome}). All state is guarded by {@code this}; nothing blocks or does I/O
 * under the lock. {@link Journal} callbacks (durable writes, self-pause) run on the calling thread after the lock
 * is released, so the governor never holds its lock while calling out, except for the cheap, side-effect-free
 * {@link Environment} and {@link Actuator#validate} / {@code blastRadius} / {@code knobs} calls.
 */
public class DefaultSafetyGovernor implements SafetyGovernor
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultSafetyGovernor.class);

    public static final String COOLDOWN_PREFIX = "cooldown/";
    public static final String QUARANTINE_PREFIX = "quarantine/";
    /** Rule 10 of overview 7.3: an irreversible actuator runs at most once per target per day. */
    public static final long IRREVERSIBLE_COOLDOWN_MILLIS = TimeUnit.DAYS.toMillis(1);
    static final long RATE_WINDOW_MILLIS = TimeUnit.HOURS.toMillis(1);

    /** What the governor reads from the rest of the layer. Every method must be cheap and must not block. */
    public interface Environment
    {
        AutonomicConfig config();

        long nowMillis();

        /** Check 1: null if the layer is armed and running, otherwise KILLED, PAUSED or NOT_ARMED. */
        String layerGate();

        /** Null if unknown. */
        Actuator actuator(String name);

        boolean actuatorEnabled(String name);

        /** Check 5: the node is JOINED and not draining. */
        boolean nodeReady();

        /** Check 6: the write-ahead record can be written. */
        boolean storeAvailable();

        default ClusterGate clusterGate()
        {
            return ClusterGate.CLOSED;
        }
    }

    /** Effects of governor decisions; called outside the governor's lock, on the thread that called the governor. */
    public interface Journal
    {
        /** Durable flag in {@code local_state}; best effort. */
        void putState(String key, String value);

        void deleteState(String key);

        void quarantined(String policy, long untilMillis);

        /** {@code rollbacks} of the last {@code window} actions rolled back or failed: the layer must pause. */
        void breakerTripped(int rollbacks, int window);

        Journal NONE = new Journal()
        {
            public void putState(String key, String value) {}
            public void deleteState(String key) {}
            public void quarantined(String policy, long untilMillis) {}
            public void breakerTripped(int rollbacks, int window) {}
        };
    }

    private static final class Reservation
    {
        final Action action;
        final String policy;
        final Set<String> knobs;
        /** Awaiting operator approval: counts for CONFLICT but not for CONCURRENCY or RATE. */
        boolean held;

        Reservation(Action action, String policy, Set<String> knobs, boolean held)
        {
            this.action = action;
            this.policy = policy;
            this.knobs = knobs;
            this.held = held;
        }
    }

    private static final class Cooldown
    {
        final long untilMillis;
        final int consecutiveRollbacks;

        Cooldown(long untilMillis, int consecutiveRollbacks)
        {
            this.untilMillis = untilMillis;
            this.consecutiveRollbacks = consecutiveRollbacks;
        }
    }

    private final Environment env;
    private final Journal journal;

    // all guarded by this
    private final Map<UUID, Reservation> reservations = new LinkedHashMap<>();
    private final Map<String, Cooldown> cooldowns = new HashMap<>();
    private final Map<String, Long> quarantines = new HashMap<>();
    private final Map<String, ArrayDeque<Long>> policyRollbacks = new HashMap<>();
    private final ArrayDeque<Long> approvals = new ArrayDeque<>();
    private final ArrayDeque<Long> clusterApprovals = new ArrayDeque<>();
    private final Map<String, ArrayDeque<Long>> actuatorApprovals = new HashMap<>();
    /** Outcomes of the most recent applied actions, oldest first; true for rolled back or failed. */
    private final ArrayDeque<Boolean> breaker = new ArrayDeque<>();

    public DefaultSafetyGovernor(Environment env, Journal journal)
    {
        this.env = env;
        this.journal = journal;
    }

    @Override
    public Decision evaluate(ActionRecord proposal, KnowledgeView knowledge)
    {
        return decide(proposal, knowledge, false);
    }

    @Override
    public Decision approve(ActionRecord proposal, KnowledgeView knowledge)
    {
        return decide(proposal, knowledge, true);
    }

    private synchronized Decision decide(ActionRecord proposal, KnowledgeView knowledge, boolean operatorApproved)
    {
        Action action = proposal.action;
        Set<String> knobs = new HashSet<>();
        Decision rejection;
        try
        {
            rejection = firstFailingCheck(proposal, knowledge, operatorApproved, knobs);
        }
        catch (Throwable t)
        {
            // fail static: a check that cannot be evaluated does not pass
            logger.warn("Autonomic governor could not evaluate {}", action, t);
            rejection = Decision.reject(Reasons.GOVERNOR_ERROR, t.toString());
        }
        if (rejection != null)
        {
            reservations.remove(action.id);
            return rejection;
        }
        // 13 APPROVAL
        if (!operatorApproved && !SafetyConfig.APPROVAL_AUTO.equals(approvalMode(action.scope)))
        {
            reservations.put(action.id, new Reservation(action, proposal.policy, knobs, true));
            return Decision.holdForApproval(Reasons.APPROVAL);
        }
        reservations.put(action.id, new Reservation(action, proposal.policy, knobs, false));
        long now = env.nowMillis();
        approvals.add(now);
        actuatorApprovals.computeIfAbsent(action.actuator, k -> new ArrayDeque<>()).add(now);
        if (action.scope == ActionScope.CLUSTER)
            clusterApprovals.add(now);
        return Decision.approve();
    }

    /** Checks 1 to 12; null if all pass. Fills {@code knobs} for the reservation. */
    private Decision firstFailingCheck(ActionRecord proposal, KnowledgeView knowledge, boolean operatorApproved, Set<String> knobs)
    {
        Action action = proposal.action;
        AutonomicConfig config = env.config();
        SafetyConfig safety = config.safety;
        long now = env.nowMillis();
        boolean cluster = action.scope == ActionScope.CLUSTER;

        // 1 KILLED / PAUSED / not armed
        String gate = env.layerGate();
        if (gate != null)
            return Decision.reject(gate);

        // 2 DRY_RUN
        if (config.dry_run)
            return Decision.reject(Reasons.DRY_RUN);

        // 3 ACTUATOR_DISABLED, and the cluster gate
        Actuator actuator = env.actuator(action.actuator);
        if (actuator == null || !env.actuatorEnabled(action.actuator))
            return Decision.reject(Reasons.ACTUATOR_DISABLED, actuator == null ? "unknown actuator" : null);
        ClusterGate clusterGate = env.clusterGate();
        if (cluster && !clusterGate.enabled())
            return Decision.reject(Reasons.CLUSTER_DISABLED);

        // 4 VALIDATION
        if (actuator.scope() != action.scope)
            return Decision.reject(Reasons.VALIDATION, "action scope " + action.scope + " differs from actuator scope " + actuator.scope());
        if (action.id.version() != 1)
            return Decision.reject(Reasons.VALIDATION, "action id is not a timeuuid");
        if (action.verify == null)
            return Decision.reject(Reasons.VALIDATION, "no verify spec");
        // rule 10 of overview 7.3, waived under manual approval (FC4)
        if (!actuator.reversible() && SafetyConfig.APPROVAL_AUTO.equals(approvalMode(action.scope))
            && severityOf(proposal.diagnosisId, knowledge) != Severity.CRITICAL)
            return Decision.reject(Reasons.VALIDATION, "an irreversible actuator may only be selected at severity CRITICAL");
        Optional<String> invalid = actuator.validate(action, knowledge);
        if (invalid.isPresent())
            return Decision.reject(Reasons.VALIDATION, invalid.get());

        // 5 NODE_STATE
        if (!env.nodeReady())
            return Decision.reject(Reasons.NODE_STATE);
        if (cluster)
        {
            Optional<String> state = clusterGate.nodeState(action);
            if (state.isPresent())
                return Decision.reject(Reasons.NODE_STATE, state.get());
        }

        // 6 STORE_UNAVAILABLE
        if (!env.storeAvailable())
            return Decision.reject(Reasons.STORE_UNAVAILABLE);

        // 7 QUARANTINED
        Long quarantinedUntil = proposal.policy == null ? null : quarantines.get(proposal.policy);
        if (quarantinedUntil != null && now < quarantinedUntil)
            return Decision.reject(Reasons.QUARANTINED, "policy " + proposal.policy + " until " + quarantinedUntil);

        // 8 COOLDOWN
        Cooldown cooldown = cooldowns.get(action.dedupeKey);
        if (cooldown != null && now < cooldown.untilMillis)
            return Decision.reject(Reasons.COOLDOWN, "until " + cooldown.untilMillis);

        // 9 CONFLICT
        Collection<String> declared = actuator.knobs(action);
        if (declared != null)
            knobs.addAll(declared);
        for (Reservation other : reservations.values())
        {
            if (other.action.id.equals(action.id))
                continue;
            if (other.action.dedupeKey.equals(action.dedupeKey) || !Collections.disjoint(other.knobs, knobs))
                return Decision.reject(Reasons.CONFLICT, "with action " + other.action.id);
        }

        // 10 CONCURRENCY
        for (Reservation other : reservations.values())
            if (!other.held && other.action.scope == action.scope && !other.action.id.equals(action.id))
                return Decision.reject(Reasons.CONCURRENCY, "action " + other.action.id + " is in flight");
        if (cluster)
        {
            Optional<String> busy = clusterGate.concurrency(action);
            if (busy.isPresent())
                return Decision.reject(Reasons.CONCURRENCY, busy.get());
        }

        // 11 RATE
        if (count(approvals, now) >= safety.max_actions_per_hour)
            return Decision.reject(Reasons.RATE, "node");
        ArrayDeque<Long> perActuator = actuatorApprovals.get(action.actuator);
        if (perActuator != null && count(perActuator, now) >= safety.max_actions_per_actuator_per_hour)
            return Decision.reject(Reasons.RATE, "actuator");
        if (cluster && count(clusterApprovals, now) >= safety.max_cluster_actions_per_hour)
            return Decision.reject(Reasons.RATE, "cluster");

        // 12 BLAST_RADIUS
        Optional<String> tooLarge = actuator.blastRadius(action, knowledge, safety.max_step_fraction);
        if (tooLarge.isPresent())
            return Decision.reject(Reasons.BLAST_RADIUS, tooLarge.get());
        return null;
    }

    private String approvalMode(ActionScope scope)
    {
        SafetyConfig safety = env.config().safety;
        return scope == ActionScope.CLUSTER ? safety.cluster_approval : safety.node_approval;
    }

    private static Severity severityOf(UUID diagnosisId, KnowledgeView knowledge)
    {
        if (diagnosisId == null)
            return null;
        for (Diagnosis d : knowledge.activeDiagnoses())
            if (d.id.equals(diagnosisId))
                return d.severity;
        return null;
    }

    /** Entries within the sliding window; older ones are dropped. */
    private static int count(ArrayDeque<Long> window, long now)
    {
        while (!window.isEmpty() && window.peekFirst() <= now - RATE_WINDOW_MILLIS)
            window.pollFirst();
        return window.size();
    }

    @Override
    public void recordOutcome(Action action, ActionOutcome outcome)
    {
        List<Runnable> effects = new ArrayList<>();
        synchronized (this)
        {
            Reservation reservation = reservations.remove(action.id);
            SafetyConfig safety = env.config().safety;
            long now = env.nowMillis();
            long afterRollback = safety.cooldown_after_rollback.toMilliseconds();
            int consecutive = consecutiveRollbacks(action.dedupeKey);
            switch (outcome.finalState)
            {
                case COMMITTED:
                {
                    Actuator actuator = env.actuator(action.actuator);
                    long duration = safety.cooldown_after_commit.toMilliseconds();
                    if (actuator != null && !actuator.reversible())
                        duration = Math.max(duration, IRREVERSIBLE_COOLDOWN_MILLIS);
                    setCooldown(action.dedupeKey, now + duration, 0, effects);
                    recordBreaker(false, safety, effects);
                    break;
                }
                case SUPERSEDED:
                    // an operator owns the value now: stay away, but do not blame the policy
                    setCooldown(action.dedupeKey, now + afterRollback, consecutive, effects);
                    recordBreaker(false, safety, effects);
                    break;
                case ROLLED_BACK:
                case FAILED:
                    if (outcome.finalState == ActionState.ROLLED_BACK && Reasons.operatorCaused(outcome.detail))
                    {
                        setCooldown(action.dedupeKey, now + afterRollback, consecutive, effects);
                        break;
                    }
                    consecutive++;
                    long max = safety.cooldown_max.toMilliseconds();
                    long duration = consecutive > 30 ? max : Math.min(max, afterRollback << (consecutive - 1));
                    setCooldown(action.dedupeKey, now + duration, consecutive, effects);
                    if (reservation != null && reservation.policy != null)
                        recordPolicyRollback(reservation.policy, now, safety, effects);
                    recordBreaker(true, safety, effects);
                    break;
                default:
                    // REJECTED: never applied, only the reservation is released
                    break;
            }
            prune(now, safety);
        }
        effects.forEach(Runnable::run);
    }

    private int consecutiveRollbacks(String dedupeKey)
    {
        Cooldown c = cooldowns.get(dedupeKey);
        return c == null ? 0 : c.consecutiveRollbacks;
    }

    private void setCooldown(String dedupeKey, long until, int consecutive, List<Runnable> effects)
    {
        Cooldown existing = cooldowns.get(dedupeKey);
        long effective = existing == null ? until : Math.max(until, existing.untilMillis);
        cooldowns.put(dedupeKey, new Cooldown(effective, consecutive));
        effects.add(() -> journal.putState(COOLDOWN_PREFIX + dedupeKey, effective + "|" + consecutive));
    }

    private void recordPolicyRollback(String policy, long now, SafetyConfig safety, List<Runnable> effects)
    {
        ArrayDeque<Long> times = policyRollbacks.computeIfAbsent(policy, k -> new ArrayDeque<>());
        times.add(now);
        long window = safety.quarantine_window.toMilliseconds();
        while (!times.isEmpty() && times.peekFirst() <= now - window)
            times.pollFirst();
        if (times.size() < safety.quarantine_after_rollbacks)
            return;
        long until = now + safety.quarantine_duration.toMilliseconds();
        quarantines.put(policy, until);
        times.clear();
        effects.add(() -> {
            journal.putState(QUARANTINE_PREFIX + policy, String.valueOf(until));
            journal.quarantined(policy, until);
        });
    }

    private void recordBreaker(boolean rolledBack, SafetyConfig safety, List<Runnable> effects)
    {
        breaker.add(rolledBack);
        while (breaker.size() > Math.max(1, safety.breaker_window))
            breaker.pollFirst();
        int rollbacks = 0;
        for (boolean b : breaker)
            if (b)
                rollbacks++;
        if (rolledBack && rollbacks >= safety.breaker_rollbacks)
        {
            int count = rollbacks;
            int window = breaker.size();
            effects.add(() -> journal.breakerTripped(count, window));
        }
    }

    /** Drops expired quarantines, and cooldown entries old enough that their consecutive-rollback count is moot. */
    private void prune(long now, SafetyConfig safety)
    {
        long keep = safety.cooldown_max.toMilliseconds();
        for (Iterator<Cooldown> it = cooldowns.values().iterator(); it.hasNext();)
            if (now >= it.next().untilMillis + keep)
                it.remove();
        quarantines.values().removeIf(until -> now >= until);
    }

    @Override
    public void recordRestart(String dedupeKey)
    {
        List<Runnable> effects = new ArrayList<>();
        synchronized (this)
        {
            long until = env.nowMillis() + env.config().safety.cooldown_after_rollback.toMilliseconds();
            setCooldown(dedupeKey, until, consecutiveRollbacks(dedupeKey), effects);
        }
        effects.forEach(Runnable::run);
    }

    @Override
    public void restoreState(String key, String value)
    {
        boolean expired = false;
        try
        {
            synchronized (this)
            {
                long now = env.nowMillis();
                if (key.startsWith(COOLDOWN_PREFIX))
                {
                    int sep = value.indexOf('|');
                    long until = Long.parseLong(value.substring(0, sep));
                    int consecutive = Integer.parseInt(value.substring(sep + 1));
                    if (now >= until + env.config().safety.cooldown_max.toMilliseconds())
                        expired = true;
                    else
                        cooldowns.put(key.substring(COOLDOWN_PREFIX.length()), new Cooldown(until, consecutive));
                }
                else if (key.startsWith(QUARANTINE_PREFIX))
                {
                    long until = Long.parseLong(value);
                    if (now >= until)
                        expired = true;
                    else
                        quarantines.put(key.substring(QUARANTINE_PREFIX.length()), until);
                }
            }
        }
        catch (RuntimeException e)
        {
            logger.warn("Ignoring unreadable autonomic state {}={}", key, value);
            expired = true;
        }
        if (expired)
            journal.deleteState(key);
    }

    /** Restart recovery: an action approved in the last hour by a previous run counts in the rate windows. */
    @Override
    public synchronized void restoreApproval(Action action, long approvedMillis)
    {
        if (approvedMillis <= env.nowMillis() - RATE_WINDOW_MILLIS)
            return;
        approvals.add(approvedMillis);
        actuatorApprovals.computeIfAbsent(action.actuator, k -> new ArrayDeque<>()).add(approvedMillis);
        if (action.scope == ActionScope.CLUSTER)
            clusterApprovals.add(approvedMillis);
    }

    /** {@code resume()}: forgets the outcomes the circuit breaker counted. Cooldowns and quarantines stay. */
    public synchronized void resetBreaker()
    {
        breaker.clear();
    }

    /** Policies currently quarantined, with the time the quarantine ends. */
    public synchronized Map<String, Long> quarantines()
    {
        long now = env.nowMillis();
        Map<String, Long> out = new LinkedHashMap<>();
        quarantines.forEach((policy, until) -> {
            if (now < until)
                out.put(policy, until);
        });
        return out;
    }

    /** Dedupe keys in cooldown, with the time the cooldown ends. */
    public synchronized Map<String, Long> cooldowns()
    {
        long now = env.nowMillis();
        Map<String, Long> out = new LinkedHashMap<>();
        cooldowns.forEach((key, c) -> {
            if (now < c.untilMillis)
                out.put(key, c.untilMillis);
        });
        return out;
    }

    /** Actions reserved: in flight, or awaiting approval. */
    public synchronized int reservations()
    {
        return reservations.size();
    }
}
