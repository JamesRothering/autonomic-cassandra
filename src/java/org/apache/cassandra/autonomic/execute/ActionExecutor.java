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
package org.apache.cassandra.autonomic.execute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.knowledge.KnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.PlanSink;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.autonomic.safety.Decision;
import org.apache.cassandra.autonomic.safety.Reasons;
import org.apache.cassandra.autonomic.safety.SafetyGovernor;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * The action state machine of overview 7.2 for node-scoped actions, with the verify/rollback contract of 7.3.
 *
 * <h3>Threading invariant</h3>
 * <ol>
 * <li><b>Actuator thread only</b> (every task given to the {@link StepScheduler}): the {@link #inflight} map, every
 *     {@link Execution} and its dead-man handle, and every call of {@code Actuator.apply / verify / rollback /
 *     overridden}. An action is in a state from APPLYING onwards only here, so execution needs no lock.</li>
 * <li><b>Proposal side</b> (control thread in {@link #accept} and {@link #onControlTick}; JMX threads in
 *     {@link #approve} and {@link #reject}): PROPOSED, REJECTED and APPROVED transitions, the {@link #held}
 *     proposals and the repeated-rejection filter, all under {@link #proposalLock}. No actuator is called and the
 *     actuator thread never takes this lock.</li>
 * <li><b>Crossing to the actuator thread</b> happens only by enqueueing a task: once per action as APPROVED, through
 *     the bounded hand-off ({@link #QUEUE_CAPACITY}, else {@code EXECUTOR_BUSY}); and unbounded but deduplicated
 *     for verify, abort and rollback requests, which name an action id and are no-ops if it is no longer in
 *     flight. Kill and shutdown are volatile flags read through {@link Host}.</li>
 * <li><b>Crossing back</b> happens only through immutable {@link ActionRecord}s published in the concurrent
 *     {@link #live} and {@link #history} collections, the {@link #currentStep} marker and counters. Anyone may
 *     read them; a record of an action is written by one side of the hand-off at a time.</li>
 * <li><b>Rollback never waits on the store or the control thread:</b> the dead-man task is scheduled on the
 *     actuator thread itself, and {@code Actuator.rollback} is called before the ROLLING_BACK and ROLLED_BACK
 *     records are written. Store calls never throw into the state machine and their failure never prevents a
 *     rollback.</li>
 * </ol>
 *
 * <p>Persistence: APPLYING (with the {@code inflight/<id>} key) and VERIFYING are written before their side effect
 * or continuation, and an action is not applied if that write fails. Every other transition is published in memory
 * first and then written; see the deviations noted at {@link #rollback}.
 *
 * <p>T2.8 seams: {@link ClusterExecution} receives approved cluster-scoped actions and cluster {@code inflight/*}
 * rows at recovery, and reports transitions through {@link Reporter}; nothing in the node path (baseline,
 * {@code MetricVerifier}, dead-man deadline) applies to them (FC4, FC5).
 */
public class ActionExecutor implements PlanSink
{
    private static final Logger logger = LoggerFactory.getLogger(ActionExecutor.class);

    /** Bound of the hand-off from the control thread to the actuator thread (overview 3.5). */
    public static final int QUEUE_CAPACITY = 16;
    public static final String INFLIGHT_PREFIX = "inflight/";
    public static final String PAUSED_KEY = "paused";
    /** Terminal records kept in memory for {@code KnowledgeView.recentActions}. */
    static final int HISTORY_CAPACITY = 256;
    /** An identical rejection (same dedupe key, policy and reason) is written to the store once per this interval. */
    public static final long REPEATED_REJECTION_INTERVAL_MILLIS = 10 * 60 * 1000L;
    private static final int REJECTION_FILTER_CAPACITY = 256;

    /** What the executor needs from the service. Every method is callable from any thread and must not block. */
    public interface Host
    {
        /** Null if actions may be applied, otherwise KILLED, PAUSED or NOT_ARMED. */
        String gate();

        /** Kill or shutdown was requested: anything in flight is being rolled back. */
        boolean killed();

        /** A rollback threw: raise {@code autonomic.action_failed} and pause the layer. */
        void actionFailed(ActionRecord record, String detail);
    }

    /** T2.9 seam (metrics, diagnostic events): called for every transition, on the thread that made it. */
    public interface TransitionListener
    {
        void onTransition(ActionRecord record, ActionState from, String detail);
    }

    /** T2.8 seam: how a {@link ClusterExecution} reports the transitions of a cluster-scoped action. */
    public interface Reporter
    {
        /**
         * Publishes and persists {@code next}; a terminal state releases the governor reservation with the given
         * outcome and deletes the {@code inflight/<id>} key. Returns false if the store write failed.
         */
        boolean transition(ActionRecord next, ActionState from, String detail, ActionOutcome terminalOutcome);
    }

    /**
     * T2.8 seam: execution of cluster-scoped actions (the ledger, {@code ClusterActuator.step}). All methods are
     * called on the actuator thread, except {@link #recover}, which runs during restart recovery before any step.
     * A cluster actuator owns its verification and returns a terminal state itself; no baseline, verdict or
     * dead-man deadline of this class applies to it (FC4).
     */
    public interface ClusterExecution
    {
        /** An APPROVED cluster-scoped action; the implementation writes its own {@code inflight/<id>} key. */
        void start(ActionRecord approved, Reporter reporter);

        /**
         * Kill or shutdown: release every claim this node owns at its next safe step, as
         * {@code COMMITTED_CLEANUP_PENDING} past a commit point (FC5).
         */
        void kill(String reason);

        /** Overview 7.6 step 3 for one cluster-scoped {@code inflight/*} row. Returns false to retry later. */
        boolean recover(UUID id, Optional<ActionRecord> row, Reporter reporter);
    }

    /** Result of {@link #recover}. */
    public static final class Recovery
    {
        /** False if {@code local_state} could not be read: the layer must not arm. */
        public final boolean scanned;
        /** False if some row could not be recovered yet; recovery must be run again. */
        public final boolean complete;
        /** Value of the persisted pause flag; null if not paused. */
        public final String paused;
        /** Actions marked rolled back because of the restart. */
        public final int rolledBack;

        Recovery(boolean scanned, boolean complete, String paused, int rolledBack)
        {
            this.scanned = scanned;
            this.complete = complete;
            this.paused = paused;
            this.rolledBack = rolledBack;
        }
    }

    /** One action from APPLYING to its terminal state. Actuator thread only. */
    private final class Execution
    {
        final Actuator actuator;
        final long startedMillis;
        ActionRecord record;
        /** What rollback gets: the result of apply, or the undo it recorded before throwing. Null if nothing changed. */
        Applied applied;
        boolean applyReturned;
        Runnable cancelDeadMan;

        Execution(Actuator actuator, ActionRecord record, long startedMillis)
        {
            this.actuator = actuator;
            this.record = record;
            this.startedMillis = startedMillis;
        }
    }

    private final class Context implements ActionContext
    {
        private final long deadline = clock.getAsLong() + config.get().safety.action_step_timeout.toMilliseconds();
        Map<String, String> undo;

        @Override
        public long stepDeadlineMillis()
        {
            return deadline;
        }

        @Override
        public long nowMillis()
        {
            return clock.getAsLong();
        }

        @Override
        public boolean killed()
        {
            return host.killed();
        }

        @Override
        public void recordUndo(Map<String, String> undo)
        {
            this.undo = new LinkedHashMap<>(undo);
        }
    }

    private static final class Held
    {
        final ActionRecord record;
        final long expiresMillis;

        Held(ActionRecord record, long expiresMillis)
        {
            this.record = record;
            this.expiresMillis = expiresMillis;
        }
    }

    private static final class Step
    {
        final String what;
        final long sinceMillis;

        Step(String what, long sinceMillis)
        {
            this.what = what;
            this.sinceMillis = sinceMillis;
        }
    }

    private final LongSupplier clock;
    private final Supplier<AutonomicConfig> config;
    private final SafetyGovernor governor;
    /** Nullable: nothing is persisted. */
    private final KnowledgeStore store;
    private final ActuatorRegistry actuators;
    private final StepScheduler scheduler;
    private final Host host;
    private final Supplier<KnowledgeView> knowledge;
    private final List<TransitionListener> listeners = new CopyOnWriteArrayList<>();
    private volatile ClusterExecution clusterExecution;

    // published state, readable from any thread (invariant 4)
    private final Map<UUID, ActionRecord> live = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<ActionRecord> history = new ConcurrentLinkedDeque<>();
    private final AtomicInteger historySize = new AtomicInteger();
    private final AtomicInteger queued = new AtomicInteger();
    private final Set<UUID> requested = ConcurrentHashMap.newKeySet();
    private final Map<String, LongAdder> rejections = new ConcurrentHashMap<>();
    private volatile Step currentStep;

    // proposal side (invariant 2)
    private final Object proposalLock = new Object();
    private final Map<UUID, Held> held = new LinkedHashMap<>();
    private final Map<String, Long> lastRejection = new LinkedHashMap<String, Long>(16, 0.75f, true)
    {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest)
        {
            return size() > REJECTION_FILTER_CAPACITY;
        }
    };

    // actuator thread only (invariant 1)
    private final Map<UUID, Execution> inflight = new LinkedHashMap<>();

    public ActionExecutor(LongSupplier clock, Supplier<AutonomicConfig> config, SafetyGovernor governor, KnowledgeStore store,
                          ActuatorRegistry actuators, StepScheduler scheduler, Host host, Supplier<KnowledgeView> knowledge)
    {
        this.clock = clock;
        this.config = config;
        this.governor = governor;
        this.store = store;
        this.actuators = actuators;
        this.scheduler = scheduler;
        this.host = host;
        this.knowledge = knowledge;
    }

    public void addListener(TransitionListener listener)
    {
        listeners.add(listener);
    }

    /** T2.8 seam; see {@link ClusterExecution}. */
    public void setClusterExecution(ClusterExecution clusterExecution)
    {
        this.clusterExecution = clusterExecution;
    }

    // ---------------------------------------------------------------------------------------------------------
    // proposal side: control thread and JMX threads
    // ---------------------------------------------------------------------------------------------------------

    /** Control thread. Never throws and never waits for the actuator thread. */
    @Override
    public void accept(Plan plan, KnowledgeView knowledge)
    {
        for (Action action : plan.actions)
        {
            try
            {
                propose(plan, action, knowledge);
            }
            catch (Throwable t)
            {
                handle("proposal of " + action, t);
            }
        }
    }

    /**
     * Evaluates one proposed action and hands it over if approved. Returns its record after the last transition made
     * here, or null if it was rejected for the same reason as a recent identical proposal and therefore not recorded.
     */
    public ActionRecord propose(Plan plan, Action action, KnowledgeView knowledge)
    {
        long now = clock.getAsLong();
        ActionRecord proposed = new ActionRecord(action, ActionState.PROPOSED, now, plan.diagnosisId, plan.policy, config.get().dry_run,
                                                 null, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), null, now);
        String planDetail = "plan " + plan.id + " by " + plan.policy + (plan.rationale == null ? "" : ": " + plan.rationale);
        synchronized (proposalLock)
        {
            Decision decision = evaluate(proposed, knowledge, false);
            switch (decision.type)
            {
                case REJECT:
                    count(decision.reason);
                    if (repeatedRejection(proposed, decision.reason, now))
                        return null;
                    journal(proposed, null, planDetail);
                    return finish(proposed.withState(ActionState.REJECTED, decision.reason, now), ActionState.PROPOSED, decision.detail);
                case HOLD_FOR_APPROVAL:
                    if (!journal(proposed, null, planDetail))
                    {
                        release(action);
                        return finish(proposed.withState(ActionState.REJECTED, Reasons.STORE_UNAVAILABLE, now), ActionState.PROPOSED, null);
                    }
                    held.put(action.id, new Held(proposed, now + config.get().safety.proposal_ttl.toMilliseconds()));
                    return proposed;
                default:
                    boolean written = journal(proposed, null, planDetail);
                    return handOver(proposed, knowledge, written, "governor");
            }
        }
    }

    /** Operator approval of a held proposal; any thread. */
    public ActionRecord approve(UUID id)
    {
        synchronized (proposalLock)
        {
            Held h = held.remove(id);
            if (h == null)
                throw new IllegalArgumentException("No action awaiting approval with id " + id);
            long now = clock.getAsLong();
            if (now >= h.expiresMillis)
                return expire(h, now);
            KnowledgeView view = knowledge.get();
            Decision decision = evaluate(h.record, view, true);
            if (decision.type != Decision.Type.APPROVE)
            {
                count(decision.reason);
                return finish(h.record.withState(ActionState.REJECTED, decision.reason, now), ActionState.PROPOSED, decision.detail);
            }
            return handOver(h.record, view, true, "operator");
        }
    }

    /** Operator rejection of a held proposal; any thread. */
    public ActionRecord reject(UUID id)
    {
        synchronized (proposalLock)
        {
            Held h = held.remove(id);
            if (h == null)
                throw new IllegalArgumentException("No action awaiting approval with id " + id);
            release(h.record.action);
            count(Reasons.OPERATOR_REJECTED);
            return finish(h.record.withState(ActionState.REJECTED, Reasons.OPERATOR_REJECTED, clock.getAsLong()), ActionState.PROPOSED, null);
        }
    }

    private Decision evaluate(ActionRecord proposal, KnowledgeView view, boolean operatorApproved)
    {
        try
        {
            Decision decision = operatorApproved ? governor.approve(proposal, view) : governor.evaluate(proposal, view);
            if (decision.type != Decision.Type.APPROVE)
                return decision;
            // conditions the governor cannot see; the reservation it just made is released on rejection
            Decision late = null;
            if (proposal.action.scope == ActionScope.CLUSTER && clusterExecution == null)
                late = Decision.reject(Reasons.NO_CLUSTER_EXECUTOR);
            else if (proposal.action.scope == ActionScope.NODE
                     && (proposal.action.verify == null || !MetricVerifier.baseline(proposal.action.verify, view).isPresent()))
                late = Decision.reject(Reasons.INSUFFICIENT_BASELINE);
            if (late == null)
                return decision;
            release(proposal.action);
            return late;
        }
        catch (Throwable t)
        {
            handle("governor evaluation of " + proposal.action, t);
            release(proposal.action);
            return Decision.reject(Reasons.GOVERNOR_ERROR, t.toString());
        }
    }

    /** PROPOSED to APPROVED and onto the actuator thread, or to REJECTED. Caller holds {@link #proposalLock}. */
    private ActionRecord handOver(ActionRecord proposed, KnowledgeView view, boolean storeWritten, String approver)
    {
        long now = clock.getAsLong();
        Action action = proposed.action;
        String rejection = null;
        if (!storeWritten)
            rejection = Reasons.STORE_UNAVAILABLE;
        else if (queued.incrementAndGet() > QUEUE_CAPACITY)
        {
            queued.decrementAndGet();
            rejection = Reasons.EXECUTOR_BUSY;
        }
        if (rejection == null)
        {
            ActionRecord approved = proposed.withState(ActionState.APPROVED, null, now);
            if (journal(approved, ActionState.PROPOSED, approver))
            {
                try
                {
                    scheduler.execute(step("start " + action.id, () -> start(action.id)));
                    return approved;
                }
                catch (Throwable t)
                {
                    handle("hand-over of " + action, t);
                    rejection = Reasons.EXECUTOR_BUSY;
                }
            }
            else
            {
                rejection = Reasons.STORE_UNAVAILABLE;
            }
            queued.decrementAndGet();
            proposed = approved;
        }
        release(action);
        count(rejection);
        return finish(proposed.withState(ActionState.REJECTED, rejection, now), proposed.state, null);
    }

    private ActionRecord expire(Held h, long now)
    {
        release(h.record.action);
        count(Reasons.PROPOSAL_TTL);
        return finish(h.record.withState(ActionState.REJECTED, Reasons.PROPOSAL_TTL, now), ActionState.PROPOSED, null);
    }

    /** Releases the governor's reservation of an action that was never applied. */
    private void release(Action action)
    {
        long now = clock.getAsLong();
        feedGovernor(action, new ActionOutcome(ActionState.REJECTED, null, null, Collections.emptyMap(), Collections.emptyMap(), now, now));
    }

    private void feedGovernor(Action action, ActionOutcome outcome)
    {
        try
        {
            governor.recordOutcome(action, outcome);
        }
        catch (Throwable t)
        {
            handle("governor outcome of " + action, t);
        }
    }

    /** True if the same rejection was recorded recently; such repeats are counted but not written. */
    private boolean repeatedRejection(ActionRecord proposed, String reason, long now)
    {
        // a rejection for an unwritable store is always attempted: it is the probe that finds the store writable again
        if (Reasons.STORE_UNAVAILABLE.equals(reason))
            return false;
        String key = proposed.action.dedupeKey + '|' + proposed.policy + '|' + reason;
        Long last = lastRejection.get(key);
        if (last != null && now - last < REPEATED_REJECTION_INTERVAL_MILLIS)
            return true;
        lastRejection.put(key, now);
        return false;
    }

    private void count(String reason)
    {
        rejections.computeIfAbsent(String.valueOf(reason), k -> new LongAdder()).increment();
    }

    /**
     * Control thread, every tick while armed (also while PAUSED: in-flight actions finish verification). Expires
     * proposals, and asks the actuator thread to verify actions whose observation window has elapsed or to roll back
     * actions whose guards have breached the abort tolerance (rule 8). Does not call actuators and does not wait.
     */
    public void onControlTick(KnowledgeView view)
    {
        long now = clock.getAsLong();
        synchronized (proposalLock)
        {
            for (Iterator<Held> it = held.values().iterator(); it.hasNext();)
            {
                Held h = it.next();
                if (now < h.expiresMillis)
                    continue;
                it.remove();
                expire(h, now);
            }
        }
        for (ActionRecord record : live.values())
        {
            VerifySpec spec = record.action.verify;
            if (record.state != ActionState.VERIFYING || record.action.scope != ActionScope.NODE || spec == null)
                continue;
            UUID id = record.id();
            long sinceApply = now - record.updatedMillis;
            try
            {
                if (sinceApply >= spec.observationWindowMillis)
                    request(id, "verify " + id, () -> verify(id));
                else if (MetricVerifier.guardBreached(spec, record.baseline, view, sinceApply).isPresent())
                    request(id, "abort " + id, () -> rollback(id, Reasons.EARLY_ABORT, null));
            }
            catch (Throwable t)
            {
                handle("verification trigger of " + id, t);
            }
        }
    }

    /** Enqueues at most one verify or abort request per action. */
    private void request(UUID id, String what, Runnable task)
    {
        if (!requested.add(id))
            return;
        try
        {
            scheduler.execute(step(what, task));
        }
        catch (Throwable t)
        {
            requested.remove(id);
            throw t;
        }
    }

    /**
     * Kill and shutdown, any thread: rolls back every in-flight node-scoped action as soon as the actuator thread is
     * free, and tells the cluster execution to release its claims. The future completes when that has happened.
     */
    public Future<Void> rollbackAll(String reason)
    {
        AsyncPromise<Void> done = new AsyncPromise<>();
        try
        {
            scheduler.execute(step("rollback all: " + reason, () -> {
                try
                {
                    for (Execution x : new ArrayList<>(inflight.values()))
                        rollback(x, reason, null);
                    ClusterExecution cluster = clusterExecution;
                    if (cluster != null)
                        cluster.kill(reason);
                }
                finally
                {
                    done.trySuccess(null);
                }
            }));
        }
        catch (Throwable t)
        {
            done.tryFailure(t);
        }
        return done;
    }

    /** Description of the actuator-thread step that has been running for longer than the timeout, or null. */
    public String stalledStep(long timeoutMillis)
    {
        Step step = currentStep;
        return step != null && clock.getAsLong() - step.sinceMillis > timeoutMillis ? step.what : null;
    }

    /** Wraps every actuator-thread task: stall marker, and no throwable escapes into the executor. */
    private Runnable step(String what, Runnable body)
    {
        return () -> {
            currentStep = new Step(what, clock.getAsLong());
            try
            {
                body.run();
            }
            catch (Throwable t)
            {
                handle("actuator step '" + what + '\'', t);
            }
            finally
            {
                currentStep = null;
            }
        };
    }

    // ---------------------------------------------------------------------------------------------------------
    // execution: actuator thread only
    // ---------------------------------------------------------------------------------------------------------

    /** APPROVED to APPLYING, apply, to VERIFYING with the dead-man deadline armed. */
    private void start(UUID id)
    {
        queued.decrementAndGet();
        ActionRecord approved = live.get(id);
        if (approved == null || approved.state != ActionState.APPROVED)
            return;
        Action action = approved.action;
        String gate = host.gate();
        Actuator actuator = actuators.get(action.actuator);
        if (gate != null || actuator == null)
        {
            rejectApproved(approved, gate != null ? gate : Reasons.ACTUATOR_DISABLED);
            return;
        }
        if (action.scope == ActionScope.CLUSTER)
        {
            ClusterExecution cluster = clusterExecution;
            if (cluster == null)
                rejectApproved(approved, Reasons.NO_CLUSTER_EXECUTOR);
            else
                cluster.start(approved, reporter);
            return;
        }

        // rule 5: baseline first
        Optional<Map<String, Double>> baseline = baseline(action);
        if (!baseline.isPresent())
        {
            rejectApproved(approved, Reasons.INSUFFICIENT_BASELINE);
            return;
        }
        long now = clock.getAsLong();
        ActionRecord applying = approved.withBaseline(baseline.get()).withState(ActionState.APPLYING, null, now);
        // write-ahead: the inflight key, then the record; nothing is applied unless both are durable
        if (!putInflight(applying) || !persist(applying, ActionState.APPROVED, null))
        {
            deleteInflight(id);
            rejectApproved(approved, Reasons.STORE_UNAVAILABLE);
            return;
        }
        Execution x = new Execution(actuator, applying, now);
        inflight.put(id, x);
        publish(applying, ActionState.APPROVED, null);

        Context ctx = new Context();
        try
        {
            x.applied = Objects.requireNonNull(actuator.apply(action, ctx), "apply returned null");
            x.applyReturned = true;
            x.record = applying = applying.withUndo(x.applied.undo);
        }
        catch (Throwable t)
        {
            handle("apply of " + action, t);
            // partial effect, if any, is described by the undo recorded before the throw
            x.applied = ctx.undo == null ? null : new Applied(ctx.undo);
            rollback(x, host.killed() ? Reasons.KILL : Reasons.APPLY_FAILED, t.toString());
            return;
        }

        if (host.killed())
        {
            rollback(x, Reasons.KILL, "killed while applying");
            return;
        }
        ActionRecord verifying = applying.withState(ActionState.VERIFYING, null, clock.getAsLong());
        if (!persist(verifying, ActionState.APPLYING, null))
        {
            // the undo could not be made durable: restore now rather than verify an unrecorded change
            rollback(x, Reasons.STORE_UNAVAILABLE, "undo not persisted");
            return;
        }
        x.record = verifying;
        publish(verifying, ActionState.APPLYING, null);
        try
        {
            // rule 9: scheduled here, on the actuator thread, independent of the control thread
            long delay = action.verify.observationWindowMillis + config.get().safety.verify_grace.toMilliseconds();
            x.cancelDeadMan = scheduler.schedule(step("deadline " + id, () -> rollback(id, Reasons.DEADLINE, null)), delay);
        }
        catch (Throwable t)
        {
            handle("dead-man scheduling of " + action, t);
            rollback(x, Reasons.DEADLINE, "dead-man deadline could not be scheduled: " + t);
        }
    }

    private Optional<Map<String, Double>> baseline(Action action)
    {
        try
        {
            return action.verify == null ? Optional.empty() : MetricVerifier.baseline(action.verify, knowledge.get());
        }
        catch (Throwable t)
        {
            handle("baseline of " + action, t);
            return Optional.empty();
        }
    }

    /** APPROVED to REJECTED: the actuator thread found a reason not to apply. Nothing was changed. */
    private void rejectApproved(ActionRecord approved, String reason)
    {
        count(reason);
        release(approved.action);
        finish(approved.withState(ActionState.REJECTED, reason, clock.getAsLong()), ActionState.APPROVED, null);
    }

    /** Verification at the end of the observation window, requested by the control thread. */
    private void verify(UUID id)
    {
        requested.remove(id);
        Execution x = inflight.get(id);
        if (x == null || x.record.state != ActionState.VERIFYING)
            return;
        Action action = x.record.action;
        if (overridden(x))
        {
            supersede(x);
            return;
        }
        Verdict verdict;
        Map<String, Double> result = Collections.emptyMap();
        try
        {
            KnowledgeView view = knowledge.get();
            verdict = Objects.requireNonNull(x.actuator.verify(action, x.applied, x.record.baseline, view));
            result = MetricVerifier.means(action.verify, view, action.verify.observationWindowMillis);
        }
        catch (Throwable t)
        {
            handle("verification of " + action, t);
            verdict = Verdict.INCONCLUSIVE;
        }
        x.record = x.record.withResult(result, verdict);
        // rule 7: only IMPROVED commits; rule 10: an irreversible action is verified for reporting only
        if (verdict == Verdict.IMPROVED || !x.actuator.reversible())
            commit(x, verdict == Verdict.IMPROVED ? null : Reasons.IRREVERSIBLE);
        else
            rollback(x, verdict.name(), null);
    }

    private boolean overridden(Execution x)
    {
        try
        {
            return x.applyReturned && x.actuator.overridden(x.record.action, x.applied);
        }
        catch (Throwable t)
        {
            // cannot tell: restoring what is in flight is the safer assumption
            handle("override check of " + x.record.action, t);
            return false;
        }
    }

    private void commit(Execution x, String reason)
    {
        cancelDeadMan(x);
        complete(x, x.record.withState(ActionState.COMMITTED, reason, clock.getAsLong()), null);
    }

    /** Rule 4: an operator changed the value; neither verification nor rollback touches it. */
    private void supersede(Execution x)
    {
        cancelDeadMan(x);
        complete(x, x.record.withState(ActionState.SUPERSEDED, Reasons.OVERRIDDEN, clock.getAsLong()), null);
    }

    private void rollback(UUID id, String reason, String detail)
    {
        requested.remove(id);
        Execution x = inflight.get(id);
        if (x != null)
            rollback(x, reason, detail);
    }

    /**
     * APPLYING or VERIFYING to ROLLING_BACK, then ROLLED_BACK, FAILED, or SUPERSEDED / COMMITTED where there is
     * nothing this layer may or can undo.
     *
     * <p>Deviation from the table in overview 7.2, deliberate: ROLLING_BACK is published in memory before
     * {@code Actuator.rollback} is called but written to the store after it, together with the terminal record, so
     * that an unavailable or slow store cannot delay a rollback (7.3 rule 3, 7.5). Recovery does not depend on that
     * write: the {@code inflight/<id>} key exists since APPLYING.
     */
    private void rollback(Execution x, String reason, String detail)
    {
        cancelDeadMan(x);
        Action action = x.record.action;
        ActionState from = x.record.state;
        if (from != ActionState.APPLYING && from != ActionState.VERIFYING)
            return;
        if (x.applyReturned && !x.actuator.reversible())
        {
            // rule 10: nothing can be undone; the terminal state is COMMITTED
            complete(x, x.record.withState(ActionState.COMMITTED, Reasons.IRREVERSIBLE, clock.getAsLong()), "not rolled back: " + reason);
            return;
        }
        if (overridden(x))
        {
            supersede(x);
            return;
        }
        ActionRecord rolling = x.record.withState(ActionState.ROLLING_BACK, reason, clock.getAsLong());
        x.record = rolling;
        publish(rolling, from, detail);
        Throwable failure = null;
        // nothing to restore if apply threw before recording an undo, or the actuator cannot undo
        if (x.applied != null && x.actuator.reversible())
        {
            try
            {
                x.actuator.rollback(action, x.applied, new Context());
            }
            catch (Throwable t)
            {
                failure = t;
                handle("rollback of " + action, t);
            }
        }
        persist(rolling, from, detail);
        if (failure == null)
        {
            complete(x, rolling.withState(ActionState.ROLLED_BACK, reason, clock.getAsLong()), null);
            return;
        }
        ActionRecord failed = rolling.withState(ActionState.FAILED, Reasons.ROLLBACK_FAILED, clock.getAsLong());
        complete(x, failed, failure.toString());
        host.actionFailed(failed, "rollback of " + action.dedupeKey + " threw " + failure);
    }

    private void cancelDeadMan(Execution x)
    {
        Runnable cancel = x.cancelDeadMan;
        x.cancelDeadMan = null;
        if (cancel != null)
        {
            try
            {
                cancel.run();
            }
            catch (Throwable t)
            {
                handle("dead-man cancellation", t);
            }
        }
    }

    /** Terminal transition of an execution: record, inflight key, governor feedback. */
    private void complete(Execution x, ActionRecord terminal, String detail)
    {
        ActionState from = x.record.state;
        UUID id = terminal.id();
        x.record = terminal;
        inflight.remove(id);
        requested.remove(id);
        publish(terminal, from, detail);
        persist(terminal, from, detail);
        deleteInflight(id);
        feedGovernor(terminal.action, new ActionOutcome(terminal.state, terminal.verdict, terminal.stateReason, terminal.baseline,
                                                        terminal.result, x.startedMillis, terminal.updatedMillis));
    }

    private final Reporter reporter = (next, from, detail, terminalOutcome) -> {
        publish(next, from, detail);
        boolean written = persist(next, from, detail);
        if (next.state.isTerminal())
        {
            deleteInflight(next.id());
            feedGovernor(next.action, Objects.requireNonNull(terminalOutcome));
        }
        return written;
    };

    // ---------------------------------------------------------------------------------------------------------
    // records: any thread, one writer per action at a time
    // ---------------------------------------------------------------------------------------------------------

    /** Publishes then persists a transition that needs no write-ahead. Returns false if the store write failed. */
    private boolean journal(ActionRecord next, ActionState from, String detail)
    {
        publish(next, from, detail);
        return persist(next, from, detail);
    }

    private ActionRecord finish(ActionRecord terminal, ActionState from, String detail)
    {
        journal(terminal, from, detail);
        return terminal;
    }

    private void publish(ActionRecord next, ActionState from, String detail)
    {
        if (next.state.isTerminal())
        {
            live.remove(next.id());
            remember(next);
        }
        else
        {
            live.put(next.id(), next);
        }
        logger.info("Autonomic action {} ({}) {} -> {}{}{}", next.id(), next.action.dedupeKey, from == null ? "-" : from, next.state,
                    next.stateReason == null ? "" : " reason " + next.stateReason, detail == null ? "" : ": " + detail);
        for (TransitionListener listener : listeners)
        {
            try
            {
                listener.onTransition(next, from, detail);
            }
            catch (Throwable t)
            {
                handle("transition listener", t);
            }
        }
    }

    private void remember(ActionRecord terminal)
    {
        history.addLast(terminal);
        if (historySize.incrementAndGet() > HISTORY_CAPACITY && history.pollFirst() != null)
            historySize.decrementAndGet();
    }

    private boolean persist(ActionRecord record, ActionState from, String detail)
    {
        if (store == null)
            return true;
        try
        {
            return store.recordTransition(record, from, detail);
        }
        catch (Throwable t)
        {
            handle("store write of " + record, t);
            return false;
        }
    }

    /** {@code inflight/<id>} carries what recovery needs even if the action row cannot be read. */
    private boolean putInflight(ActionRecord record)
    {
        if (store == null)
            return true;
        try
        {
            Action a = record.action;
            String value = "v1|" + record.proposedMillis + '|' + a.scope + '|' + (record.policy == null ? "" : record.policy) + '|' + a.dedupeKey;
            return store.putState(INFLIGHT_PREFIX + a.id, value, clock.getAsLong());
        }
        catch (Throwable t)
        {
            handle("inflight key of " + record, t);
            return false;
        }
    }

    private boolean deleteInflight(UUID id)
    {
        if (store == null)
            return true;
        try
        {
            return store.deleteState(INFLIGHT_PREFIX + id);
        }
        catch (Throwable t)
        {
            handle("inflight key of " + id, t);
            return false;
        }
    }

    private static void handle(String what, Throwable t)
    {
        if (t instanceof Error)
        {
            try
            {
                JVMStabilityInspector.inspectThrowable(t);
            }
            catch (Throwable ignored)
            {
                // the inspector may rethrow; the state machine must still complete its transition
            }
        }
        logger.warn("Autonomic {} failed: {}", what, t.toString(), t);
    }

    // ---------------------------------------------------------------------------------------------------------
    // restart recovery (overview 7.6): before the first step, on the starting or the control thread
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Reads {@code local_state} once and recovers from it: the pause flag, cooldowns and quarantines, and every
     * {@code inflight/*} action. A restart has already restored node-scoped knobs, so those actions are marked
     * {@code ROLLED_BACK(RESTART)} without calling {@code rollback}, and their dedupe keys get a cooldown.
     * Idempotent; must be repeated until {@link Recovery#complete}.
     *
     * @param notRestored ids of actions a previous run in this JVM stopped without rolling back; for these the
     *                    knob was <em>not</em> restored, so they are marked FAILED and reported to the host
     */
    public Recovery recover(Set<UUID> notRestored)
    {
        if (store == null)
            return new Recovery(true, true, null, 0);
        Optional<Map<String, String>> scan;
        try
        {
            scan = store.tryScanState("");
        }
        catch (Throwable t)
        {
            handle("recovery scan", t);
            scan = Optional.empty();
        }
        if (!scan.isPresent())
            return new Recovery(false, false, null, 0);

        boolean complete = true;
        int rolledBack = 0;
        Set<UUID> inflightIds = new java.util.HashSet<>();
        for (Map.Entry<String, String> e : scan.get().entrySet())
        {
            String key = e.getKey();
            try
            {
                if (key.startsWith(INFLIGHT_PREFIX))
                {
                    UUID id = UUID.fromString(key.substring(INFLIGHT_PREFIX.length()));
                    inflightIds.add(id);
                    if (recoverInflight(key, id, e.getValue(), notRestored))
                        rolledBack++;
                    else
                        complete = false;
                }
                else if (!PAUSED_KEY.equals(key) && e.getValue() != null)
                {
                    governor.restoreState(key, e.getValue());
                }
            }
            catch (Throwable t)
            {
                handle("recovery of " + key, t);
                complete = false;
            }
        }
        if (complete)
            restoreHistory(inflightIds);
        return new Recovery(true, complete, scan.get().get(PAUSED_KEY), rolledBack);
    }

    /** Returns false if the row must be retried. */
    private boolean recoverInflight(String key, UUID id, String value, Set<UUID> notRestored)
    {
        String[] parts = value == null ? new String[0] : value.split("\\|", 5);
        if (parts.length != 5 || !"v1".equals(parts[0]))
        {
            logger.warn("Dropping unreadable autonomic state {}={}", key, value);
            return store.deleteState(key);
        }
        long proposedMillis = Long.parseLong(parts[1]);
        String dedupeKey = parts[4];
        long readFailures = store.readFailures();
        Optional<ActionRecord> row = store.getAction(id, proposedMillis);
        if (!row.isPresent() && store.readFailures() != readFailures)
            return false;

        if (ActionScope.CLUSTER.name().equals(parts[2]))
        {
            ClusterExecution cluster = clusterExecution;
            if (cluster != null)
                return cluster.recover(id, row, reporter);
            logger.warn("Autonomic action {} is cluster-scoped and no cluster execution exists; leaving {} in place", id, key);
            return true;
        }

        boolean stopped = notRestored.contains(id);
        long now = clock.getAsLong();
        if (row.isPresent() && !row.get().state.isTerminal())
        {
            ActionRecord before = row.get();
            boolean applied = before.state == ActionState.APPLYING || before.state == ActionState.VERIFYING || before.state == ActionState.ROLLING_BACK;
            ActionRecord after = stopped ? before.withState(ActionState.FAILED, Reasons.STOPPED_WITHOUT_ROLLBACK, now)
                                         : before.withState(applied ? ActionState.ROLLED_BACK : ActionState.REJECTED, Reasons.RESTART, now);
            if (!persist(after, before.state, null))
                return false;
            publish(after, before.state, null);
            governor.recordRestart(dedupeKey);
            if (stopped)
                host.actionFailed(after, "the layer was stopped while " + dedupeKey + " was applied and its rollback did not complete");
        }
        else if (!row.isPresent())
        {
            store.appendEvent(id, null, ActionState.ROLLED_BACK, Reasons.RESTART + ": action row not found");
            governor.recordRestart(dedupeKey);
        }
        return store.deleteState(key);
    }

    /**
     * Seeds the in-memory history and the governor's rate windows from the last hour of the store, and closes
     * proposals of a previous run that never reached the actuator thread. Best effort.
     */
    private void restoreHistory(Set<UUID> inflightIds)
    {
        long now = clock.getAsLong();
        long since = now - Math.max(60 * 60 * 1000L, config.get().safety.proposal_ttl.toMilliseconds());
        List<ActionRecord> recent = new ArrayList<>(store.recentActions(since, now, HISTORY_CAPACITY));
        Collections.reverse(recent);
        for (ActionRecord record : recent)
        {
            if (live.containsKey(record.id()) || contains(history, record.id()) || inflightIds.contains(record.id()))
                continue;
            if (!record.state.isTerminal())
            {
                ActionState from = record.state;
                record = record.withState(ActionState.REJECTED, Reasons.RESTART, now);
                persist(record, from, null);
            }
            else if (record.state != ActionState.REJECTED)
            {
                governor.restoreApproval(record.action, record.proposedMillis);
            }
            remember(record);
        }
    }

    private static boolean contains(Collection<ActionRecord> records, UUID id)
    {
        for (ActionRecord r : records)
            if (r.id().equals(id))
                return true;
        return false;
    }

    // ---------------------------------------------------------------------------------------------------------
    // read side: any thread
    // ---------------------------------------------------------------------------------------------------------

    /** Records whose latest transition is within the lookback: non-terminal ones and the in-memory history. */
    public Collection<ActionRecord> recentActions(long lookbackMillis)
    {
        long since = clock.getAsLong() - lookbackMillis;
        List<ActionRecord> out = new ArrayList<>();
        for (ActionRecord r : history)
            if (r.updatedMillis >= since)
                out.add(r);
        for (ActionRecord r : live.values())
            if (r.updatedMillis >= since)
                out.add(r);
        return out;
    }

    /** Non-terminal actions: awaiting approval, queued, or in flight. */
    public Collection<ActionRecord> liveActions()
    {
        return new ArrayList<>(live.values());
    }

    /** Ids of actions that are applied and not yet committed or restored. */
    public Set<UUID> appliedActions()
    {
        Set<UUID> out = new java.util.HashSet<>();
        for (ActionRecord r : live.values())
            if (r.state == ActionState.APPLYING || r.state == ActionState.VERIFYING || r.state == ActionState.ROLLING_BACK)
                out.add(r.id());
        return out;
    }

    /** The record of an action that is live or in the in-memory history. */
    public Optional<ActionRecord> record(UUID id)
    {
        ActionRecord r = live.get(id);
        if (r != null)
            return Optional.of(r);
        for (Iterator<ActionRecord> it = history.descendingIterator(); it.hasNext();)
        {
            ActionRecord h = it.next();
            if (h.id().equals(id))
                return Optional.of(h);
        }
        return Optional.empty();
    }

    /** T2.9 seam: rejections by reason since start, including repeats that were not written to the store. */
    public Map<String, Long> rejectionCounts()
    {
        Map<String, Long> out = new LinkedHashMap<>();
        rejections.forEach((reason, n) -> out.put(reason, n.sum()));
        return out;
    }

    public int queuedActions()
    {
        return queued.get();
    }
}
