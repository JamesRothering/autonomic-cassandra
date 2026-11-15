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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator.Outcome;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator.Status;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.Recent;
import org.apache.cassandra.autonomic.knowledge.KnowledgeStore;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.safety.Reasons;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Execution of cluster-scoped actions: drives a {@link ClusterActuator} under a claim of the cluster ledger
 * (overview 6.4, 7.6 step 3; FC2, FC4, FC5).
 *
 * <p>Local states: APPROVED, then APPLYING once the claim is won (the {@code inflight/<id>} key is written before
 * the claim is requested), then one of COMMITTED, ROLLED_BACK or FAILED; REJECTED if the claim is not won. Every
 * phase change is an APPLYING to APPLYING transition in the audit trail.
 *
 * <p>Rules:
 * <ul>
 * <li>Before every step the claim is read from the ledger. If this node no longer owns it, or its own clock has
 *     reached {@code leaseExpiresAt - lease_safety_margin}, the action becomes FAILED with reason CLAIM_LOST and
 *     the actuator is never called again for it. Nothing is undone and nothing more is committed: the new owner's
 *     {@code recover} decides.</li>
 * <li>An actuator may end an action it can neither complete nor undo with {@link StepResult#failed}: the claim is
 *     released as FAILED and the local record becomes FAILED with reason ACTUATOR_FAILED.</li>
 * <li>A step result is carried out by a ledger commit and counts only once the ledger shows it. Until then the same
 *     commit is retried, without calling the actuator again for a plain advance.</li>
 * <li>The claim is released in the ledger before the local record becomes terminal, so a crash in between never
 *     leaves a finished action to be "recovered" by another node.</li>
 * <li>Kill, shutdown and a step that keeps throwing: past the commit point the claim is released as
 *     COMMITTED_CLEANUP_PENDING; before it the actuator's abort path runs until it reports a terminal result.</li>
 * <li>A lease is renewed every third of its length by the periodic tick, which also takes over expired claims
 *     of any node when this node is the preferred proposer, and then drives {@code recover}.</li>
 * </ul>
 *
 * <p>Threading: everything runs on the actuator thread (through the {@link StepScheduler}), which is where commits
 * to the metadata log are allowed to block for up to {@code action_step_timeout}. {@link #recover} and
 * {@link #onPhase} are called on other threads and only enqueue.
 */
public class ClusterActionExecution implements ActionExecutor.ClusterExecution, ClusterCoordinator.PhaseListener
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterActionExecution.class);

    /** Phase of a claim before the first step of its actuator. */
    public static final String INITIAL_PHASE = "CLAIMED";
    // final states written to the ledger
    public static final String FINAL_COMMITTED = "COMMITTED";
    public static final String FINAL_COMMITTED_CLEANUP_PENDING = "COMMITTED_CLEANUP_PENDING";
    public static final String FINAL_ROLLED_BACK = "ROLLED_BACK";
    public static final String FINAL_FAILED = "FAILED";
    /** Policy name of the local record of a takeover. */
    public static final String TAKEOVER_POLICY = "takeover";
    static final int MAX_STEP_FAILURES = 3;
    /** Immediate steps that kill and shutdown run in the calling thread. */
    static final int MAX_STOP_STEPS = 10;

    private enum Mode
    {
        STEP, ABORT, RECOVER
    }

    /** One cluster action on its owner. Actuator thread only. */
    private final class Run
    {
        final ClusterActuator actuator;
        final Lane lane;
        final long startedMillis;
        ActionRecord record;
        Claim claim;
        Mode mode;
        /** Why the action is being stopped: KILL, SHUTDOWN or APPLY_FAILED; null while it runs normally. */
        String stopReason;
        /** Reason of a ROLLED_BACK that recovery arrives at: RESTART or RECOVERY; null for an action started here. */
        String recoveryReason;
        /** A result whose ledger commit is not visible yet. */
        StepResult pending;
        int failures;
        boolean done;
        /** Invalidates step tasks scheduled earlier. */
        int generation;
        /** Delay of the step scheduled last. */
        long nextDelayMillis = -1;

        Run(ClusterActuator actuator, Lane lane, ActionRecord record, Claim claim, Mode mode, long startedMillis)
        {
            this.actuator = actuator;
            this.lane = lane;
            this.record = record;
            this.claim = claim;
            this.mode = mode;
            this.startedMillis = startedMillis;
        }

        UUID id()
        {
            return record.id();
        }
    }

    private final class Context implements ActionContext
    {
        private final Run run;
        private final long deadline = clock.getAsLong() + config.get().safety.action_step_timeout.toMilliseconds();

        Context(Run run)
        {
            this.run = run;
        }

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
            return host.killed() || (run != null && run.stopReason != null);
        }

        @Override
        public void recordUndo(Map<String, String> undo)
        {
            // cluster actuators keep what recovery needs in the claim, not in a local undo record
        }
    }

    private final LongSupplier clock;
    private final Supplier<AutonomicConfig> config;
    private final ClusterCoordinator coordinator;
    private final ActuatorRegistry actuators;
    private final StepScheduler scheduler;
    /** Nullable: nothing is persisted. */
    private final KnowledgeStore store;
    private final ActionExecutor.Host host;
    private final Supplier<ActionExecutor.Reporter> reporter;

    // actuator thread only
    private final Map<UUID, Run> runs = new LinkedHashMap<>();
    /** Ids handed to the actuator thread by {@link #recover}; any thread. */
    private final Set<UUID> recovering = ConcurrentHashMap.newKeySet();
    private volatile boolean stopped;
    private volatile Runnable cancelTick;

    public ClusterActionExecution(LongSupplier clock, Supplier<AutonomicConfig> config, ClusterCoordinator coordinator, ActuatorRegistry actuators,
                                  StepScheduler scheduler, KnowledgeStore store, ActionExecutor.Host host, Supplier<ActionExecutor.Reporter> reporter)
    {
        this.clock = clock;
        this.config = config;
        this.coordinator = coordinator;
        this.actuators = actuators;
        this.scheduler = scheduler;
        this.store = store;
        this.host = host;
        this.reporter = reporter;
    }

    /** Starts the periodic tick (renewal, loss detection, takeover). */
    public void start()
    {
        stopped = false;
        scheduleTick();
    }

    /** Stops the tick; claims stay in the ledger for restart recovery or takeover. */
    public void stop()
    {
        stopped = true;
        Runnable cancel = cancelTick;
        if (cancel != null)
            cancel.run();
    }

    /** Ids of the cluster actions this node is driving. Actuator thread, or tests. */
    public Set<UUID> running()
    {
        return Collections.unmodifiableSet(runs.keySet());
    }

    private long leaseMillis()
    {
        return config.get().coordination.lease.toMilliseconds();
    }

    /** Tick period: often enough to renew at a third of the lease and to notice the safety margin in time. */
    long tickMillis()
    {
        long margin = config.get().coordination.lease_safety_margin.toMilliseconds();
        return Math.max(50, Math.min(10_000, Math.min(leaseMillis() / 6, Math.max(1, margin / 2))));
    }

    private long retryDelayMillis()
    {
        return Math.max(50, Math.min(5_000, leaseMillis() / 60));
    }

    // ---------------------------------------------------------------------------------------------------------
    // start of an approved action
    // ---------------------------------------------------------------------------------------------------------

    @Override
    public void start(ActionRecord approved, ActionExecutor.Reporter reporter)
    {
        Action action = approved.action;
        Actuator actuator = actuators.get(action.actuator);
        if (!(actuator instanceof ClusterActuator))
        {
            reject(approved, Reasons.VALIDATION, action.actuator + " is not a cluster actuator");
            return;
        }
        Lane lane = ((ClusterActuator) actuator).lane();
        if (!coordinator.enabled())
        {
            reject(approved, Reasons.CLUSTER_DISABLED, null);
            return;
        }
        if (!coordinator.preferredProposer())
        {
            reject(approved, Reasons.NODE_STATE, "this node is not the preferred proposer");
            return;
        }
        Claim held = coordinator.ledger().claims.get(lane);
        if (held != null)
        {
            reject(approved, Reasons.CONCURRENCY, "lane " + lane + " is held by action " + held.actionId);
            return;
        }
        // write-ahead: the inflight key exists before the claim can
        if (!putInflight(approved))
        {
            reject(approved, Reasons.STORE_UNAVAILABLE, null);
            return;
        }
        Outcome outcome = coordinator.claim(lane, action.id, action.actuator, action.target.asString(), INITIAL_PHASE, Collections.emptyMap());
        if (!outcome.ok())
        {
            deleteInflight(action.id);
            reject(approved, Reasons.CLAIM_REJECTED, outcome.detail);
            return;
        }
        long now = clock.getAsLong();
        Run run = new Run((ClusterActuator) actuator, lane, approved, outcome.claim, Mode.STEP, now);
        runs.put(action.id, run);
        ActionRecord applying = approved.withState(ActionState.APPLYING, null, now);
        boolean written = transition(run, applying, "claimed lane " + lane);
        if (!written)
        {
            // the claim is held but cannot be recorded: nothing was done under it, give it back
            run.stopReason = Reasons.STORE_UNAVAILABLE;
            run.pending = StepResult.rolledBack("action record could not be written");
        }
        scheduleStep(run, 0);
    }

    private void reject(ActionRecord approved, String reason, String detail)
    {
        long now = clock.getAsLong();
        ActionRecord rejected = approved.withState(ActionState.REJECTED, reason, now);
        reporter.get().transition(rejected, approved.state, detail,
                                  new ActionOutcome(ActionState.REJECTED, null, reason, Collections.emptyMap(), Collections.emptyMap(), now, now));
    }

    // ---------------------------------------------------------------------------------------------------------
    // steps
    // ---------------------------------------------------------------------------------------------------------

    private void scheduleStep(Run run, long delayMillis)
    {
        int generation = ++run.generation;
        run.nextDelayMillis = delayMillis;
        Runnable task = guarded("cluster step of " + run.id(), () -> {
            if (!run.done && run.generation == generation)
                runStep(run);
        });
        try
        {
            if (delayMillis <= 0)
                scheduler.execute(task);
            else
                scheduler.schedule(task, delayMillis);
        }
        catch (Throwable t)
        {
            // shutting down: the claim stays for restart recovery or takeover
            logger.info("Autonomic cluster action {} could not schedule its next step: {}", run.id(), t.toString());
        }
    }

    private Runnable guarded(String what, Runnable body)
    {
        return () -> {
            try
            {
                body.run();
            }
            catch (Throwable t)
            {
                handle(what, t);
            }
        };
    }

    private void runStep(Run run)
    {
        if (!sync(run))
            return;
        if (run.stopReason == null && host.killed())
            run.stopReason = Reasons.KILL;

        StepResult result = run.pending;
        try
        {
            if (run.stopReason != null && run.mode != Mode.ABORT && (result == null || !result.isTerminal()))
            {
                // FC5: past the commit point nothing is undone
                if (run.actuator.pastCommitPoint(run.claim))
                {
                    result = StepResult.committedCleanupPending("stopped past the commit point: " + run.stopReason);
                }
                else
                {
                    run.mode = Mode.ABORT;
                    result = null;
                }
                run.pending = null;
            }
            if (result == null)
            {
                Context ctx = new Context(run);
                switch (run.mode)
                {
                    case STEP:
                        result = run.actuator.step(run.record.action, run.claim, ctx);
                        break;
                    case RECOVER:
                        result = run.actuator.recover(run.claim, ctx);
                        break;
                    default:
                        result = run.actuator.abort(run.record.action, run.claim, ctx);
                }
                Objects.requireNonNull(result, "step returned null");
                if (!result.isTerminal() && result.kind != StepResult.Kind.STAY)
                {
                    String invalid = Claim.invalidState(result.state);
                    if (invalid != null)
                        throw new IllegalArgumentException(invalid);
                }
                run.failures = 0;
            }
        }
        catch (Throwable t)
        {
            stepFailed(run, t);
            return;
        }
        carryOut(run, result);
    }

    /** Reads the claim from the ledger. Returns false if the action ended here: claim lost, or already released. */
    private boolean sync(Run run)
    {
        Claim own = coordinator.ownClaim(run.id());
        if (own == null)
        {
            Recent released = coordinator.ledger().recent(run.id());
            if (released != null && run.pending != null && run.pending.isTerminal() && released.finalState.equals(finalState(run.pending)))
            {
                // our own release, whose commit was not visible at the time
                finish(run, run.pending);
                return false;
            }
            Claim held = coordinator.ledger().claim(run.id());
            lost(run, released != null ? "released as " + released.finalState
                                       : held != null ? "claim is owned by node " + held.owner.id() : "claim is no longer in the ledger");
            return false;
        }
        run.claim = own;
        if (!coordinator.mayAct(own))
        {
            lost(run, "lease expires at " + own.leaseExpiresAtMillis + " and could not be renewed before the safety margin");
            return false;
        }
        return true;
    }

    private void carryOut(Run run, StepResult result)
    {
        switch (result.kind)
        {
            case STAY:
                run.pending = null;
                scheduleStep(run, result.delayMillis);
                return;
            case ADVANCE:
            case COMMIT:
            {
                Outcome outcome;
                try
                {
                    outcome = result.kind == StepResult.Kind.ADVANCE
                              ? coordinator.advance(run.claim, result.phase, result.state)
                              : coordinator.commitInstead(run.claim, result.transformation, result.phase, result.state);
                }
                catch (Throwable t)
                {
                    stepFailed(run, t);
                    return;
                }
                if (outcome.status == Status.LOST)
                {
                    lost(run, outcome.detail);
                    return;
                }
                if (outcome.status == Status.OK)
                {
                    run.claim = outcome.claim;
                    run.pending = null;
                    transition(run, run.record.withState(ActionState.APPLYING, run.record.stateReason, clock.getAsLong()),
                               "phase " + outcome.claim.phase + " (step " + outcome.claim.step + ')' + (result.detail == null ? "" : ": " + result.detail));
                    scheduleStep(run, result.delayMillis);
                    return;
                }
                if (outcome.claim != null)
                    run.claim = outcome.claim;
                // a plain advance is retried as it is; a transformation of the actuator embeds the step it expected,
                // so the actuator is asked again
                run.pending = result.kind == StepResult.Kind.ADVANCE ? result : null;
                scheduleStep(run, retryDelayMillis());
                return;
            }
            default:
            {
                Outcome outcome = coordinator.release(run.claim, finalState(result));
                if (outcome.status == Status.LOST)
                    lost(run, outcome.detail);
                else if (outcome.status == Status.OK)
                    finish(run, result);
                else
                {
                    run.pending = result;
                    scheduleStep(run, retryDelayMillis());
                }
            }
        }
    }

    private static String finalState(StepResult terminal)
    {
        switch (terminal.kind)
        {
            case COMMITTED:
                return FINAL_COMMITTED;
            case COMMITTED_CLEANUP_PENDING:
                return FINAL_COMMITTED_CLEANUP_PENDING;
            case FAILED:
                return FINAL_FAILED;
            default:
                return FINAL_ROLLED_BACK;
        }
    }

    private void stepFailed(Run run, Throwable t)
    {
        handle(run.mode + " of cluster action " + run.id(), t);
        run.pending = null;
        run.failures++;
        if (run.failures < MAX_STEP_FAILURES)
        {
            scheduleStep(run, retryDelayMillis() * run.failures);
            return;
        }
        if (run.mode == Mode.STEP && run.stopReason == null)
        {
            // stop the action: abort before the commit point, COMMITTED_CLEANUP_PENDING past it
            run.stopReason = Reasons.APPLY_FAILED;
            run.failures = 0;
            transition(run, run.record.withState(ActionState.APPLYING, Reasons.APPLY_FAILED, clock.getAsLong()), "step keeps failing: " + t);
            scheduleStep(run, retryDelayMillis());
            return;
        }
        // the claim is deliberately not released: the cluster is in a state only recover() of the next owner can
        // judge, and the lease expiry hands it over
        end(run, ActionState.FAILED, Reasons.RECOVERY_FAILED, null, run.mode + " keeps failing: " + t, true);
    }

    /** Overview 6.4: the claim is gone or can no longer be trusted. No further side effects. */
    private void lost(Run run, String detail)
    {
        end(run, ActionState.FAILED, Reasons.CLAIM_LOST, null, detail, true);
    }

    private void finish(Run run, StepResult terminal)
    {
        switch (terminal.kind)
        {
            case COMMITTED:
                end(run, ActionState.COMMITTED, null, terminal.verdict, terminal.detail, false);
                break;
            case COMMITTED_CLEANUP_PENDING:
                end(run, ActionState.COMMITTED, Reasons.COMMITTED_CLEANUP_PENDING, terminal.verdict, terminal.detail, false);
                break;
            case FAILED:
                // the claim is released: an operator, not the next owner, has to judge the cluster
                end(run, ActionState.FAILED, Reasons.ACTUATOR_FAILED, null, terminal.detail, true);
                break;
            default:
                String reason = run.stopReason != null ? run.stopReason : run.recoveryReason != null ? run.recoveryReason : Reasons.ACTUATOR_ROLLBACK;
                end(run, ActionState.ROLLED_BACK, reason, terminal.verdict, terminal.detail, false);
        }
    }

    private void end(Run run, ActionState state, String reason, Verdict verdict, String detail, boolean failed)
    {
        run.done = true;
        run.generation++;
        runs.remove(run.id());
        recovering.remove(run.id());
        long now = clock.getAsLong();
        ActionState from = run.record.state;
        ActionRecord terminal = (verdict == null ? run.record : run.record.withResult(Collections.emptyMap(), verdict)).withState(state, reason, now);
        run.record = terminal;
        reporter.get().transition(terminal, from, detail,
                                  new ActionOutcome(state, verdict, reason, Collections.emptyMap(), Collections.emptyMap(), run.startedMillis, now));
        if (failed)
            host.actionFailed(terminal, "cluster action " + terminal.action.dedupeKey + ": " + reason + (detail == null ? "" : " (" + detail + ')'));
    }

    private boolean transition(Run run, ActionRecord next, String detail)
    {
        ActionState from = run.record.state;
        run.record = next;
        return reporter.get().transition(next, from, detail, null);
    }

    // ---------------------------------------------------------------------------------------------------------
    // kill and shutdown
    // ---------------------------------------------------------------------------------------------------------

    @Override
    public void kill(String reason)
    {
        for (Run run : new ArrayList<>(runs.values()))
        {
            if (run.done)
                continue;
            if (run.stopReason == null)
                run.stopReason = reason;
            // the first stop step runs now: a shutdown waits for this call only
            run.generation++;
            try
            {
                runStep(run);
                // a stop that takes several immediate steps (a scale-out: cancel, then release) is finished here, because
                // a shutdown waits for this call only and then interrupts the actuator thread
                for (int i = 1; i < MAX_STOP_STEPS && !run.done && run.nextDelayMillis <= 0; i++)
                    runStep(run);
            }
            catch (Throwable t)
            {
                handle("stop of cluster action " + run.id(), t);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // periodic tick: renewal, loss detection, abandoned claims, takeover
    // ---------------------------------------------------------------------------------------------------------

    private void scheduleTick()
    {
        if (stopped)
            return;
        try
        {
            cancelTick = scheduler.schedule(guarded("cluster tick", this::tick), tickMillis());
        }
        catch (Throwable t)
        {
            logger.info("Autonomic cluster tick could not be scheduled: {}", t.toString());
        }
    }

    /** Actuator thread. Visible for tests, which call it instead of waiting for the timer. */
    public void tick()
    {
        try
        {
            if (stopped)
                return;
            for (Run run : new ArrayList<>(runs.values()))
            {
                if (run.done || !sync(run))
                    continue;
                if (coordinator.renewalDue(run.claim) && (run.pending == null || !run.pending.isTerminal()))
                {
                    Outcome outcome = coordinator.renew(run.claim);
                    if (outcome.status == Status.LOST)
                        lost(run, outcome.detail);
                    else if (outcome.claim != null)
                        run.claim = outcome.claim;
                }
            }
            coordinator.releaseAbandoned();
            takeOverExpired();
        }
        finally
        {
            scheduleTick();
        }
    }

    /** Overview 6.4: an expired claim is replaced by a RECOVERING claim of this node and its actuator recovers. */
    private void takeOverExpired()
    {
        // a recovery is new work for this node: not while paused, killed or not armed
        if (host.gate() != null || !coordinator.enabled())
            return;
        Map<Lane, Claim> expired = coordinator.expiredClaims();
        if (expired.isEmpty() || !coordinator.preferredProposer())
            return;
        for (Map.Entry<Lane, Claim> e : expired.entrySet())
        {
            Lane lane = e.getKey();
            Claim old = e.getValue();
            Actuator actuator = actuators.get(old.actuator);
            if (runs.containsKey(old.actionId) || !(actuator instanceof ClusterActuator) || ((ClusterActuator) actuator).lane() != lane
                || !actuators.isEnabled(old.actuator, config.get()))
                continue;
            UUID id = coordinator.newActionId();
            long now = clock.getAsLong();
            ActionRecord record = takeoverRecord(id, old, now);
            if (!putInflight(record))
                continue;
            Outcome outcome = coordinator.takeOver(lane, old, id);
            if (!outcome.ok())
            {
                deleteInflight(id);
                continue;
            }
            Run run = new Run((ClusterActuator) actuator, lane, record, outcome.claim, Mode.RECOVER, now);
            run.recoveryReason = Reasons.RECOVERY;
            runs.put(id, run);
            reporter.get().transition(record, null, "took over lane " + lane + " from action " + old.actionId + " of node " + old.owner.id()
                                                    + " in phase " + old.phase + ", lease expired at " + old.leaseExpiresAtMillis, null);
            scheduleStep(run, 0);
        }
    }

    /** The local record of a recovery: the claimed action itself exists only on the node that proposed it. */
    private static ActionRecord takeoverRecord(UUID id, Claim old, long now)
    {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("recovers", old.actionId.toString());
        params.put("expired_owner", String.valueOf(old.owner.id()));
        params.put("expired_phase", old.phase);
        Action action = new Action(id, old.actuator, ActionScope.CLUSTER, Subject.parse(old.target), params, null);
        return new ActionRecord(action, ActionState.APPLYING, now, null, TAKEOVER_POLICY, false, Reasons.RECOVERY,
                                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), null, now);
    }

    // ---------------------------------------------------------------------------------------------------------
    // restart recovery (overview 7.6 step 3)
    // ---------------------------------------------------------------------------------------------------------

    @Override
    public boolean recover(UUID id, Optional<ActionRecord> row, ActionExecutor.Reporter reporter)
    {
        if (!recovering.add(id))
            return true;
        try
        {
            scheduler.execute(guarded("restart recovery of cluster action " + id, () -> recoverOnActuatorThread(id, row.orElse(null))));
            return true;
        }
        catch (Throwable t)
        {
            recovering.remove(id);
            handle("hand-over of restart recovery of " + id, t);
            return false;
        }
    }

    private void recoverOnActuatorThread(UUID id, ActionRecord before)
    {
        if (runs.containsKey(id))
            return;
        long now = clock.getAsLong();
        if (before != null && before.state.isTerminal())
        {
            // terminal record, stale key
            deleteInflight(id);
            recovering.remove(id);
            return;
        }
        Claim own = coordinator.ownClaim(id);
        if (before == null)
        {
            if (own == null)
            {
                if (store != null)
                    store.appendEvent(id, null, ActionState.FAILED, Reasons.CLAIM_LOST + ": action row not found");
                deleteInflight(id);
                recovering.remove(id);
                return;
            }
            // the row is unreadable but the claim is ours: recover from the claim alone
            Action action = new Action(id, own.actuator, ActionScope.CLUSTER, Subject.parse(own.target), Collections.emptyMap(), null);
            before = new ActionRecord(action, ActionState.APPLYING, now, null, null, false, Reasons.RESTART,
                                      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), null, now);
        }
        Actuator actuator = actuators.get(before.action.actuator);
        Run run = new Run(actuator instanceof ClusterActuator ? (ClusterActuator) actuator : null, own == null ? null : coordinator.ledger().lane(id),
                          before, own, Mode.RECOVER, now);
        if (own == null)
        {
            Recent released = coordinator.ledger().recent(id);
            if (released != null && released.finalState.startsWith(FINAL_COMMITTED))
                end(run, ActionState.COMMITTED, FINAL_COMMITTED.equals(released.finalState) ? null : released.finalState, null, "found released after restart", false);
            else if (released != null && released.finalState.equals(FINAL_ROLLED_BACK))
                end(run, ActionState.ROLLED_BACK, Reasons.RESTART, null, "found released after restart", false);
            else if (released == null && before.state == ActionState.APPROVED)
                // stopped between the inflight key and a visible claim: nothing was done
                end(run, ActionState.REJECTED, Reasons.RESTART, null, null, false);
            else
                end(run, ActionState.FAILED, Reasons.CLAIM_LOST, null, released == null ? "claim not owned after restart" : "released as " + released.finalState, true);
            return;
        }
        if (run.actuator == null)
        {
            end(run, ActionState.FAILED, Reasons.ACTUATOR_DISABLED, null, "no cluster actuator named " + before.action.actuator + "; the claim is left to expire", true);
            return;
        }
        if (!coordinator.mayAct(own))
        {
            lost(run, "lease expires at " + own.leaseExpiresAtMillis + "; too late to recover after restart");
            return;
        }
        boolean pastCommitPoint;
        try
        {
            pastCommitPoint = run.actuator.pastCommitPoint(own);
        }
        catch (Throwable t)
        {
            handle("commit point check of " + id, t);
            end(run, ActionState.FAILED, Reasons.RECOVERY_FAILED, null, "commit point check threw " + t + "; the claim is left to expire", true);
            return;
        }
        run.recoveryReason = Reasons.RESTART;
        runs.put(id, run);
        transition(run, before.withState(ActionState.APPLYING, Reasons.RESTART, now), "restart: recovering in phase " + own.phase);
        // FC5: past the commit point the claim is released as COMMITTED_CLEANUP_PENDING and recover is not called
        if (pastCommitPoint)
            run.pending = StepResult.committedCleanupPending("restart past the commit point");
        scheduleStep(run, 0);
    }

    // ---------------------------------------------------------------------------------------------------------
    // phase changes committed by any node
    // ---------------------------------------------------------------------------------------------------------

    /** Metadata log thread: only hands over to the actuator thread. */
    @Override
    public void onPhase(Lane lane, Claim claim)
    {
        if (stopped)
            return;
        Actuator actuator = actuators.get(claim.actuator);
        if (!(actuator instanceof ClusterActuator) || !actuators.isEnabled(claim.actuator, config.get()))
            return;
        try
        {
            scheduler.execute(guarded("phase " + claim.phase + " of " + claim.actionId,
                                      () -> ((ClusterActuator) actuator).onPhase(claim, new Context(null))));
        }
        catch (Throwable t)
        {
            logger.debug("Autonomic phase change of {} not dispatched: {}", claim.actionId, t.toString());
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // store
    // ---------------------------------------------------------------------------------------------------------

    private boolean putInflight(ActionRecord record)
    {
        if (store == null)
            return true;
        try
        {
            return store.putState(ActionExecutor.INFLIGHT_PREFIX + record.id(), ActionExecutor.inflightValue(record), clock.getAsLong());
        }
        catch (Throwable t)
        {
            handle("inflight key of " + record, t);
            return false;
        }
    }

    private void deleteInflight(UUID id)
    {
        if (store == null)
            return;
        try
        {
            store.deleteState(ActionExecutor.INFLIGHT_PREFIX + id);
        }
        catch (Throwable t)
        {
            handle("inflight key of " + id, t);
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
}
