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
package org.apache.cassandra.autonomic.plan;

import java.util.List;
import java.util.Objects;

import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;

/** What must improve, and what must not regress, for an applied action to be committed. */
public final class VerifySpec
{
    /** A signal that must not regress by more than {@code tolerance} (relative). */
    public static final class Guard
    {
        public final SignalId signal;
        public final Subject subject;
        public final boolean lowerIsBetter;
        public final double tolerance;
        /**
         * Operations per second of the request class this guard protects, on the guard's subject; null if the guard is
         * always judged. A guard whose class had less than {@link VerifySpec#guardMinOpsPerSec} in the baseline is not
         * judged (ADR-045).
         */
        public final SignalId trafficSignal;

        public Guard(SignalId signal, Subject subject, boolean lowerIsBetter, double tolerance)
        {
            this(signal, subject, lowerIsBetter, tolerance, null);
        }

        public Guard(SignalId signal, Subject subject, boolean lowerIsBetter, double tolerance, SignalId trafficSignal)
        {
            this.signal = signal;
            this.subject = subject;
            this.lowerIsBetter = lowerIsBetter;
            this.tolerance = tolerance;
            this.trafficSignal = trafficSignal;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof Guard))
                return false;
            Guard g = (Guard) o;
            return signal.equals(g.signal) && subject.equals(g.subject) && lowerIsBetter == g.lowerIsBetter
                   && Double.compare(tolerance, g.tolerance) == 0 && Objects.equals(trafficSignal, g.trafficSignal);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(signal, subject, lowerIsBetter, tolerance, trafficSignal);
        }

        @Override
        public String toString()
        {
            return "Guard{" + signal + "@" + subject + ", tolerance=" + tolerance + (trafficSignal == null ? "" : ", traffic=" + trafficSignal) + '}';
        }
    }

    /** Must improve. */
    public final SignalId goal;
    public final Subject goalSubject;
    public final boolean lowerIsBetter;
    /** Relative, for example 0.10. */
    public final double minImprovement;
    public final List<Guard> guards;
    /** Window before apply used as baseline. */
    public final long baselineWindowMillis;
    /** Window after apply used for the verdict. */
    public final long observationWindowMillis;
    /** Start of the problem, epoch millis: the baseline is taken from then (at most the window); {@link #NO_BASELINE_SINCE} for the trailing window. */
    public final long baselineSinceMillis;

    /** A guard with a traffic signal is not judged if that signal's baseline mean is below this (ops/s); {@code safety.guard_min_ops_per_sec}. */
    public final double guardMinOpsPerSec;

    public static final long NO_BASELINE_SINCE = Long.MIN_VALUE;
    public static final double DEFAULT_GUARD_MIN_OPS_PER_SEC = 10;

    public VerifySpec(SignalId goal, Subject goalSubject, boolean lowerIsBetter, double minImprovement, List<Guard> guards,
                      long baselineWindowMillis, long observationWindowMillis)
    {
        this(goal, goalSubject, lowerIsBetter, minImprovement, guards, baselineWindowMillis, observationWindowMillis, NO_BASELINE_SINCE);
    }

    public VerifySpec(SignalId goal, Subject goalSubject, boolean lowerIsBetter, double minImprovement, List<Guard> guards,
                      long baselineWindowMillis, long observationWindowMillis, long baselineSinceMillis)
    {
        this(goal, goalSubject, lowerIsBetter, minImprovement, guards, baselineWindowMillis, observationWindowMillis, baselineSinceMillis,
             DEFAULT_GUARD_MIN_OPS_PER_SEC);
    }

    public VerifySpec(SignalId goal, Subject goalSubject, boolean lowerIsBetter, double minImprovement, List<Guard> guards,
                      long baselineWindowMillis, long observationWindowMillis, long baselineSinceMillis, double guardMinOpsPerSec)
    {
        this.goal = Objects.requireNonNull(goal);
        this.goalSubject = Objects.requireNonNull(goalSubject);
        this.lowerIsBetter = lowerIsBetter;
        this.minImprovement = minImprovement;
        this.guards = List.copyOf(guards);
        this.baselineWindowMillis = baselineWindowMillis;
        this.observationWindowMillis = observationWindowMillis;
        this.baselineSinceMillis = baselineSinceMillis;
        this.guardMinOpsPerSec = guardMinOpsPerSec;
    }

    /** A copy whose baseline starts at {@code sinceMillis} (capped at the baseline window). */
    public VerifySpec withBaselineSince(long sinceMillis)
    {
        return new VerifySpec(goal, goalSubject, lowerIsBetter, minImprovement, guards, baselineWindowMillis, observationWindowMillis, sinceMillis, guardMinOpsPerSec);
    }

    /** A copy with the traffic floor below which a guard with a traffic signal is not judged. */
    public VerifySpec withGuardMinOps(double opsPerSec)
    {
        return new VerifySpec(goal, goalSubject, lowerIsBetter, minImprovement, guards, baselineWindowMillis, observationWindowMillis, baselineSinceMillis, opsPerSec);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof VerifySpec))
            return false;
        VerifySpec v = (VerifySpec) o;
        return goal.equals(v.goal) && goalSubject.equals(v.goalSubject) && lowerIsBetter == v.lowerIsBetter
               && Double.compare(minImprovement, v.minImprovement) == 0 && guards.equals(v.guards)
               && baselineWindowMillis == v.baselineWindowMillis && observationWindowMillis == v.observationWindowMillis
               && baselineSinceMillis == v.baselineSinceMillis && Double.compare(guardMinOpsPerSec, v.guardMinOpsPerSec) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(goal, goalSubject, lowerIsBetter, minImprovement, guards, baselineWindowMillis, observationWindowMillis, baselineSinceMillis, guardMinOpsPerSec);
    }

    @Override
    public String toString()
    {
        return "VerifySpec{goal=" + goal + "@" + goalSubject + ", guards=" + guards
               + (baselineSinceMillis == NO_BASELINE_SINCE ? "" : ", baselineSince=" + baselineSinceMillis) + '}';
    }
}
