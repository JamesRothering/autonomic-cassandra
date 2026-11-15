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
package org.apache.cassandra.autonomic.layout;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Phase of one split partition (adaptive-schema design 4.1). The {@link #id} is the serialized form and must never
 * change; new phases get new ids. {@link #coordinatorClass} is the class C0 to C3 of design 3.2.
 */
public enum Phase
{
    ANNOUNCED(0, 0),
    DUAL_WRITE(1, 1),
    BACKFILL(2, 1),
    VERIFY(3, 1),
    CUTOVER(4, 2),
    NEW_ONLY(5, 3),
    CLEANUP(6, 3),
    STEADY(7, 3),
    ROLLBACK_DUAL(8, 1),
    ABORTING(9, 0);

    public final int id;
    /** 0 to 3: C0 to C3 of design 3.2. */
    public final int coordinatorClass;

    Phase(int id, int coordinatorClass)
    {
        this.id = id;
        this.coordinatorClass = coordinatorClass;
    }

    /**
     * The transitions {@code AdvanceLayout} may commit (design 4.2), keyed by the phase left. {@code CLEANUP -> STEADY}
     * is not in it: it is {@code FinishKey}, which also clears the owning action. {@code ANNOUNCED} is entered by
     * {@code RegisterKey} and {@code ABORTING} is left by {@code RemoveKey}.
     */
    private static final Map<Phase, Set<Phase>> TRANSITIONS = new EnumMap<>(Phase.class);

    static
    {
        TRANSITIONS.put(ANNOUNCED, EnumSet.of(DUAL_WRITE, ABORTING));
        TRANSITIONS.put(DUAL_WRITE, EnumSet.of(BACKFILL, ABORTING));
        TRANSITIONS.put(BACKFILL, EnumSet.of(VERIFY, ABORTING));
        TRANSITIONS.put(VERIFY, EnumSet.of(CUTOVER, BACKFILL, ABORTING));
        TRANSITIONS.put(CUTOVER, EnumSet.of(NEW_ONLY, ROLLBACK_DUAL));
        TRANSITIONS.put(NEW_ONLY, EnumSet.of(CLEANUP));
        TRANSITIONS.put(CLEANUP, EnumSet.noneOf(Phase.class));
        TRANSITIONS.put(STEADY, EnumSet.noneOf(Phase.class));
        TRANSITIONS.put(ROLLBACK_DUAL, EnumSet.of(ABORTING));
        TRANSITIONS.put(ABORTING, EnumSet.noneOf(Phase.class));
    }

    /** True if {@code from -> to} (different phases) is a transition of the table. */
    public static boolean isTransition(Phase from, Phase to)
    {
        return TRANSITIONS.get(from).contains(to);
    }

    /** True if a phase may be re-entered with the next generation ({@code X -> X}, a backfill restart). */
    public boolean restartable()
    {
        return this == BACKFILL || this == CLEANUP;
    }

    /** True if {@code ReportBackfill} is accepted in this phase. */
    public boolean acceptsBackfillReports()
    {
        return this == BACKFILL || this == CLEANUP;
    }

    /** Null if the id is unknown. */
    public static Phase fromId(int id)
    {
        for (Phase phase : values())
            if (phase.id == id)
                return phase;
        return null;
    }
}
