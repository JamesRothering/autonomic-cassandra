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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClaimAction;
import org.apache.cassandra.autonomic.coord.FakeCluster;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.A;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.B;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.K1;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.K2;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.LEASE;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.LOGICAL;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.N1;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.N2;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.NOW;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.VERSION;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.advance;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.assertRejected;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.execute;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.header;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.ledger;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.registry;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.report;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.start;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The reverse path of the layout registry (T6.17a, design 11.4 and 11.5): phases and classes, the transition table,
 * {@code BeginUnsplit}, reports, {@code RemoveKey} from {@code REVERSE_CLEANUP}, {@code ReleaseKey} with base state,
 * registry format 2 with legacy decoding, and the upgrade gate.
 */
public class ReverseLayoutTest
{
    private static final KeyState.BaseState RESIDUE = KeyState.BaseState.RESIDUE;
    private static final KeyState.BaseState TOMBSTONED = KeyState.BaseState.TOMBSTONED;
    private static final KeyState.ReverseMode FAST = KeyState.ReverseMode.FAST;
    private static final KeyState.ReverseMode SLOW = KeyState.ReverseMode.SLOW;
    private static final long FROZEN = 100;
    private static final long DUAL = 500;
    private static final long TT = 7_000_000;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    // ---- hand-built metadata ----

    /** Metadata with the layout registry replaced by {@code registry}. */
    private static ClusterMetadata with(ClusterMetadata md, LayoutRegistry registry)
    {
        return md.transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(registry)).build().metadata;
    }

    private static ClusterMetadata with(ClusterMetadata md, ByteBuffer key, KeyState state, int format)
    {
        TableLayout layout = new TableLayout(header(), 2, ImmutableMap.of(key, state), 5);
        return with(md, new LayoutRegistry(format, ImmutableMap.of(LOGICAL, layout)));
    }

    /** A format 2 registry in which K1 is in {@code phase}, owned by A, claim of A at step 0. */
    private static ClusterMetadata reverseIn(Phase phase, KeyState.ReverseMode mode, KeyState.BaseState base, long tt, int generation, Set<NodeId> done, boolean generationStartedHere)
    {
        ClusterMetadata md = start();
        Epoch e = md.epoch.nextEpoch();
        Epoch generationEpoch = generationStartedHere ? e : Epoch.create(3);
        long dual = phase == Phase.REVERSE_ANNOUNCED ? 0 : DUAL;
        KeyState state = new KeyState(phase, e, NOW, 1, generation, generationEpoch, done, A, 400, ImmutableSet.of(), base, FROZEN, tt, mode, dual);
        return with(md, K1, state, LayoutRegistry.FORMAT_VERSION_2);
    }

    private static ClusterMetadata reverseIn(Phase phase, KeyState.ReverseMode mode)
    {
        return reverseIn(phase, mode, mode == SLOW ? TOMBSTONED : RESIDUE, mode == SLOW ? TT : 0, 1, ImmutableSet.of(), true);
    }

    private static ClusterMetadata reverseIn(Phase phase)
    {
        return reverseIn(phase, FAST);
    }

    /** A format 2 registry with K1 in a forward phase and the given base state; unowned in STEADY, else owned by {@code owner}. */
    private static ClusterMetadata forwardIn(Phase phase, UUID owner, KeyState.BaseState base, long tt, int format)
    {
        ClusterMetadata md = start();
        Epoch e = md.epoch.nextEpoch();
        KeyState state = new KeyState(phase, e, NOW, 1, 1, e, ImmutableSet.of(), owner, 400, ImmutableSet.of(), base, FROZEN, tt, null, 0);
        return with(md, K1, state, format);
    }

    private static KeyState key(ClusterMetadata md)
    {
        return registry(md).layout(LOGICAL).keyState(K1);
    }

    private static BeginUnsplit begin(KeyState.ReverseMode mode, UUID previous)
    {
        return new BeginUnsplit(A, N1, 0, LOGICAL, K1, mode, previous, NOW + 5, LEASE);
    }

    private static ClusterMetadata withOldNode(ClusterMetadata md)
    {
        return FakeCluster.register(md, 3, new NodeVersion(new CassandraVersion("6.0.0"), Version.V10));
    }

    // ---- phases and classes ----

    @Test
    public void reversePhasesHaveTheIdsAndClassesOfDesign114()
    {
        Phase[] phases = { Phase.REVERSE_ANNOUNCED, Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_BACKFILL, Phase.REVERSE_VERIFY, Phase.CUTBACK,
                           Phase.BASE_ONLY, Phase.REVERSE_CLEANUP, Phase.REVERSE_ROLLBACK };
        int[] classes = { 3, 2, 2, 2, 1, 0, 0, 2 };
        for (int i = 0; i < phases.length; i++)
        {
            assertEquals(10 + i, phases[i].id);
            assertSame(phases[i], Phase.fromId(10 + i));
            assertEquals(phases[i].name(), classes[i], phases[i].coordinatorClass);
            assertTrue(phases[i].isReverse());
        }
        for (Phase forward : LayoutTransformationTest.FORWARD)
            assertFalse(forward.isReverse());
        // each mirrors the forward phase of the same class and role
        assertEquals(Phase.STEADY.coordinatorClass, Phase.REVERSE_ANNOUNCED.coordinatorClass);
        assertEquals(Phase.CUTOVER.coordinatorClass, Phase.REVERSE_VERIFY.coordinatorClass);
        assertEquals(Phase.DUAL_WRITE.coordinatorClass, Phase.CUTBACK.coordinatorClass);
        assertEquals(Phase.ANNOUNCED.coordinatorClass, Phase.BASE_ONLY.coordinatorClass);
        assertEquals(Phase.ABORTING.coordinatorClass, Phase.REVERSE_CLEANUP.coordinatorClass);
    }

    /** The reverse allowed pairs of 11.4, written out independently of {@link Phase}. */
    private static Set<Phase> reverseTargets(Phase from)
    {
        switch (from)
        {
            case REVERSE_ANNOUNCED: return EnumSet.of(Phase.REVERSE_DUAL_WRITE, Phase.STEADY);
            case REVERSE_DUAL_WRITE: return EnumSet.of(Phase.REVERSE_BACKFILL, Phase.STEADY);
            case REVERSE_BACKFILL: return EnumSet.of(Phase.REVERSE_VERIFY, Phase.STEADY);
            case REVERSE_VERIFY: return EnumSet.of(Phase.CUTBACK, Phase.REVERSE_BACKFILL, Phase.STEADY);
            case CUTBACK: return EnumSet.of(Phase.BASE_ONLY, Phase.REVERSE_ROLLBACK);
            case BASE_ONLY: return EnumSet.of(Phase.REVERSE_CLEANUP);
            case REVERSE_ROLLBACK: return EnumSet.of(Phase.STEADY);
            default: return EnumSet.noneOf(Phase.class); // REVERSE_CLEANUP leaves through RemoveKey
        }
    }

    @Test
    public void everyReversePairOfPhasesIsAcceptedOrRejectedAsTheTable()
    {
        EnumSet<Phase> restartable = EnumSet.of(Phase.REVERSE_ANNOUNCED, Phase.REVERSE_BACKFILL, Phase.REVERSE_CLEANUP);
        for (Phase from : Phase.values())
        {
            if (!from.isReverse())
                continue;
            ClusterMetadata md = reverseIn(from);
            for (Phase to : Phase.values())
            {
                boolean legal = reverseTargets(from).contains(to);
                boolean restart = from == to && restartable.contains(from);
                boolean newGeneration = restart || (from == Phase.REVERSE_VERIFY && to == Phase.REVERSE_BACKFILL);
                String what = from + " -> " + to;
                assertEquals(what, legal, Phase.isTransition(from, to));
                int generation = newGeneration ? 2 : 1;
                if (legal || restart)
                {
                    ClusterMetadata next = execute(md, advance(0, from, to, generation));
                    KeyState k = key(next);
                    assertEquals(what, to, k.phase);
                    assertEquals(what, generation, k.generation);
                    assertEquals(what, next.epoch, k.phaseEpoch);
                    assertEquals(what, NOW + 2, k.phaseSinceMillis);
                    assertEquals(what, A, k.actionId);
                    assertEquals(what, 1, ledger(next).claim(A).step);
                    assertEquals(what, to.name(), ledger(next).claim(A).phase);
                    assertEquals(what, newGeneration ? next.epoch : md.epoch, k.generationEpoch);
                }
                else
                {
                    assertRejected("illegal transition", md, advance(0, from, to, 1));
                    assertRejected("illegal transition", md, advance(0, from, to, 2));
                }
            }
        }
        // a wrong generation for a legal pair
        assertRejected("requires generation 1, not 2", reverseIn(Phase.REVERSE_DUAL_WRITE), advance(0, Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_BACKFILL, 2));
        assertRejected("requires generation 2, not 1", reverseIn(Phase.REVERSE_VERIFY), advance(0, Phase.REVERSE_VERIFY, Phase.REVERSE_BACKFILL, 1));
        assertRejected("requires generation 2, not 1", reverseIn(Phase.REVERSE_BACKFILL), advance(0, Phase.REVERSE_BACKFILL, Phase.REVERSE_BACKFILL, 1));
        // nothing leaves BASE_ONLY or REVERSE_CLEANUP but forward
        for (Phase to : Phase.values())
        {
            if (to != Phase.REVERSE_CLEANUP)
                assertFalse(Phase.isTransition(Phase.BASE_ONLY, to));
            assertFalse(Phase.isTransition(Phase.REVERSE_CLEANUP, to));
        }
    }

    @Test
    public void everyReverseTransitionMovesToTheSameOrAnAdjacentClass()
    {
        for (Phase from : Phase.values())
            for (Phase to : Phase.values())
                if (Phase.isTransition(from, to))
                    assertTrue(from + " -> " + to, Math.abs(from.coordinatorClass - to.coordinatorClass) <= 1);
        // the reverse chain goes C3, C2, C2, C2, C1, C0, C0
        Phase[] chain = { Phase.REVERSE_ANNOUNCED, Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_BACKFILL, Phase.REVERSE_VERIFY, Phase.CUTBACK, Phase.BASE_ONLY, Phase.REVERSE_CLEANUP };
        for (int i = 0; i + 1 < chain.length; i++)
            assertTrue(chain[i] + " -> " + chain[i + 1], Phase.isTransition(chain[i], chain[i + 1]));
    }

    @Test
    public void reverseChainEndToEndWithOneClaim()
    {
        ClusterMetadata md = reverseIn(Phase.STEADY);
        md = with(md, K1, new KeyState(Phase.STEADY, md.epoch, NOW, 1, 4, md.epoch, ImmutableSet.of(), null, 400, ImmutableSet.of(), RESIDUE, FROZEN, 0, null, 0),
                  LayoutRegistry.FORMAT_VERSION_2);
        md = execute(md, begin(FAST, null));
        assertEquals(Phase.REVERSE_ANNOUNCED, key(md).phase);
        assertEquals(5, key(md).generation);
        int step = 1;
        Phase from = Phase.REVERSE_ANNOUNCED;
        for (Phase to : new Phase[]{ Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_BACKFILL, Phase.REVERSE_VERIFY, Phase.CUTBACK, Phase.BASE_ONLY, Phase.REVERSE_CLEANUP })
        {
            md = execute(md, advance(step++, from, to, key(md).generation));
            from = to;
        }
        assertEquals(NOW + 2, key(md).reverseDualSinceMillis);
        md = execute(md, advance(step++, Phase.REVERSE_CLEANUP, Phase.REVERSE_CLEANUP, 6));
        md = execute(md, report(md, LOGICAL, K1, 6, N1));
        md = execute(md, new RemoveKey(A, N1, step, LOGICAL, K1, NOW + 3, LEASE));
        assertFalse(LayoutRegistry.of(md).byLogical.containsKey(LOGICAL));
        assertEquals("KEY_REMOVED", ledger(md).claim(A).phase);
        assertEquals(step + 1, ledger(md).claim(A).step);
    }

    // ---- BeginUnsplit ----

    @Test
    public void beginUnsplitFromSteadyKeepsTheBaseStateAndStartsAGeneration()
    {
        for (KeyState.ReverseMode mode : KeyState.ReverseMode.values())
        {
            KeyState.BaseState base = mode == FAST ? RESIDUE : TOMBSTONED;
            ClusterMetadata prev = forwardIn(Phase.STEADY, null, base, mode == FAST ? 0 : TT, LayoutRegistry.FORMAT_VERSION_2);
            ClusterMetadata next = execute(prev, begin(mode, null));
            KeyState before = key(prev);
            KeyState k = key(next);
            assertEquals(Phase.REVERSE_ANNOUNCED, k.phase);
            assertEquals(next.epoch, k.phaseEpoch);
            assertEquals(NOW + 5, k.phaseSinceMillis);
            assertEquals(before.generation + 1, k.generation);
            assertEquals(next.epoch, k.generationEpoch);
            assertTrue(k.backfillDone.isEmpty());
            assertEquals(A, k.actionId);
            assertEquals(mode, k.reverseMode);
            assertEquals(0, k.reverseDualSinceMillis);
            assertEquals(base, k.baseState);
            assertEquals(before.frozenSinceMillis, k.frozenSinceMillis);
            assertEquals(before.baseTombstoneMicros, k.baseTombstoneMicros);
            assertEquals(before.incarnation, k.incarnation);
            assertEquals(before.dualWriteSinceMillis, k.dualWriteSinceMillis);
            assertEquals(6, registry(next).layout(LOGICAL).version);
            assertEquals(LayoutRegistry.FORMAT_VERSION_2, registry(next).formatVersion);
            assertEquals(1, ledger(next).claim(A).step);
            assertEquals("REVERSE_ANNOUNCED", ledger(next).claim(A).phase);
            assertEquals(next.epoch, next.extensions.get(LayoutRegistryValue.KEY).lastModified());
            assertEquals(next.epoch, next.extensions.get(org.apache.cassandra.autonomic.coord.ClusterLedgerValue.KEY).lastModified());
        }
    }

    @Test
    public void beginUnsplitFromNewOnlyAndCleanupTakesOverTheForwardAction()
    {
        for (Phase phase : new Phase[]{ Phase.NEW_ONLY, Phase.CLEANUP })
        {
            // owned by an action that holds no claim
            ClusterMetadata prev = forwardIn(phase, B, RESIDUE, 0, LayoutRegistry.FORMAT_VERSION_2);
            ClusterMetadata next = execute(prev, begin(FAST, B));
            assertEquals(phase.name(), Phase.REVERSE_ANNOUNCED, key(next).phase);
            assertEquals(phase.name(), A, key(next).actionId);
            assertEquals(phase.name(), FAST, key(next).reverseMode);
            assertEquals(phase.name(), RESIDUE, key(next).baseState);
            assertEquals(phase.name(), FROZEN, key(next).frozenSinceMillis);
            // owned by the claiming action itself
            assertEquals(Phase.REVERSE_ANNOUNCED, key(execute(forwardIn(phase, A, RESIDUE, 0, LayoutRegistry.FORMAT_VERSION_2), begin(FAST, A))).phase);
            // a slow reverse needs a tombstone, which these phases never have
            assertRejected("slow mode needs the base partition in state TOMBSTONED", prev, begin(SLOW, B));
            // the previous action must own the key and must not hold a claim
            assertRejected("owned by action " + B + ", not " + A, prev, begin(FAST, A));
            assertRejected("not null", prev, begin(FAST, null));
            ClusterMetadata held = execute(prev, new ClaimAction(Lane.MAINTENANCE, new Claim(B, "x", "t", N1, "P", 0, NOW, NOW + LEASE, ImmutableMap.of()), NOW, 0).wrap());
            assertRejected("still holds a claim", held, begin(FAST, B));
        }
    }

    @Test
    public void beginUnsplitRejections()
    {
        // phases it cannot start from
        for (Phase phase : Phase.values())
        {
            if (phase == Phase.STEADY || phase == Phase.NEW_ONLY || phase == Phase.CLEANUP)
                continue;
            ClusterMetadata md = phase.isReverse() ? reverseIn(phase) : LayoutTransformationTest.keyIn(phase);
            assertRejected("BeginUnsplit needs STEADY, NEW_ONLY or CLEANUP", md, begin(FAST, null));
        }
        // STEADY must be unowned: the reclaim of the base partition is not finished
        ClusterMetadata owned = forwardIn(Phase.STEADY, B, RESIDUE, 0, LayoutRegistry.FORMAT_VERSION_2);
        assertRejected("still owned by action " + B, owned, begin(FAST, null));
        ClusterMetadata steady = forwardIn(Phase.STEADY, null, RESIDUE, 0, LayoutRegistry.FORMAT_VERSION_2);
        assertRejected("owned by no action, not by " + B, steady, begin(FAST, B));
        // the mode must fit the base state
        assertRejected("fast mode needs the base partition in state RESIDUE, it is TOMBSTONED",
                       forwardIn(Phase.STEADY, null, TOMBSTONED, TT, LayoutRegistry.FORMAT_VERSION_2), begin(FAST, null));
        assertRejected("slow mode needs the base partition in state TOMBSTONED, it is RESIDUE", steady, begin(SLOW, null));
        // claim and lookup
        assertRejected("does not hold lane", LayoutTransformationTest.empty(), begin(FAST, null));
        assertRejected("has no layout", start(), begin(FAST, null));
        assertRejected("key is not registered", steady, new BeginUnsplit(A, N1, 0, LOGICAL, K2, FAST, null, NOW, LEASE));
        assertRejected("owned by node 1", steady, new BeginUnsplit(A, N2, 0, LOGICAL, K1, FAST, null, NOW, LEASE));
        assertRejected("step 0, not 2", steady, new BeginUnsplit(A, N1, 2, LOGICAL, K1, FAST, null, NOW, LEASE));
        // nothing changed by a rejection
        assertEquals(Phase.STEADY, key(steady).phase);
    }

    // ---- format 2, legacy decoding, the gate ----

    @Test
    public void format1ValuesDecodeAsDesign115Says() throws IOException
    {
        Epoch e = Epoch.create(9);
        UUID owner = A;
        long dual = 400;
        Object[][] cases = {
            // phase, owner, expected base state, expected frozenSince
            { Phase.ANNOUNCED, owner, RESIDUE, 0L },
            { Phase.DUAL_WRITE, owner, RESIDUE, 0L },
            { Phase.BACKFILL, owner, RESIDUE, 0L },
            { Phase.VERIFY, owner, RESIDUE, 0L },
            { Phase.CUTOVER, owner, RESIDUE, 0L },
            { Phase.NEW_ONLY, owner, RESIDUE, dual },
            { Phase.CLEANUP, owner, RESIDUE, dual },
            { Phase.STEADY, owner, RESIDUE, dual },
            { Phase.STEADY, null, TOMBSTONED, dual },
            { Phase.ROLLBACK_DUAL, owner, RESIDUE, 0L },
            { Phase.ABORTING, owner, RESIDUE, 0L },
        };
        for (Object[] c : cases)
        {
            Phase phase = (Phase) c[0];
            UUID action = (UUID) c[1];
            // a state that carries everything format 2 can, written in format 1 so that the extra fields are dropped
            KeyState rich = new KeyState(phase, e, NOW, 1, 1, e, ImmutableSet.of(N1), action, dual, ImmutableSet.of(),
                                         TOMBSTONED, 12345, TT, null, 0);
            LayoutRegistry legacy = new LayoutRegistry(LayoutRegistry.FORMAT_VERSION,
                                                       ImmutableMap.of(LOGICAL, new TableLayout(header(), 2, ImmutableMap.of(K1, rich), 3)));
            DataOutputBuffer out = new DataOutputBuffer();
            new LayoutRegistryValue(legacy).serialize(out, VERSION);
            assertEquals("format 1 is written as 1", LayoutRegistry.FORMAT_VERSION, out.buffer().get(0));
            LayoutRegistryValue read = new LayoutRegistryValue();
            read.deserialize(new DataInputBuffer(out.buffer(), false), VERSION);
            assertEquals(LayoutRegistry.FORMAT_VERSION, read.getValue().formatVersion);
            KeyState k = read.getValue().layout(LOGICAL).keyState(K1);
            String what = phase + " owned by " + action;
            assertEquals(what, c[2], k.baseState);
            assertEquals(what, c[3], k.frozenSinceMillis);
            assertEquals(what, 0, k.baseTombstoneMicros);
            assertNull(what, k.reverseMode);
            assertEquals(what, 0, k.reverseDualSinceMillis);
            assertEquals(what, phase, k.phase);
            assertEquals(what, ImmutableSet.of(N1), k.backfillDone);
            // and the constructors of format 1 give the same
            assertEquals(what, new KeyState(phase, e, NOW, 1, 1, e, ImmutableSet.of(N1), action, dual), k);
        }
    }

    @Test
    public void format2RoundTrips() throws IOException
    {
        Epoch e = Epoch.create(9);
        List<KeyState> states = new ArrayList<>();
        states.add(new KeyState(Phase.REVERSE_BACKFILL, e, NOW, 3, 2, e, ImmutableSet.of(N1, N2), A, 400, ImmutableSet.of(), RESIDUE, FROZEN, 0, FAST, DUAL));
        states.add(new KeyState(Phase.REVERSE_ANNOUNCED, e, NOW, 4, 5, e, ImmutableSet.of(N2), B, 400, ImmutableSet.of(), TOMBSTONED, FROZEN, TT, SLOW, 0));
        states.add(new KeyState(Phase.STEADY, e, NOW, 5, 1, e, ImmutableSet.of(), null, 0, ImmutableSet.of(), TOMBSTONED, 0, 0, null, 0));
        states.add(new KeyState(Phase.ANNOUNCED, e, NOW, 6, 1, e, ImmutableSet.of(), A, 0, ImmutableSet.of(N1), RESIDUE, 0, 0, null, 0));
        for (KeyState state : states)
        {
            LayoutRegistry registry = new LayoutRegistry(LayoutRegistry.FORMAT_VERSION_2,
                                                         ImmutableMap.of(LOGICAL, new TableLayout(header(), 7, ImmutableMap.of(K1, state), 3)));
            LayoutRegistryValue value = new LayoutRegistryValue(registry);
            DataOutputBuffer out = new DataOutputBuffer();
            value.serialize(out, VERSION);
            assertEquals(LayoutRegistry.FORMAT_VERSION_2, out.buffer().get(0));
            assertEquals(out.getLength(), value.serializedSize(VERSION));
            LayoutRegistryValue read = new LayoutRegistryValue();
            read.deserialize(new DataInputBuffer(out.buffer(), false), VERSION);
            assertEquals(registry, read.getValue());
            assertEquals(state, read.getValue().layout(LOGICAL).keyState(K1));
            assertEquals(LayoutRegistry.FORMAT_VERSION_2, read.getValue().formatVersion);
        }
        // a format 2 registry holds the whole metadata round trip too
        ClusterMetadata md = reverseIn(Phase.REVERSE_VERIFY, SLOW);
        assertEquals(registry(md), registry(LayoutTransformationTest.roundTrip(md)));
        // an unknown base state or reverse mode is corruption
        DataOutputBuffer out = new DataOutputBuffer();
        new LayoutRegistryValue(new LayoutRegistry(LayoutRegistry.FORMAT_VERSION_2,
                                                   ImmutableMap.of(LOGICAL, new TableLayout(header(), 7, ImmutableMap.of(K1, states.get(0)), 3)))).serialize(out, VERSION);
        ByteBuffer bytes = out.buffer();
        int baseStateAt = bytes.limit() - (int) Epoch.serializer.serializedSize(Epoch.EMPTY, VERSION) - (1 + 8 + 8 + 1 + 8);
        assertEquals(RESIDUE.id, bytes.get(baseStateAt));
        bytes.put(baseStateAt, (byte) 9);
        try
        {
            new LayoutRegistryValue().deserialize(new DataInputBuffer(bytes, false), VERSION);
            fail("an unknown base state must not be read");
        }
        catch (IOException expected)
        {
            assertTrue(expected.getMessage().contains("base state"));
        }
    }

    @Test
    public void aFormat1RegistryStaysAsALegacyValueUntilSomethingNeedsFormat2()
    {
        // the forward path in a format 1 registry never holds more than a format 1 value can: replay and snapshot agree
        ClusterMetadata md = LayoutTransformationTest.keyIn(Phase.CUTOVER);
        md = execute(md, advance(0, Phase.CUTOVER, Phase.NEW_ONLY, 1));
        assertEquals(LayoutRegistry.FORMAT_VERSION, registry(md).formatVersion);
        KeyState k = key(md);
        assertEquals(0, k.frozenSinceMillis);
        assertEquals(k, key(LayoutTransformationTest.roundTrip(md)));
        // the same transition in a format 2 registry records the freeze
        ClusterMetadata two = forwardIn(Phase.CUTOVER, A, RESIDUE, 0, LayoutRegistry.FORMAT_VERSION_2);
        two = execute(two, advance(0, Phase.CUTOVER, Phase.NEW_ONLY, 1));
        assertEquals(NOW + 2, key(two).frozenSinceMillis);
        assertEquals(RESIDUE, key(two).baseState);
        // and the format 1 forward release stays a format 1 value
        ClusterMetadata steady = LayoutTransformationTest.keyIn(Phase.STEADY);
        ClusterMetadata owned = with(steady, K1, new KeyState(Phase.STEADY, steady.epoch, NOW, 1, 1, steady.epoch, ImmutableSet.of(), A, 0),
                                     LayoutRegistry.FORMAT_VERSION);
        ClusterMetadata released = execute(owned, new ReleaseKey(A, N1, 0, LOGICAL, K1, NOW, LEASE));
        assertEquals(LayoutRegistry.FORMAT_VERSION, registry(released).formatVersion);
        assertNull(key(released).actionId);
        assertEquals(TOMBSTONED, key(released).baseState);
        assertEquals(0, key(released).baseTombstoneMicros);
    }

    @Test
    public void aFormat2ValueIsRefusedBeforeTheUpgradeGate()
    {
        // BeginUnsplit upgrades a format 1 registry, but not while a node runs a release that cannot read format 2
        ClusterMetadata steady = LayoutTransformationTest.keyIn(Phase.STEADY);
        assertEquals(LayoutRegistry.FORMAT_VERSION, registry(steady).formatVersion);
        assertNull(LayoutRegistry.formatGateRefusal(steady));
        ClusterMetadata old = withOldNode(steady);
        assertTrue(LayoutRegistry.formatGateRefusal(old).contains("format 2 needs every node at"));
        assertRejected("format 2 needs every node at", old, begin(SLOW, null));
        assertRejected("format 2 needs every node at", with(old, K1, new KeyState(Phase.STEADY, old.epoch, NOW, 1, 1, old.epoch, ImmutableSet.of(), null, 0, ImmutableSet.of(),
                                                                                  RESIDUE, 0, 0, null, 0), LayoutRegistry.FORMAT_VERSION), begin(FAST, null));
        assertEquals(LayoutRegistry.FORMAT_VERSION, registry(old).formatVersion);
        // once the cluster is upgraded the same transformation commits a format 2 value
        ClusterMetadata next = execute(steady, begin(SLOW, null));
        assertEquals(LayoutRegistry.FORMAT_VERSION_2, registry(next).formatVersion);
        assertEquals(TOMBSTONED, key(next).baseState);
        assertEquals(SLOW, key(next).reverseMode);
        // a release that carries more than a format 1 release does is refused by the gate as well
        ClusterMetadata owned = with(old, K1, new KeyState(Phase.STEADY, old.epoch, NOW, 1, 1, old.epoch, ImmutableSet.of(), A, 0), LayoutRegistry.FORMAT_VERSION);
        assertRejected("format 2 needs every node at", owned, new ReleaseKey(A, N1, 0, LOGICAL, K1, RESIDUE, 0, NOW, LEASE));
        assertRejected("format 2 needs every node at", owned, new ReleaseKey(A, N1, 0, LOGICAL, K1, TOMBSTONED, TT, NOW, LEASE));
        // the format 1 release is not
        assertNull(key(execute(owned, new ReleaseKey(A, N1, 0, LOGICAL, K1, NOW, LEASE))).actionId);
        // a registry already in format 2 needs no gate
        ClusterMetadata two = withOldNode(forwardIn(Phase.STEADY, null, TOMBSTONED, TT, LayoutRegistry.FORMAT_VERSION_2));
        assertEquals(Phase.REVERSE_ANNOUNCED, key(execute(two, begin(SLOW, null))).phase);
    }

    // ---- ReportBackfill ----

    @Test
    public void reportsAreAcceptedInReverseAnnouncedBackfillAndCleanup()
    {
        for (Phase phase : new Phase[]{ Phase.REVERSE_ANNOUNCED, Phase.REVERSE_BACKFILL, Phase.REVERSE_CLEANUP })
        {
            ClusterMetadata md = reverseIn(phase);
            assertTrue(phase.acceptsBackfillReports());
            assertTrue(phase.name(), key(md).reportsDue());
            ClusterMetadata next = execute(md, report(md, LOGICAL, K1, 1, N2));
            assertEquals(phase.name(), ImmutableSet.of(N2), key(next).backfillDone);
            // idempotent
            assertSame(registry(next), registry(execute(next, report(md, LOGICAL, K1, 1, N2))));
        }
        // the replicas' forward pass is never due in a reverse phase
        for (Phase phase : Phase.values())
            if (phase.isReverse())
                assertFalse(phase.name(), key(reverseIn(phase)).backfillDue());
        // REVERSE_CLEANUP takes reports for the reconcile generation only
        ClusterMetadata entered = reverseIn(Phase.REVERSE_CLEANUP, FAST, RESIDUE, 0, 1, ImmutableSet.of(N1), false);
        assertFalse(key(entered).reportsDue());
        assertRejected("the reconcile generation has not started", entered, new ReportBackfill(LOGICAL, K1, 1, key(entered).generationEpoch, N2));
        // wrong generation and epoch are rejected as in the forward path
        ClusterMetadata md = reverseIn(Phase.REVERSE_BACKFILL);
        assertRejected("generation 1, not 2", md, report(md, LOGICAL, K1, 2, N1));
        assertRejected("started at epoch", md, new ReportBackfill(LOGICAL, K1, 1, Epoch.create(1), N1));
        // the other reverse phases take none
        for (Phase phase : new Phase[]{ Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_VERIFY, Phase.CUTBACK, Phase.BASE_ONLY, Phase.REVERSE_ROLLBACK })
        {
            assertFalse(phase.acceptsBackfillReports());
            ClusterMetadata other = reverseIn(phase);
            assertRejected("takes no backfill reports", other, report(other, LOGICAL, K1, 1, N1));
        }
    }

    @Test
    public void reverseReportSetIsBoundedByMaxHintReports()
    {
        assertEquals(LayoutRegistry.MAX_BACKFILL_NODES, LayoutRegistry.maxBackfillReports(Phase.BACKFILL));
        assertEquals(LayoutRegistry.MAX_BACKFILL_NODES, LayoutRegistry.maxBackfillReports(Phase.CLEANUP));
        for (Phase phase : new Phase[]{ Phase.REVERSE_ANNOUNCED, Phase.REVERSE_BACKFILL, Phase.REVERSE_CLEANUP })
            assertEquals(LayoutRegistry.MAX_HINT_REPORTS, LayoutRegistry.maxBackfillReports(phase));
        Set<NodeId> almost = new java.util.TreeSet<>();
        for (int i = 1; i < LayoutRegistry.MAX_HINT_REPORTS; i++)
            almost.add(new NodeId(i));
        // one below the bound: the last report is accepted, the next is not
        ClusterMetadata md = reverseIn(Phase.REVERSE_BACKFILL, FAST, RESIDUE, 0, 1, almost, true);
        assertEquals(LayoutRegistry.MAX_HINT_REPORTS - 1, key(md).backfillDone.size());
        ClusterMetadata full = execute(md, report(md, LOGICAL, K1, 1, new NodeId(LayoutRegistry.MAX_HINT_REPORTS)));
        assertEquals(LayoutRegistry.MAX_HINT_REPORTS, key(full).backfillDone.size());
        assertRejected("backfill_done already has " + LayoutRegistry.MAX_HINT_REPORTS + " nodes", full,
                       report(full, LOGICAL, K1, 1, new NodeId(LayoutRegistry.MAX_HINT_REPORTS + 1)));
        // a node that already reported is still idempotent at the bound
        assertSame(registry(full), registry(execute(full, report(full, LOGICAL, K1, 1, N1))));
        // the key state refuses more, and in the forward phases refuses more than 32
        Set<NodeId> tooMany = new java.util.TreeSet<>(almost);
        tooMany.add(new NodeId(LayoutRegistry.MAX_HINT_REPORTS));
        tooMany.add(new NodeId(LayoutRegistry.MAX_HINT_REPORTS + 1));
        Epoch e = Epoch.create(1);
        try
        {
            new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, tooMany, A);
            fail("the reverse bound is 1024");
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage().contains("the limit is 1024"));
        }
        Set<NodeId> thirtyThree = new java.util.TreeSet<>();
        for (int i = 1; i <= LayoutRegistry.MAX_BACKFILL_NODES + 1; i++)
            thirtyThree.add(new NodeId(i));
        new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, thirtyThree, A);
        try
        {
            new KeyState(Phase.BACKFILL, e, 0, 1, 1, e, thirtyThree, A);
            fail("the forward bound is 32");
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage().contains("the limit is 32"));
        }
    }

    @Test
    public void leavingTheReversePathDropsReportsThatTheForwardBoundCouldNotHold()
    {
        Set<NodeId> many = new java.util.TreeSet<>();
        for (int i = 1; i <= 100; i++)
            many.add(new NodeId(i));
        ClusterMetadata md = reverseIn(Phase.REVERSE_BACKFILL, FAST, RESIDUE, 0, 1, many, true);
        md = execute(md, advance(0, Phase.REVERSE_BACKFILL, Phase.REVERSE_VERIFY, 1));
        assertEquals(100, key(md).backfillDone.size());
        md = execute(md, advance(1, Phase.REVERSE_VERIFY, Phase.STEADY, 1));
        assertTrue(key(md).backfillDone.isEmpty());
        // BeginUnsplit starts clean too
        ClusterMetadata again = execute(with(md, K1, key(md).released(RESIDUE, 0), LayoutRegistry.FORMAT_VERSION_2),
                                        new BeginUnsplit(A, N1, 2, LOGICAL, K1, FAST, null, NOW + 5, LEASE));
        assertTrue(key(again).backfillDone.isEmpty());
    }

    // ---- RemoveKey ----

    private static RemoveKey remove(int step)
    {
        return new RemoveKey(A, N1, step, LOGICAL, K1, NOW + 3, LEASE);
    }

    @Test
    public void removeKeyFromReverseCleanupNeedsACompleteReconcile()
    {
        // the placement of the test keyspace puts the whole ring on node 1
        ClusterMetadata started = reverseIn(Phase.REVERSE_CLEANUP, FAST, RESIDUE, 0, 2, ImmutableSet.of(), true);
        assertRejected("natural replicas [1] have not reported reconcile generation 2", started, remove(0));
        ClusterMetadata reported = execute(started, report(started, LOGICAL, K1, 2, N1));
        ClusterMetadata removed = execute(reported, remove(0));
        assertFalse("the layout goes with its last key", LayoutRegistry.of(removed).byLogical.containsKey(LOGICAL));
        assertEquals("KEY_REMOVED", ledger(removed).claim(A).phase);
        assertEquals(1, ledger(removed).claim(A).step);

        // with two replicas every bucket partition's replicas must report
        ClusterMetadata two = LayoutTransformationTest.withPlacement(reported, 1, 2);
        assertRejected("natural replicas [2] have not reported reconcile generation 2", two, remove(0));
        two = execute(two, report(two, LOGICAL, K1, 2, N2));
        assertTrue(registry(execute(two, remove(0))).byLogical.isEmpty());
        // a node that is not a replica does not count
        ClusterMetadata outsider = execute(started, report(started, LOGICAL, K1, 2, new NodeId(9)));
        assertRejected("natural replicas [1] have not reported", outsider, remove(0));
        // the reconcile generation must have started: the generation of the backfill is not it
        ClusterMetadata notStarted = reverseIn(Phase.REVERSE_CLEANUP, FAST, RESIDUE, 0, 1, ImmutableSet.of(N1), false);
        assertRejected("the reconcile generation of the key has not started", notStarted, remove(0));
        // owner
        assertTrue(registry(reported).withoutKey(LOGICAL, K1, B, ImmutableSet.of(N1)).rejection.contains("owned by action " + A + ", not " + B));
        // no placement, no removal
        assertRejected("no natural replica", LayoutTransformationTest.withPlacement(reported), remove(0));
        // another key stays
        TableLayout layout = registry(reported).layout(LOGICAL);
        KeyState other = new KeyState(Phase.STEADY, Epoch.create(2), 0, 2, 1, Epoch.create(2), ImmutableSet.of(), null);
        ClusterMetadata twoKeys = with(reported, new LayoutRegistry(LayoutRegistry.FORMAT_VERSION_2,
                                                                    ImmutableMap.of(LOGICAL, new TableLayout(header(), 3, ImmutableMap.of(K1, layout.keyState(K1), K2, other), 5))));
        ClusterMetadata kept = execute(twoKeys, remove(0));
        assertNull(registry(kept).layout(LOGICAL).keyState(K1));
        assertEquals(other.phase, registry(kept).layout(LOGICAL).keyState(K2).phase);
    }

    @Test
    public void removeKeyRejectsEveryOtherReversePhaseAndKeepsTheForwardRule()
    {
        for (Phase phase : Phase.values())
        {
            if (phase == Phase.ABORTING || phase == Phase.REVERSE_CLEANUP)
                continue;
            ClusterMetadata md = phase.isReverse() ? reverseIn(phase) : LayoutTransformationTest.keyIn(phase);
            assertRejected("not ABORTING or REVERSE_CLEANUP", md, remove(0));
        }
        // the forward rule still works for ABORTING
        assertTrue(registry(execute(LayoutTransformationTest.keyIn(Phase.ABORTING), remove(0))).byLogical.isEmpty());
        // and ABORTING needs no placement
        assertTrue(registry(execute(LayoutTransformationTest.withPlacement(LayoutTransformationTest.keyIn(Phase.ABORTING)), remove(0))).byLogical.isEmpty());
    }

    // ---- ReleaseKey ----

    @Test
    public void releaseKeyCarriesBaseStateAndTt()
    {
        ClusterMetadata owned = forwardIn(Phase.STEADY, A, RESIDUE, 0, LayoutRegistry.FORMAT_VERSION_2);
        ClusterMetadata tombstoned = execute(owned, new ReleaseKey(A, N1, 0, LOGICAL, K1, TOMBSTONED, TT, NOW + 1, LEASE));
        KeyState k = key(tombstoned);
        assertNull(k.actionId);
        assertEquals(TOMBSTONED, k.baseState);
        assertEquals(TT, k.baseTombstoneMicros);
        assertEquals(FROZEN, k.frozenSinceMillis);
        assertEquals("KEY_RELEASED", ledger(tombstoned).claim(A).phase);
        // unknown Tt
        assertEquals(0, key(execute(owned, new ReleaseKey(A, N1, 0, LOGICAL, K1, TOMBSTONED, 0, NOW + 1, LEASE))).baseTombstoneMicros);
        // RESIDUE: nothing was written (the forward cleanup of ADR-036 releases like this)
        ClusterMetadata residue = execute(owned, new ReleaseKey(A, N1, 0, LOGICAL, K1, RESIDUE, 0, NOW + 1, LEASE));
        assertEquals(RESIDUE, key(residue).baseState);
        assertEquals(FROZEN, key(residue).frozenSinceMillis);
        assertNull(key(residue).actionId);
        // rejections: a residue has no tombstone time, the time is not negative, the phase, the owner
        assertRejected("cannot have a tombstone time", owned, new ReleaseKey(A, N1, 0, LOGICAL, K1, RESIDUE, TT, NOW, LEASE));
        assertRejected("cannot have a tombstone time", owned, new ReleaseKey(A, N1, 0, LOGICAL, K1, TOMBSTONED, -1, NOW, LEASE));
        assertRejected("not STEADY", forwardIn(Phase.CLEANUP, A, RESIDUE, 0, LayoutRegistry.FORMAT_VERSION_2), new ReleaseKey(A, N1, 0, LOGICAL, K1, TOMBSTONED, TT, NOW, LEASE));
        assertRejected("not STEADY", reverseIn(Phase.REVERSE_ANNOUNCED), new ReleaseKey(A, N1, 0, LOGICAL, K1, TOMBSTONED, TT, NOW, LEASE));
        assertTrue(registry(owned).withReleasedKey(LOGICAL, K1, B, TOMBSTONED, TT, null).rejection.contains("owned by action " + A + ", not " + B));
        // already released: unchanged, whatever the base state asked for
        ClusterMetadata again = execute(tombstoned, new ReleaseKey(A, N1, 1, LOGICAL, K1, RESIDUE, 0, NOW + 2, LEASE));
        assertEquals(TOMBSTONED, key(again).baseState);
        // a reverse aborted to STEADY and released: the abort's base state is kept, the release reports its own
        ClusterMetadata aborted = execute(reverseIn(Phase.REVERSE_VERIFY), advance(0, Phase.REVERSE_VERIFY, Phase.STEADY, 1));
        assertEquals(RESIDUE, key(aborted).baseState);
        assertEquals(A, key(aborted).actionId);
        ClusterMetadata releasedAfterAbort = execute(aborted, new ReleaseKey(A, N1, 1, LOGICAL, K1, TOMBSTONED, TT + 1, NOW + 3, LEASE));
        assertEquals(TT + 1, key(releasedAfterAbort).baseTombstoneMicros);
        assertEquals(NOW + 2, key(releasedAfterAbort).frozenSinceMillis);
    }

    // ---- base state on transitions ----

    @Test
    public void entryIntoC3FromALowerClassSetsFrozenSince()
    {
        // forward: CUTOVER -> NEW_ONLY, in a format 2 registry
        ClusterMetadata md = execute(forwardIn(Phase.CUTOVER, A, RESIDUE, 0, LayoutRegistry.FORMAT_VERSION_2), advance(0, Phase.CUTOVER, Phase.NEW_ONLY, 1));
        assertEquals(NOW + 2, key(md).frozenSinceMillis);
        // C3 to C3 changes nothing
        md = execute(md, advance(1, Phase.NEW_ONLY, Phase.CLEANUP, 1));
        assertEquals(NOW + 2, key(md).frozenSinceMillis);
        // reverse: C2 -> STEADY (C3) from a complete generation: window restarts
        for (Phase phase : new Phase[]{ Phase.REVERSE_VERIFY, Phase.REVERSE_ROLLBACK })
            for (KeyState.ReverseMode mode : KeyState.ReverseMode.values())
            {
                KeyState k = key(execute(reverseIn(phase, mode), advance(0, phase, Phase.STEADY, 1)));
                String what = phase + " " + mode;
                assertEquals(what, RESIDUE, k.baseState);
                assertEquals(what, NOW + 2, k.frozenSinceMillis);
                assertEquals(what, 0, k.baseTombstoneMicros);
                assertNull(what, k.reverseMode);
                assertEquals(what, 0, k.reverseDualSinceMillis);
            }
    }

    @Test
    public void abortRulesSetTheBaseState()
    {
        // from REVERSE_ANNOUNCED nothing was routed differently: the base state is exactly what it was
        for (KeyState.ReverseMode mode : KeyState.ReverseMode.values())
        {
            ClusterMetadata md = reverseIn(Phase.REVERSE_ANNOUNCED, mode);
            KeyState before = key(md);
            KeyState k = key(execute(md, advance(0, Phase.REVERSE_ANNOUNCED, Phase.STEADY, 1)));
            assertEquals(before.baseState, k.baseState);
            assertEquals(before.frozenSinceMillis, k.frozenSinceMillis);
            assertEquals(before.baseTombstoneMicros, k.baseTombstoneMicros);
            assertNull(k.reverseMode);
        }
        // a partial copy, fast mode: RESIDUE with the old frozenSince
        for (Phase phase : new Phase[]{ Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_BACKFILL })
        {
            KeyState fast = key(execute(reverseIn(phase, FAST), advance(0, phase, Phase.STEADY, 1)));
            assertEquals(RESIDUE, fast.baseState);
            assertEquals(FROZEN, fast.frozenSinceMillis);
            assertEquals(0, fast.baseTombstoneMicros);
            assertEquals(0, fast.reverseDualSinceMillis);
            // slow mode: RESIDUE with the window already closed, and the old tombstone is gone
            KeyState slow = key(execute(reverseIn(phase, SLOW), advance(0, phase, Phase.STEADY, 1)));
            assertEquals(RESIDUE, slow.baseState);
            assertEquals(KeyState.WINDOW_CLOSED_MILLIS, slow.frozenSinceMillis);
            assertEquals(0, slow.baseTombstoneMicros);
            assertNull(slow.reverseMode);
        }
        // a failed verification re-backfills in the same mode and keeps the base state
        KeyState again = key(execute(reverseIn(Phase.REVERSE_VERIFY, SLOW), advance(0, Phase.REVERSE_VERIFY, Phase.REVERSE_BACKFILL, 2)));
        assertEquals(SLOW, again.reverseMode);
        assertEquals(TOMBSTONED, again.baseState);
        assertEquals(DUAL, again.reverseDualSinceMillis);
        assertTrue(again.backfillDone.isEmpty());
    }

    @Test
    public void reverseDualSinceIsSetByReverseDualWriteAndByNothingElse()
    {
        KeyState k = key(execute(reverseIn(Phase.REVERSE_ANNOUNCED), advance(0, Phase.REVERSE_ANNOUNCED, Phase.REVERSE_DUAL_WRITE, 1)));
        assertEquals(NOW + 2, k.reverseDualSinceMillis);
        k = key(execute(execute(reverseIn(Phase.REVERSE_DUAL_WRITE), advance(0, Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_BACKFILL, 1)),
                        advance(1, Phase.REVERSE_BACKFILL, Phase.REVERSE_VERIFY, 1)));
        assertEquals(DUAL, k.reverseDualSinceMillis);
        // a restart of REVERSE_ANNOUNCED (a purge retry) keeps the mode
        KeyState restarted = key(execute(reverseIn(Phase.REVERSE_ANNOUNCED, SLOW), advance(0, Phase.REVERSE_ANNOUNCED, Phase.REVERSE_ANNOUNCED, 2)));
        assertEquals(SLOW, restarted.reverseMode);
        assertEquals(TT, restarted.baseTombstoneMicros);
    }

    @Test
    public void replicaLimitsOfDesign115()
    {
        int gcGrace = 28800; // G = 14,400,000 ms
        long g = gcGrace * 1000L / 2;
        long wt = 2000;
        Epoch e = Epoch.create(1);
        KeyState fast = new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), RESIDUE, 100, 0, FAST, 500);
        assertEquals(100 + g - wt, fast.reverseReplicaLimitMillis(gcGrace, wt));
        // fast: the smaller of frozenSince and reverseDualSince
        KeyState later = new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), RESIDUE, 900, 0, FAST, 500);
        assertEquals(500 + g - wt, later.reverseReplicaLimitMillis(gcGrace, wt));
        // slow: reverseDualSince, whatever frozenSince is
        KeyState slow = new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), TOMBSTONED, 100, TT, SLOW, 500);
        assertEquals(500 + g - wt, slow.reverseReplicaLimitMillis(gcGrace, wt));
        // before REVERSE_DUAL_WRITE nothing may be written
        KeyState announced = new KeyState(Phase.REVERSE_ANNOUNCED, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), RESIDUE, 100, 0, FAST, 0);
        assertEquals(Long.MIN_VALUE, announced.reverseReplicaLimitMillis(gcGrace, wt));
        // the forward limit is untouched
        assertEquals(400 + g, fast.backfillLimitMillis(gcGrace));
    }

    // ---- serializers and registration ----

    @Test
    public void beginUnsplitAndReleaseKeySerializers() throws IOException
    {
        LayoutTransformation[] all = { begin(FAST, null), begin(SLOW, B),
                                       new ReleaseKey(A, N1, 3, LOGICAL, K1, TOMBSTONED, TT, NOW, LEASE),
                                       new ReleaseKey(A, N1, 3, LOGICAL, K1, RESIDUE, 0, NOW, LEASE),
                                       new ReleaseKey(A, N1, 3, LOGICAL, K1, TOMBSTONED, 0, NOW, LEASE) };
        for (LayoutTransformation t : all)
        {
            assertEquals(t, LayoutTransformationTest.roundTrip(t));
            assertEquals(t.hashCode(), LayoutTransformationTest.roundTrip(t).hashCode());
            assertEquals(t.toString(), LayoutTransformationTest.roundTrip(t).toString());
            assertEquals(t, CustomTransformation.serializer.deserialize(serialized(t.wrap()), VERSION).child());
        }
        assertEquals("org.apache.cassandra.autonomic.layout.BeginUnsplit", BeginUnsplit.NAME);
        assertSame(BeginUnsplit.serializer, CustomTransformation.getSerializer(BeginUnsplit.NAME));
        // the release that format 1 meant is written as it always was, and a format 1 entry of the log still reads
        ReleaseKey legacy = new ReleaseKey(A, N1, 3, LOGICAL, K1, NOW, LEASE);
        DataOutputBuffer out = new DataOutputBuffer();
        ReleaseKey.serializer.serialize(legacy, out, VERSION);
        assertEquals(1, out.buffer().get(0));
        assertEquals(legacy, ReleaseKey.serializer.deserialize(new DataInputBuffer(out.buffer(), false), VERSION));
        assertEquals(TOMBSTONED, ((ReleaseKey) ReleaseKey.serializer.deserialize(new DataInputBuffer(out.buffer(), false), VERSION)).baseState);
        DataOutputBuffer richer = new DataOutputBuffer();
        ReleaseKey.serializer.serialize(all[2], richer, VERSION);
        assertEquals(2, richer.buffer().get(0));
        richer.buffer().put(0, (byte) 3);
        try
        {
            ReleaseKey.serializer.deserialize(new DataInputBuffer(richer.buffer(), false), VERSION);
            fail("an unknown format must not be read");
        }
        catch (IOException expected)
        {
            assertTrue(expected.getMessage().contains("format"));
        }
    }

    private static DataInputBuffer serialized(CustomTransformation t) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        CustomTransformation.serializer.serialize(t, out, VERSION);
        assertEquals(out.getLength(), CustomTransformation.serializer.serializedSize(t, VERSION));
        return new DataInputBuffer(out.buffer(), false);
    }

    @Test
    public void beginUnsplitIsDeterministic() throws IOException
    {
        ClusterMetadata prev = forwardIn(Phase.STEADY, null, TOMBSTONED, TT, LayoutRegistry.FORMAT_VERSION_2);
        BeginUnsplit t = begin(SLOW, null);
        ClusterMetadata one = execute(prev, t);
        ClusterMetadata two = LayoutTransformationTest.roundTrip(t).wrap().execute(LayoutTransformationTest.roundTrip(prev)).success().metadata;
        assertEquals(registry(one), registry(two));
        assertEquals(LayoutTransformationTest.bytes(one), LayoutTransformationTest.bytes(two));
    }

    @Test
    public void keyStateEqualityCoversTheNewFields()
    {
        Epoch e = Epoch.create(1);
        KeyState base = new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), RESIDUE, 100, 0, FAST, 500);
        assertEquals(base, new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), RESIDUE, 100, 0, FAST, 500));
        KeyState[] differing = {
            new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), TOMBSTONED, 100, 0, FAST, 500),
            new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), RESIDUE, 101, 0, FAST, 500),
            new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), RESIDUE, 100, 1, FAST, 500),
            new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), RESIDUE, 100, 0, SLOW, 500),
            new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), RESIDUE, 100, 0, FAST, 501) };
        for (KeyState other : differing)
            assertFalse(other.toString(), base.equals(other));
        try
        {
            new KeyState(Phase.REVERSE_BACKFILL, e, 0, 1, 1, e, ImmutableSet.of(), A, 400, ImmutableSet.of(), RESIDUE, -1, 0, FAST, 500);
            fail("negative times are rejected");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }
}
