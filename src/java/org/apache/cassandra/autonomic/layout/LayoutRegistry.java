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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * The layout registry of design 2.3: the {@link TableLayout} of every logical table that has one. Immutable.
 *
 * <p>The {@code with*} methods are the whole logic of the layout transformations, written like those of
 * {@code ClusterLedger}: pure functions of their arguments (no clock, configuration or local state, epochs and
 * times are passed in), returning the new registry or the reason for the rejection. All size bounds of design 2.3
 * are enforced here, and again by the constructors so that a registry beyond them cannot be written or read.
 */
public final class LayoutRegistry
{
    public static final int FORMAT_VERSION = 1;
    public static final int HASH_ALGO = 1;
    public static final int MAX_LAYOUTS = 8;
    public static final int MAX_KEYS = 8;
    public static final int MAX_KEY_BYTES = 1024;
    public static final int MAX_BACKFILL_NODES = 32;
    public static final int MIN_BUCKETS = 2;
    public static final int MAX_BUCKETS = 64;
    /** Exclusive: {@code incarnation * 256 + bucket} must fit an int. */
    public static final int MAX_INCARNATION = 1 << 23;
    /** The bound of {@code SchemaConstants.NAME_LENGTH}, repeated so that nothing but the format is loaded to replay the log. */
    public static final int MAX_NAME_LENGTH = 48;

    public static final LayoutRegistry EMPTY = new LayoutRegistry(FORMAT_VERSION, ImmutableMap.of());

    public final int formatVersion;
    /** By logical table id, in id order. */
    public final ImmutableMap<TableId, TableLayout> byLogical;

    public LayoutRegistry(int formatVersion, Map<TableId, TableLayout> byLogical)
    {
        if (byLogical.size() > MAX_LAYOUTS)
            throw new IllegalArgumentException("registry has " + byLogical.size() + " layouts, the limit is " + MAX_LAYOUTS);
        this.formatVersion = formatVersion;
        this.byLogical = ImmutableSortedMap.copyOf(byLogical);
    }

    /** The registry of the given metadata; {@link #EMPTY} if none was ever written. */
    public static LayoutRegistry of(ClusterMetadata metadata)
    {
        ExtensionValue<?> value = metadata == null ? null : metadata.extensions.get(LayoutRegistryValue.KEY);
        return value instanceof LayoutRegistryValue ? ((LayoutRegistryValue) value).getValue() : EMPTY;
    }

    public static boolean present(ClusterMetadata metadata)
    {
        return metadata != null && metadata.extensions.get(LayoutRegistryValue.KEY) instanceof LayoutRegistryValue;
    }

    /** The layout of the logical table, or null. */
    public TableLayout layout(TableId logical)
    {
        return byLogical.get(logical);
    }

    // ---------------------------------------------------------------------------------------------------------
    // pure transitions
    // ---------------------------------------------------------------------------------------------------------

    /** Outcome of a transition: the resulting registry, or why it was rejected. */
    public static final class Result
    {
        /** Null if rejected. */
        public final LayoutRegistry registry;
        /** Null if accepted. */
        public final String rejection;
        /** True if accepted and the registry is the one the transition was applied to (an idempotent re-apply). */
        public final boolean unchanged;

        private Result(LayoutRegistry registry, String rejection, boolean unchanged)
        {
            this.registry = registry;
            this.rejection = rejection;
            this.unchanged = unchanged;
        }

        public boolean isRejected()
        {
            return rejection != null;
        }

        @Override
        public String toString()
        {
            return isRejected() ? "Rejected{" + rejection + '}' : "Accepted{" + registry + (unchanged ? ", unchanged}" : "}");
        }
    }

    private Result changed(TableId logical, TableLayout layout)
    {
        ImmutableSortedMap.Builder<TableId, TableLayout> map = ImmutableSortedMap.naturalOrder();
        byLogical.forEach((id, l) -> {
            if (!id.equals(logical))
                map.put(id, l);
        });
        if (layout != null)
            map.put(logical, layout);
        return new Result(new LayoutRegistry(formatVersion, map.build()), null, false);
    }

    private static Result reject(String reason)
    {
        return rejected(reason);
    }

    static Result rejected(String reason)
    {
        return new Result(null, reason, false);
    }

    private String lookup(TableId logical, ByteBuffer key)
    {
        TableLayout layout = byLogical.get(logical);
        if (layout == null)
            return "table " + logical + " has no layout";
        if (layout.keyState(key) == null)
            return "key is not registered in the layout of " + layout.keyspace + '.' + layout.logicalName;
        return null;
    }

    /**
     * {@code RegisterKey}: creates the layout if absent, or requires {@code header} to equal the existing one, and
     * adds the key in {@link Phase#ANNOUNCED} with the layout's next incarnation.
     */
    public Result withRegisteredKey(LayoutHeader header, ByteBuffer key, UUID actionId, Epoch epoch, long nowMillis)
    {
        if (header == null || key == null || actionId == null)
            return reject("no layout, key or action id");
        String invalid = header.invalid();
        if (invalid != null)
            return reject(invalid);
        if (key.remaining() == 0 || key.remaining() > MAX_KEY_BYTES)
            return reject("partition key of " + key.remaining() + " bytes, the limit is " + MAX_KEY_BYTES);
        TableLayout existing = byLogical.get(header.logical);
        if (existing == null)
        {
            if (byLogical.size() >= MAX_LAYOUTS)
                return reject("the registry already has " + MAX_LAYOUTS + " layouts");
            for (TableLayout other : byLogical.values())
                if (other.physical.equals(header.physical) || other.layoutId.equals(header.layoutId))
                    return reject("layout " + other.layoutId + " already uses that physical table or layout id");
            TableLayout created = new TableLayout(header, 1, ImmutableMap.of(), 0);
            return changed(header.logical, created.withNewKey(key, KeyState.announced(1, epoch, nowMillis, actionId)));
        }
        if (!existing.header().equals(header))
            return reject("the layout header differs from the registered " + existing.header());
        if (existing.keyState(key) != null)
            return reject("key is already registered in the layout of " + existing.keyspace + '.' + existing.logicalName);
        if (existing.keys.size() >= MAX_KEYS)
            return reject("the layout already has " + MAX_KEYS + " keys");
        if (existing.nextIncarnation >= MAX_INCARNATION)
            return reject("the layout has used all incarnations");
        return changed(header.logical, existing.withNewKey(key, KeyState.announced(existing.nextIncarnation, epoch, nowMillis, actionId)));
    }

    /**
     * {@code AdvanceLayout}: the key must be in {@code from} and owned by {@code actionId}. {@code from != to} must be
     * a transition of {@link Phase#isTransition}; {@code VERIFY -> BACKFILL} and {@code from == to} (only for
     * {@link Phase#restartable} phases) must carry the next generation, any other transition the current one.
     */
    public Result withAdvancedKey(TableId logical, ByteBuffer key, UUID actionId, Phase from, Phase to, int generation, Epoch epoch, long nowMillis)
    {
        if (logical == null || key == null || actionId == null || from == null || to == null)
            return reject("missing argument");
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        if (state.phase != from)
            return reject("key is in phase " + state.phase + ", not " + from);
        if (!actionId.equals(state.actionId))
            return reject("key is owned by action " + state.actionId + ", not " + actionId);
        boolean restart = from == to;
        if (restart ? !from.restartable() : !Phase.isTransition(from, to))
            return reject("illegal transition " + from + " -> " + to);
        boolean newGeneration = restart || (from == Phase.VERIFY && to == Phase.BACKFILL);
        int expected = newGeneration ? state.generation + 1 : state.generation;
        if (generation != expected)
            return reject("transition " + from + " -> " + to + " requires generation " + expected + ", not " + generation + " (current " + state.generation + ')');
        return changed(logical, layout.withKey(key, state.advanced(to, generation, epoch, nowMillis)));
    }

    /**
     * {@code ReportBackfill}: adds the node to {@link KeyState#backfillDone} of the current generation. Idempotent.
     * The report must name the current generation and the epoch at which it started: generation numbers start over
     * when a key is removed and registered again, the epoch does not.
     */
    public Result withBackfillReported(TableId logical, ByteBuffer key, int generation, Epoch generationEpoch, NodeId node)
    {
        if (logical == null || key == null || generationEpoch == null || node == null)
            return reject("missing argument");
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        if (!state.phase.acceptsBackfillReports())
            return reject("key is in phase " + state.phase + ", which takes no backfill reports");
        if (!state.backfillDue())
            return reject("key is in phase " + state.phase + " with the generation of its backfill; the reconcile generation has not started");
        if (state.generation != generation)
            return reject("key is at generation " + state.generation + ", not " + generation);
        if (!state.generationEpoch.equals(generationEpoch))
            return reject("generation " + generation + " of the key started at epoch " + state.generationEpoch.getEpoch() + ", not " + generationEpoch.getEpoch());
        if (state.backfillDone.contains(node))
            return new Result(this, null, true);
        if (state.backfillDone.size() >= MAX_BACKFILL_NODES)
            return reject("backfill_done already has " + MAX_BACKFILL_NODES + " nodes");
        return changed(logical, layout.withKey(key, state.withBackfillDone(node)));
    }

    /** {@code RemoveKey}: the key must be {@link Phase#ABORTING} and owned by {@code actionId}; the layout goes with its last key. */
    public Result withoutKey(TableId logical, ByteBuffer key, UUID actionId)
    {
        if (logical == null || key == null || actionId == null)
            return reject("missing argument");
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        if (state.phase != Phase.ABORTING)
            return reject("key is in phase " + state.phase + ", not " + Phase.ABORTING);
        if (!actionId.equals(state.actionId))
            return reject("key is owned by action " + state.actionId + ", not " + actionId);
        TableLayout remaining = layout.withoutKey(key);
        return changed(logical, remaining.keys.isEmpty() ? null : remaining);
    }

    /** {@code FinishKey}: the key must be {@link Phase#CLEANUP} and owned by {@code actionId}; it becomes {@link Phase#STEADY}, owned by nobody. */
    public Result withFinishedKey(TableId logical, ByteBuffer key, UUID actionId, Epoch epoch, long nowMillis)
    {
        if (logical == null || key == null || actionId == null)
            return reject("missing argument");
        String missing = lookup(logical, key);
        if (missing != null)
            return reject(missing);
        TableLayout layout = byLogical.get(logical);
        KeyState state = layout.keyState(key);
        if (state.phase != Phase.CLEANUP)
            return reject("key is in phase " + state.phase + ", not " + Phase.CLEANUP);
        if (!actionId.equals(state.actionId))
            return reject("key is owned by action " + state.actionId + ", not " + actionId);
        return changed(logical, layout.withKey(key, state.finished(epoch, nowMillis)));
    }

    /** {@code RemoveOrphanLayout}: removes the layout of the logical table, whatever the state of its keys. */
    public Result withoutLayout(TableId logical)
    {
        if (logical == null || !byLogical.containsKey(logical))
            return reject("table " + logical + " has no layout");
        return changed(logical, null);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof LayoutRegistry))
            return false;
        LayoutRegistry r = (LayoutRegistry) o;
        return formatVersion == r.formatVersion && byLogical.equals(r.byLogical);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(formatVersion, byLogical);
    }

    @Override
    public String toString()
    {
        return "LayoutRegistry{v" + formatVersion + ' ' + byLogical.values() + '}';
    }

    // ---------------------------------------------------------------------------------------------------------
    // Serialized form. The format version comes first and is independent of the TCM serialization version. A new
    // format may only be written once every node can read it; an unknown one fails loudly.
    // ---------------------------------------------------------------------------------------------------------

    static void serialize(LayoutRegistry r, DataOutputPlus out, Version version) throws IOException
    {
        if (r.formatVersion != FORMAT_VERSION)
            throw new IOException("Cannot write autonomic layout registry format " + r.formatVersion);
        out.writeUnsignedVInt32(r.formatVersion);
        out.writeUnsignedVInt32(r.byLogical.size());
        for (TableLayout layout : r.byLogical.values())
            TableLayout.serialize(layout, out, version);
    }

    static LayoutRegistry deserialize(DataInputPlus in, Version version) throws IOException
    {
        int format = in.readUnsignedVInt32();
        if (format != FORMAT_VERSION)
            throw new IOException("Unknown autonomic layout registry format " + format + "; this node reads format " + FORMAT_VERSION);
        int count = in.readUnsignedVInt32();
        if (count < 0 || count > MAX_LAYOUTS)
            throw new IOException("Corrupt autonomic layout registry: " + count + " layouts");
        ImmutableMap.Builder<TableId, TableLayout> layouts = ImmutableMap.builder();
        for (int i = 0; i < count; i++)
        {
            TableLayout layout = TableLayout.deserialize(in, version);
            layouts.put(layout.logical, layout);
        }
        return new LayoutRegistry(format, layouts.build());
    }

    static long serializedSize(LayoutRegistry r, Version version)
    {
        long size = TypeSizes.sizeofUnsignedVInt(r.formatVersion) + TypeSizes.sizeofUnsignedVInt(r.byLogical.size());
        for (TableLayout layout : r.byLogical.values())
            size += TableLayout.serializedSize(layout, version);
        return size;
    }
}
