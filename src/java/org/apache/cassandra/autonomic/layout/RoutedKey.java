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

import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;

/**
 * The routing state of one key that an operation routed, as the coordinator saw it before the operation started
 * (rule C1 of design 4.3). An operation on a key that is in the registry, in any phase, is routed; it may be
 * acknowledged or returned only if {@link #unchangedIn} holds afterwards. Shared by the write and the read path.
 *
 * <p>The routing state is the layout, the incarnation and the phase. The layout (its id and its overflow table) is
 * part of it because a key removed with its layout and registered again starts over at incarnation 1. The phase epoch
 * is not: a backfill restart sets it without changing the phase, and backfill reports change neither.
 */
final class RoutedKey
{
    final TableLayout layout;
    final ByteBuffer key;
    final KeyState state;

    private RoutedKey(TableLayout layout, ByteBuffer key, KeyState state)
    {
        this.layout = layout;
        this.key = key;
        this.state = state;
    }

    /**
     * The routing state of the key of the table, or null if operations on it are not routed: the table has no layout,
     * the key is not in it, or the layout is an orphan (its logical table is not in the schema).
     */
    static RoutedKey of(ClusterMetadata cm, LayoutRegistry registry, TableId table, ByteBuffer key)
    {
        TableLayout layout = registry.layout(table);
        if (layout == null)
            return null;
        KeyState state = layout.keyState(key);
        if (state == null || logical(cm, layout) == null)
            return null;
        return new RoutedKey(layout, TableLayout.key(key), state);
    }

    /** 0 to 3: C0 to C3 of design 3.2. */
    int coordinatorClass()
    {
        return state.phase.coordinatorClass;
    }

    boolean unchangedIn(ClusterMetadata cm, LayoutRegistry registry)
    {
        TableLayout now = registry.layout(layout.logical);
        if (now == layout)
            return true;
        if (now == null || !now.layoutId.equals(layout.layoutId) || !now.physical.equals(layout.physical) || logical(cm, now) == null)
            return false;
        KeyState s = now.keyState(key);
        return s != null && s.phase == state.phase && s.incarnation == state.incarnation;
    }

    /** True if every key is in the same routing state in {@code after}. */
    static boolean unchanged(Collection<RoutedKey> routed, ClusterMetadata after)
    {
        LayoutRegistry registry = LayoutRegistry.of(after);
        for (RoutedKey r : routed)
            if (!r.unchangedIn(after, registry))
                return false;
        return true;
    }

    /** The logical table of the layout in the schema of the metadata, or null: the layout is an orphan. */
    static TableMetadata logical(ClusterMetadata cm, TableLayout layout)
    {
        KeyspaceMetadata ks = cm.schema.getKeyspaces().getNullable(layout.keyspace);
        return ks == null ? null : ks.tables.getNullable(layout.logical);
    }

    /** The overflow table of the layout, which exists whenever its logical table does (design 2.5). */
    static TableMetadata physical(ClusterMetadata cm, TableLayout layout)
    {
        KeyspaceMetadata ks = cm.schema.getKeyspaces().getNullable(layout.keyspace);
        TableMetadata physical = ks == null ? null : ks.tables.getNullable(layout.physical);
        if (physical == null)
            throw new IllegalStateException("The overflow table " + layout.keyspace + '.' + layout.physicalName + " of an adapted layout is missing");
        return physical;
    }
}
