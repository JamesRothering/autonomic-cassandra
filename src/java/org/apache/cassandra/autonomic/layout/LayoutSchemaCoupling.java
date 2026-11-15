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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;

/**
 * Hook H21 (design 4.8): client DDL against a table that has an adapted layout, applied inside
 * {@code AlterSchema.execute} on every node. It is a pure function of the metadata before the statement and the
 * keyspaces the statement produced: no clock, no configuration, no state of its own, so replay gives the same answer
 * everywhere.
 *
 * <p>For each layout of {@code prev}: dropping the logical table or its keyspace takes the overflow table with it;
 * creating-over, altering or dropping the overflow table by itself, changing the columns of the logical table or making
 * it ineligible is rejected; {@code gc_grace_seconds} is mirrored; everything else is left alone. A rejection is an
 * {@link InvalidRequestException}, which {@code AlterSchema} turns into a rejected transformation.
 *
 * <p>The owner creates the overflow table before {@code RegisterKey} (design 2.5); at that point the registry does not
 * yet name it, so that creation is not a change this class can see. Registry entries whose logical table is gone are
 * orphans: this class never touches the registry, see {@link #orphans}.
 */
public final class LayoutSchemaCoupling
{
    private LayoutSchemaCoupling()
    {
    }

    public static Keyspaces apply(ClusterMetadata prev, Keyspaces newKeyspaces)
    {
        LayoutRegistry registry = LayoutRegistry.of(prev);
        if (registry.byLogical.isEmpty())
            return newKeyspaces;

        Keyspaces result = newKeyspaces;
        for (TableLayout layout : registry.byLogical.values())
        {
            KeyspaceMetadata before = prev.schema.getKeyspaces().getNullable(layout.keyspace);
            if (before == null)
                continue; // orphan whose keyspace is already gone
            TableMetadata prevLogical = before.tables.getNullable(layout.logical);
            TableMetadata prevPhysical = before.tables.getNullable(layout.physical);

            KeyspaceMetadata after = result.getNullable(layout.keyspace);
            if (after == null)
                continue; // keyspace dropped: both tables go with it
            TableMetadata newLogical = after.tables.getNullable(layout.logical);
            TableMetadata newPhysical = after.tables.getNullable(layout.physical);

            if (prevLogical == null)
            {
                // orphan: the layout is removed by RemoveOrphanLayout, the overflow table by its owner afterwards
                if (prevPhysical != null && (newPhysical == null || !newPhysical.equals(prevPhysical)))
                    throw managed(layout);
                continue;
            }

            if (newLogical == null)
            {
                // logical table dropped: the overflow table follows in the same epoch; the registry entry becomes an orphan
                if (newPhysical != null)
                    result = result.withAddedOrUpdated(after.withSwapped(after.tables.without(newPhysical)));
                continue;
            }

            if (prevPhysical == null || newPhysical == null || !newPhysical.equals(prevPhysical))
                throw managed(layout);

            String name = layout.keyspace + '.' + layout.logicalName;
            List<Eligibility.Blocker> introduced = new ArrayList<>(Eligibility.structural(newLogical, after));
            introduced.removeAll(Eligibility.structural(prevLogical, before));
            if (!introduced.isEmpty())
                throw new InvalidRequestException(name + " has an adapted layout; the change would make the table ineligible: "
                                                  + Eligibility.describe(introduced));

            TableMetadata mirrored = newLogical.params.gcGraceSeconds == prevPhysical.params.gcGraceSeconds
                                     ? prevPhysical
                                     : prevPhysical.withSwapped(prevPhysical.params.unbuild().gcGraceSeconds(newLogical.params.gcGraceSeconds).build());
            String mismatch = PhysicalTables.mismatch(newLogical, mirrored);
            if (mismatch != null)
                throw new InvalidRequestException(name + " has an adapted layout; columns cannot be added, dropped, renamed or altered "
                                                  + "until the layout is removed (" + mismatch + ')');
            if (mirrored != prevPhysical)
                result = result.withAddedOrUpdated(after.withSwapped(after.tables.withSwapped(mirrored)));
        }
        return result;
    }

    /** The layouts of {@code metadata} whose logical table is not in its schema; their registry entries are to be removed. */
    public static List<TableLayout> orphans(ClusterMetadata metadata)
    {
        List<TableLayout> orphans = new ArrayList<>();
        for (TableLayout layout : LayoutRegistry.of(metadata).byLogical.values())
        {
            KeyspaceMetadata ks = metadata.schema.getKeyspaces().getNullable(layout.keyspace);
            if (ks == null || ks.tables.getNullable(layout.logical) == null)
                orphans.add(layout);
        }
        return orphans;
    }

    private static InvalidRequestException managed(TableLayout layout)
    {
        return new InvalidRequestException(layout.keyspace + '.' + layout.physicalName + " is managed by the adapted layout of "
                                           + layout.keyspace + '.' + layout.logicalName + " and cannot be created, altered or dropped");
    }
}
