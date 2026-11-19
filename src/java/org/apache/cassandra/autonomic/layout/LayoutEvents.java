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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Failed equivalence verifications of the actuator that owns them, for the {@code layout.verify_failed} diagnosis,
 * and slow reverse migrations aborted on {@code purge_timeout}, for {@code layout.unsplit_residue}. The abort of such a
 * migration is recorded as an ordinary rollback, which carries no reason in the action record; the actuator states it
 * here. In memory, bounded, on the node that owned the action.
 */
public final class LayoutEvents
{
    private static final int MAX = 100;

    /** One migration aborted because its verification failed. */
    public static final class VerifyFailure
    {
        public final UUID actionId;
        public final String keyspace;
        public final String table;
        public final String keyHex;
        public final String detail;
        public final long atMillis;

        VerifyFailure(UUID actionId, String keyspace, String table, String keyHex, String detail, long atMillis)
        {
            this.actionId = actionId;
            this.keyspace = keyspace;
            this.table = table;
            this.keyHex = keyHex;
            this.detail = detail;
            this.atMillis = atMillis;
        }
    }

    private static final Map<UUID, VerifyFailure> FAILURES = new LinkedHashMap<>();

    /** A slow reverse aborted because a base replica's purge probe was not clean in {@code purge_timeout}. */
    public static final class UnsplitResidue
    {
        public final UUID actionId;
        public final String keyspace;
        public final String table;
        public final String keyHex;
        /** Per node, by node id: its purge reason and detail, as it published them. */
        public final Map<String, String> nodes;
        /** What the operator can do, one sentence per kind of reason. */
        public final String remedy;
        /** The largest timestamp of a residue above the base tombstone, {@link Long#MIN_VALUE} if none. */
        public final long largestResidueTimestamp;
        public final long atMillis;

        public UnsplitResidue(UUID actionId, String keyspace, String table, String keyHex, Map<String, String> nodes, String remedy,
                              long largestResidueTimestamp, long atMillis)
        {
            this.actionId = actionId;
            this.keyspace = keyspace;
            this.table = table;
            this.keyHex = keyHex;
            this.nodes = Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
            this.remedy = remedy;
            this.largestResidueTimestamp = largestResidueTimestamp;
            this.atMillis = atMillis;
        }
    }

    private static final Map<UUID, UnsplitResidue> RESIDUES = new LinkedHashMap<>();

    /** Idempotent per action. */
    public static synchronized void unsplitResidue(UnsplitResidue residue)
    {
        if (RESIDUES.containsKey(residue.actionId))
            return;
        if (RESIDUES.size() >= MAX)
            RESIDUES.remove(RESIDUES.keySet().iterator().next());
        RESIDUES.put(residue.actionId, residue);
    }

    public static synchronized List<UnsplitResidue> unsplitResidues()
    {
        return new ArrayList<>(RESIDUES.values());
    }

    private LayoutEvents()
    {
    }

    /** Idempotent per action: a step that runs again records the same failure. */
    public static synchronized void verifyFailed(UUID actionId, String keyspace, String table, String keyHex, String detail, long nowMillis)
    {
        if (FAILURES.containsKey(actionId))
            return;
        if (FAILURES.size() >= MAX)
            FAILURES.remove(FAILURES.keySet().iterator().next());
        FAILURES.put(actionId, new VerifyFailure(actionId, keyspace, table, keyHex, detail, nowMillis));
    }

    public static synchronized List<VerifyFailure> verifyFailures()
    {
        return new ArrayList<>(FAILURES.values());
    }

    public static synchronized void clear()
    {
        FAILURES.clear();
        RESIDUES.clear();
    }
}
