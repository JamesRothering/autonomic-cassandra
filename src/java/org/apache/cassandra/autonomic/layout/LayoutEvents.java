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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Failed equivalence verifications of the actuator that owns them, for the {@code layout.verify_failed} diagnosis.
 * The abort of such a migration is recorded as an ordinary rollback, which carries no reason in the action record;
 * the actuator states it here. In memory, bounded, on the node that owned the action.
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
    }
}
