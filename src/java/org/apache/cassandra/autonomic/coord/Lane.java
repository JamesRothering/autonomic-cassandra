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
package org.apache.cassandra.autonomic.coord;

/**
 * Lanes of the cluster ledger (overview 6.2): at most one claim exists per lane. The {@link #id} is the serialized
 * form and must never change; new lanes get new ids.
 */
public enum Lane
{
    TOPOLOGY(0, true),
    SCHEMA_LAYOUT(1, true),
    MAINTENANCE(2, false);

    public final int id;
    /** A claim on this lane requires that no multi-step metadata operation is in progress (overview 6.2, FC3). */
    public final boolean requiresNoInProgressSequences;

    Lane(int id, boolean requiresNoInProgressSequences)
    {
        this.id = id;
        this.requiresNoInProgressSequences = requiresNoInProgressSequences;
    }

    /** Null if the id is unknown. */
    public static Lane fromId(int id)
    {
        for (Lane lane : values())
            if (lane.id == id)
                return lane;
        return null;
    }
}
