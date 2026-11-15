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
package org.apache.cassandra.autonomic.elastic;

import java.util.Objects;

/** What a provider reports about a ticket: PENDING, STARTED(address) or FAILED(reason). */
public final class ProvisionStatus
{
    public enum State
    {
        /** Requested, no Cassandra process started yet. */
        PENDING,
        /** A Cassandra process configured to join this cluster was started at {@link #address}. */
        STARTED,
        /** No node will come out of this ticket. */
        FAILED
    }

    private static final ProvisionStatus PENDING = new ProvisionStatus(State.PENDING, null, null);

    public final State state;
    /** STARTED: the broadcast address of the new node, as {@code host} or {@code host:storage_port}. */
    public final String address;
    /** FAILED: why. */
    public final String reason;

    private ProvisionStatus(State state, String address, String reason)
    {
        this.state = state;
        this.address = address;
        this.reason = reason;
    }

    public static ProvisionStatus pending()
    {
        return PENDING;
    }

    public static ProvisionStatus started(String address)
    {
        return new ProvisionStatus(State.STARTED, Objects.requireNonNull(address), null);
    }

    public static ProvisionStatus failed(String reason)
    {
        return new ProvisionStatus(State.FAILED, null, reason == null ? "no reason given" : reason);
    }

    @Override
    public String toString()
    {
        return state + (address != null ? "(" + address + ')' : reason != null ? "(" + reason + ')' : "");
    }
}
