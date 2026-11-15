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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Test-only provider standing for the external system shared by every node: idempotent on the action id, counts
 * calls and the distinct nodes it was made to provision, and fails on demand.
 */
public class FakeScaleOutProvider implements ScaleOutProvider
{
    public Capabilities capabilities = new Capabilities(true, false, 1, 10, 1000);
    /** Accepted requests by action id: one entry is one node provisioned. */
    public final Map<UUID, ProvisionTicket> tickets = new LinkedHashMap<>();
    /** Every call, as {@code request:<action id>}, {@code status:<ticket>}, {@code cancel:<ticket>}. */
    public final List<String> calls = new ArrayList<>();
    public final Map<String, ProvisionStatus> statuses = new HashMap<>();
    public final Set<String> cancelled = new LinkedHashSet<>();
    public Map<String, String> attributes = Map.of("zone", "z1");
    /** Thrown by the call while set. A failing request is still accepted first if {@link #acceptBeforeFailing}. */
    public ProviderException failRequest;
    public ProviderException failStatus;
    public ProviderException failCancel;
    public boolean acceptBeforeFailing;
    /** Run after a request was accepted and inside a cancel, before they return. */
    public Runnable afterAccept = () -> {};
    public Runnable duringCancel = () -> {};

    @Override
    public String name()
    {
        return "fake";
    }

    @Override
    public Capabilities capabilities()
    {
        return capabilities;
    }

    @Override
    public ProvisionTicket requestNode(ScaleRequest request) throws ProviderException
    {
        calls.add("request:" + request.actionId);
        if (failRequest != null && !acceptBeforeFailing)
            throw failRequest;
        ProvisionTicket ticket = tickets.computeIfAbsent(request.actionId, id -> new ProvisionTicket("t" + (tickets.size() + 1), attributes));
        afterAccept.run();
        if (failRequest != null)
            throw failRequest;
        return ticket;
    }

    @Override
    public ProvisionStatus status(ProvisionTicket ticket) throws ProviderException
    {
        calls.add("status:" + ticket.id);
        if (failStatus != null)
            throw failStatus;
        return statuses.getOrDefault(ticket.id, ProvisionStatus.pending());
    }

    @Override
    public void cancel(ProvisionTicket ticket) throws ProviderException
    {
        calls.add("cancel:" + ticket.id);
        if (failCancel != null)
            throw failCancel;
        duringCancel.run();
        cancelled.add(ticket.id);
    }

    @Override
    public void releaseNode(String hostId, String actionId)
    {
        calls.add("release:" + hostId);
    }

    public int count(String call)
    {
        int n = 0;
        for (String c : calls)
            if (c.startsWith(call + ':'))
                n++;
        return n;
    }

    /** Action ids the provider was asked for, distinct. */
    public Set<String> requestIds()
    {
        Set<String> ids = new LinkedHashSet<>();
        for (String c : calls)
            if (c.startsWith("request:"))
                ids.add(c.substring("request:".length()));
        return ids;
    }

    /** Nodes provisioned and not cancelled. */
    public int liveNodes()
    {
        int n = 0;
        for (ProvisionTicket t : tickets.values())
            if (!cancelled.contains(t.id))
                n++;
        return n;
    }
}
