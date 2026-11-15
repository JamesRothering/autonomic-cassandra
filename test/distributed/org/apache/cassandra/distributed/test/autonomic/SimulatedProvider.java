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
package org.apache.cassandra.distributed.test.autonomic;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.apache.cassandra.autonomic.elastic.ProviderException;
import org.apache.cassandra.autonomic.elastic.ProvisionStatus;
import org.apache.cassandra.autonomic.elastic.ProvisionTicket;
import org.apache.cassandra.autonomic.elastic.ScaleOutProvider;
import org.apache.cassandra.autonomic.elastic.ScaleRequest;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;

/**
 * A {@link ScaleOutProvider} for distributed tests: when asked for a node it starts a new in-JVM instance that joins
 * the cluster. Test code only.
 *
 * <p>An instance's classes are loaded by its own class loader, while the {@link Cluster} lives in the test's. The
 * provider that the layer builds inside the instance is therefore a thin front: it forwards each call, as strings,
 * through a {@code java.util.function.Function} that the test registers as a JVM system property object, to the
 * {@link Cloud} on the test side, where the SPI contract is kept: {@code requestNode} is idempotent on the action id
 * (the same ticket, never a second node), {@code status} is PENDING until the instance has been started and then
 * STARTED(address), {@code cancel} is idempotent and does nothing to an instance that has been started.
 */
public class SimulatedProvider implements ScaleOutProvider
{
    public static final String BRIDGE = "autonomic.test.simulated_provider";
    public static final String MAX_NODES = "max_nodes";

    private final int maxNodes;

    public SimulatedProvider(Map<String, String> parameters)
    {
        this.maxNodes = Integer.parseInt(parameters.getOrDefault(MAX_NODES, "4"));
    }

    @SuppressWarnings("unchecked")
    private static String call(String... parts) throws ProviderException
    {
        Object bridge = System.getProperties().get(BRIDGE);
        if (!(bridge instanceof Function))
            throw new ProviderException("no simulated cloud is registered");
        String answer = ((Function<String, String>) bridge).apply(String.join("|", parts));
        if (answer.startsWith("error|"))
            throw new ProviderException(answer.substring(6));
        return answer;
    }

    @Override
    public String name()
    {
        return "simulated";
    }

    @Override
    public Capabilities capabilities()
    {
        return new Capabilities(true, false, 1, maxNodes, 5_000);
    }

    @Override
    public ProvisionTicket requestNode(ScaleRequest request) throws ProviderException
    {
        return new ProvisionTicket(call("request", request.actionId.toString(), request.datacenter, request.rack), null);
    }

    @Override
    public ProvisionStatus status(ProvisionTicket ticket) throws ProviderException
    {
        String[] answer = call("status", ticket.id).split("\\|", 2);
        switch (answer[0])
        {
            case "PENDING":
                return ProvisionStatus.pending();
            case "STARTED":
                return ProvisionStatus.started(answer[1]);
            default:
                return ProvisionStatus.failed(answer.length > 1 ? answer[1] : null);
        }
    }

    @Override
    public void cancel(ProvisionTicket ticket) throws ProviderException
    {
        call("cancel", ticket.id);
    }

    @Override
    public void releaseNode(String hostId, String actionId)
    {
        // never called by the layer
    }

    /** The simulated cloud, on the test side: owns the cluster and starts the instances. */
    public static class Cloud implements Function<String, String>
    {
        private enum State
        {
            PENDING, STARTED, CANCELLED
        }

        private static final class Ticket
        {
            final String id;
            final String actionId;
            volatile State state = State.PENDING;
            volatile String address;
            volatile IInvokableInstance instance;
            volatile Thread thread;

            Ticket(String id, String actionId)
            {
                this.id = id;
                this.actionId = actionId;
            }
        }

        private final Cluster cluster;
        private final Map<String, Ticket> byAction = new ConcurrentHashMap<>();
        private final Map<String, Ticket> byId = new ConcurrentHashMap<>();
        private final List<String> calls = new CopyOnWriteArrayList<>();
        private volatile boolean held;
        private volatile Throwable failure;

        public Cloud(Cluster cluster)
        {
            this.cluster = cluster;
        }

        public Cloud register()
        {
            System.getProperties().put(BRIDGE, this);
            return this;
        }

        public void unregister()
        {
            System.getProperties().remove(BRIDGE, this);
        }

        /** While held, requests are accepted but no instance is started: the ticket stays PENDING. */
        public Cloud hold(boolean held)
        {
            this.held = held;
            if (!held)
                for (Ticket t : byId.values())
                    launch(t);
            return this;
        }

        /** Every call the provider made, as {@code verb|argument}. */
        public List<String> calls()
        {
            return new CopyOnWriteArrayList<>(calls);
        }

        public int requests()
        {
            return byAction.size();
        }

        public int started()
        {
            int n = 0;
            for (Ticket t : byId.values())
                if (t.state == State.STARTED)
                    n++;
            return n;
        }

        public int cancelled()
        {
            int n = 0;
            for (Ticket t : byId.values())
                if (t.state == State.CANCELLED)
                    n++;
            return n;
        }

        /** The instance started for a ticket, null if none. */
        public IInvokableInstance instance()
        {
            for (Ticket t : byId.values())
                if (t.instance != null)
                    return t.instance;
            return null;
        }

        public Throwable failure()
        {
            return failure;
        }

        public void await() throws InterruptedException
        {
            for (Ticket t : byId.values())
            {
                Thread thread = t.thread;
                if (thread != null)
                    thread.join(120_000);
            }
        }

        @Override
        public synchronized String apply(String call)
        {
            calls.add(call);
            String[] p = call.split("\\|");
            try
            {
                switch (p[0])
                {
                    case "request":
                    {
                        Ticket t = byAction.computeIfAbsent(p[1], action -> new Ticket("sim-" + action.substring(0, 8), action));
                        byId.put(t.id, t);
                        if (!held)
                            launch(t);
                        return t.id;
                    }
                    case "status":
                    {
                        Ticket t = byId.get(p[1]);
                        if (t == null)
                            return "FAILED|unknown ticket";
                        switch (t.state)
                        {
                            case STARTED:
                                return "STARTED|" + t.address;
                            case CANCELLED:
                                return "FAILED|cancelled";
                            default:
                                return "PENDING";
                        }
                    }
                    case "cancel":
                    {
                        Ticket t = byId.get(p[1]);
                        if (t != null && t.state == State.PENDING)
                            t.state = State.CANCELLED;
                        return "ok";
                    }
                    default:
                        return "error|unknown call " + call;
                }
            }
            catch (RuntimeException e)
            {
                return "error|" + e;
            }
        }

        /** Creates the instance, reports it STARTED and lets it join in the background. */
        private void launch(Ticket t)
        {
            synchronized (t)
            {
                if (t.state != State.PENDING || t.thread != null)
                    return;
                t.thread = new Thread(() -> {
                    try
                    {
                        IInstanceConfig config = cluster.newInstanceConfig().set("auto_bootstrap", true);
                        IInvokableInstance instance = cluster.bootstrap(config);
                        InetSocketAddress broadcast = instance.broadcastAddress();
                        t.instance = instance;
                        t.address = broadcast.getAddress().getHostAddress() + ':' + broadcast.getPort();
                        synchronized (Cloud.this)
                        {
                            if (t.state != State.PENDING)
                                return;
                            t.state = State.STARTED;
                        }
                        instance.startup();
                    }
                    catch (Throwable e)
                    {
                        failure = e;
                    }
                }, "simulated-cloud-" + t.id);
                t.thread.setDaemon(true);
                t.thread.start();
            }
        }
    }
}
