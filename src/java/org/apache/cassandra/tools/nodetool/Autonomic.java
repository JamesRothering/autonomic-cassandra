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

package org.apache.cassandra.tools.nodetool;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.autonomic.AutonomicServiceMBean;
import org.apache.cassandra.tools.NodeProbe;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Operator commands of the autonomic layer. Each subcommand is a thin call to one {@link AutonomicServiceMBean}
 * method.
 */
@Command(name = "autonomic",
         description = "Operate the autonomic layer",
         subcommands = { Autonomic.Status.class,
                         Autonomic.Pause.class,
                         Autonomic.Resume.class,
                         Autonomic.Kill.class,
                         Autonomic.DryRun.class,
                         Autonomic.Approve.class,
                         Autonomic.Reject.class,
                         Autonomic.Revert.class,
                         Autonomic.History.class,
                         Autonomic.Events.class,
                         Autonomic.Actuator.class,
                         Autonomic.Actuators.class,
                         Autonomic.Cluster.class,
                         Autonomic.Layout.class })
public class Autonomic extends AbstractCommand
{
    static final String ENABLE_WARNING =
    "WARNING: enabling cluster actions is ONE-WAY with respect to stock Apache Cassandra.\n" +
    "Once enabled, the cluster metadata log contains entries that stock Cassandra nodes cannot read.\n" +
    "Every node of the cluster must run this fork before and after enabling.\n" +
    "Re-run with --yes to enable cluster actions.";

    @Override
    protected void execute(NodeProbe probe)
    {
        AbstractCommand cmd = new Status();
        cmd.probe(probe);
        cmd.logger(output);
        cmd.run();
    }

    static AutonomicServiceMBean proxy(NodeProbe probe)
    {
        return probe.getAutonomicProxy();
    }

    static void printRecords(PrintStream out, List<Map<String, String>> records)
    {
        if (records.isEmpty())
        {
            out.println("No records.");
            return;
        }
        for (Map<String, String> record : records)
        {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> e : record.entrySet())
            {
                if (sb.length() > 0)
                    sb.append(' ');
                sb.append(e.getKey()).append('=').append(e.getValue());
            }
            out.println(sb);
        }
    }

    static boolean parseOnOff(String value)
    {
        if ("on".equalsIgnoreCase(value))
            return true;
        if ("off".equalsIgnoreCase(value))
            return false;
        throw new IllegalArgumentException("Expected 'on' or 'off', got '" + value + "'");
    }

    @Command(name = "status", description = "Print the state of the autonomic layer")
    public static class Status extends AbstractCommand
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            Map<String, String> status = proxy(probe).getStatus();
            PrintStream out = output.out;
            out.printf("State: %s%n", status.getOrDefault("state", "unknown"));
            out.printf("Enabled: %s%n", status.getOrDefault("enabled", "unknown"));
            out.printf("Dry run: %s%n", status.getOrDefault("dry_run", "unknown"));
            out.printf("Armed: %s%n", status.getOrDefault("armed", "unknown"));
            String paused = status.getOrDefault("paused", "unknown");
            if (status.containsKey("pause_reason"))
                out.printf("Paused: %s (reason: %s)%n", paused, status.get("pause_reason"));
            else
                out.printf("Paused: %s%n", paused);
            out.printf("Killed: %s%n", status.getOrDefault("killed", "unknown"));
            out.printf("In-flight actions: %s%n", status.getOrDefault("in_flight_actions", "n/a"));
            out.printf("Awaiting approval: %s%n", status.getOrDefault("awaiting_approval", "n/a"));
            out.printf("Cluster actions enabled: %s%n", status.getOrDefault("cluster_actions_enabled", "unknown"));
        }
    }

    @Command(name = "pause", description = "Stop approving new actions; sensors and detectors keep running")
    public static class Pause extends AbstractCommand
    {
        @Parameters(arity = "0..1", paramLabel = "reason", description = "Why the layer is paused")
        private String reason = "paused by operator";

        @Override
        protected void execute(NodeProbe probe)
        {
            proxy(probe).pause(reason);
            output.out.println("Autonomic layer paused: " + reason);
        }
    }

    @Command(name = "resume", description = "Allow new actions again; clears a kill and the circuit breaker")
    public static class Resume extends AbstractCommand
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            proxy(probe).resume();
            output.out.println("Autonomic layer resumed");
        }
    }

    @Command(name = "kill", description = "Pause and roll back every in-flight node-scoped action")
    public static class Kill extends AbstractCommand
    {
        @Parameters(arity = "0..1", paramLabel = "reason", description = "Why the layer is killed")
        private String reason = "killed by operator";

        @Override
        protected void execute(NodeProbe probe)
        {
            proxy(probe).kill(reason);
            output.out.println("Autonomic layer killed: " + reason);
        }
    }

    @Command(name = "dryrun", description = "Print or set dry-run mode (actions are decided but not executed)")
    public static class DryRun extends AbstractCommand
    {
        @Parameters(arity = "0..1", paramLabel = "on|off", description = "Turn dry-run on or off")
        private String value;

        @Override
        protected void execute(NodeProbe probe)
        {
            AutonomicServiceMBean autonomic = proxy(probe);
            if (value == null)
            {
                output.out.printf("Dry run: %s%n", autonomic.isDryRun());
                return;
            }
            autonomic.setDryRun(parseOnOff(value));
            output.out.printf("Dry run set to %s%n", autonomic.isDryRun());
        }
    }

    @Command(name = "approve", description = "Approve an action awaiting manual approval")
    public static class Approve extends AbstractCommand
    {
        @Parameters(arity = "1", paramLabel = "action-id", description = "Id of the action")
        private String actionId;

        @Override
        protected void execute(NodeProbe probe)
        {
            proxy(probe).approve(actionId);
            output.out.println("Approved action " + actionId);
        }
    }

    @Command(name = "reject", description = "Reject an action awaiting manual approval")
    public static class Reject extends AbstractCommand
    {
        @Parameters(arity = "1", paramLabel = "action-id", description = "Id of the action")
        private String actionId;

        @Override
        protected void execute(NodeProbe probe)
        {
            proxy(probe).reject(actionId);
            output.out.println("Rejected action " + actionId);
        }
    }

    @Command(name = "revert", description = "Restore every managed knob to its configured value")
    public static class Revert extends AbstractCommand
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            proxy(probe).revert();
            output.out.println("Reverted all managed knobs");
        }
    }

    @Command(name = "history", description = "Print recent actions (last 7 days) or, with --diagnoses, recent diagnoses (last day)")
    public static class History extends AbstractCommand
    {
        @Option(names = { "--diagnoses" }, description = "Show the diagnosis history instead of the action history")
        private boolean diagnoses;

        @Option(names = { "--limit" }, description = "Maximum number of records to print (default 20)")
        private int limit = 20;

        @Override
        protected void execute(NodeProbe probe)
        {
            checkArgument(limit >= 0, "--limit must not be negative");
            AutonomicServiceMBean autonomic = proxy(probe);
            List<Map<String, String>> records = diagnoses ? autonomic.getDiagnosisHistory(limit)
                                                          : autonomic.getHistory(limit);
            printRecords(output.out, records);
        }
    }

    @Command(name = "events", description = "Print the audit trail of one action, oldest first")
    public static class Events extends AbstractCommand
    {
        @Parameters(arity = "1", paramLabel = "action-id", description = "Id of the action")
        private String actionId;

        @Override
        protected void execute(NodeProbe probe)
        {
            printRecords(output.out, proxy(probe).getActionEvents(actionId));
        }
    }

    @Command(name = "actuator", description = "Enable or disable one actuator until restart")
    public static class Actuator extends AbstractCommand
    {
        @Parameters(arity = "1", paramLabel = "name", description = "Name of the actuator")
        private String name;

        @Parameters(arity = "1", paramLabel = "enable|disable", description = "Whether the actuator is enabled")
        private String state;

        @Override
        protected void execute(NodeProbe probe)
        {
            checkArgument("enable".equalsIgnoreCase(state) || "disable".equalsIgnoreCase(state),
                          "Expected 'enable' or 'disable', got '%s'", state);
            boolean enabled = "enable".equalsIgnoreCase(state);
            proxy(probe).setActuatorEnabled(name, enabled);
            output.out.printf("Actuator %s %s%n", name, enabled ? "enabled" : "disabled");
        }
    }

    @Command(name = "actuators", description = "List the actuators and their scopes")
    public static class Actuators extends AbstractCommand
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            for (Map.Entry<String, String> e : new TreeMap<>(proxy(probe).getActuators()).entrySet())
                output.out.printf("%s: %s%n", e.getKey(), e.getValue());
        }
    }

    @Command(name = "cluster",
             description = "Operate the cluster-scoped actions gate and ledger",
             subcommands = { Cluster.Status.class,
                             Cluster.Enable.class,
                             Cluster.Disable.class,
                             Cluster.Release.class })
    public static class Cluster extends AbstractCommand
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            AbstractCommand cmd = new Status();
            cmd.probe(probe);
            cmd.logger(output);
            cmd.run();
        }

        @Command(name = "status", description = "Print the cluster actions gate and claimed lanes")
        public static class Status extends AbstractCommand
        {
            @Override
            protected void execute(NodeProbe probe)
            {
                AutonomicServiceMBean autonomic = proxy(probe);
                Map<String, String> status = autonomic.getClusterStatus();
                for (Map.Entry<String, String> e : new TreeMap<>(status).entrySet())
                    output.out.printf("%s: %s%n", e.getKey(), e.getValue());
                output.out.println("Claims:");
                printRecords(output.out, autonomic.getClusterClaims());
            }
        }

        @Command(name = "enable", description = "Enable cluster-scoped actions (one-way for stock Cassandra nodes)")
        public static class Enable extends AbstractCommand
        {
            @Option(names = { "--yes" }, description = "Confirm the one-way change")
            private boolean yes;

            @Override
            protected void execute(NodeProbe probe)
            {
                if (!yes)
                {
                    output.out.println(ENABLE_WARNING);
                    return;
                }
                proxy(probe).enableClusterActions(true);
                output.out.println("Cluster actions enabled");
            }
        }

        @Command(name = "disable", description = "Disable cluster-scoped actions (new claims are stopped)")
        public static class Disable extends AbstractCommand
        {
            @Override
            protected void execute(NodeProbe probe)
            {
                proxy(probe).enableClusterActions(false);
                output.out.println("Cluster actions disabled");
            }
        }

        @Command(name = "release", description = "Forcibly remove the claim on a lane (TOPOLOGY, SCHEMA_LAYOUT or MAINTENANCE)")
        public static class Release extends AbstractCommand
        {
            @Parameters(arity = "1", paramLabel = "lane", description = "Lane to release")
            private String lane;

            @Override
            protected void execute(NodeProbe probe)
            {
                String released = proxy(probe).releaseClaim(lane);
                output.out.printf("Released lane %s, held by action %s%n", lane, released);
            }
        }
    }

    @Command(name = "layout",
             description = "Operate adaptive layouts: split hot partitions and follow their migration",
             subcommands = { Layout.Status.class,
                             Layout.Split.class,
                             Layout.Abort.class,
                             Layout.Resume.class })
    public static class Layout extends AbstractCommand
    {
        @Override
        protected void execute(NodeProbe probe)
        {
            AbstractCommand cmd = new Status();
            cmd.probe(probe);
            cmd.logger(output);
            cmd.run();
        }

        @Command(name = "status", description = "Print every adapted partition: phase, generation, owning action and what it waits for")
        public static class Status extends AbstractCommand
        {
            @Override
            protected void execute(NodeProbe probe)
            {
                printRecords(output.out, proxy(probe).getLayoutStatus());
            }
        }

        @Command(name = "split", description = "Propose to split one partition into buckets (passes the governor; manual approval by default)")
        public static class Split extends AbstractCommand
        {
            @Parameters(index = "0", arity = "1", paramLabel = "keyspace", description = "Keyspace of the table")
            private String keyspace;

            @Parameters(index = "1", arity = "1", paramLabel = "table", description = "Table")
            private String table;

            @Parameters(index = "2", arity = "1", paramLabel = "key-hex", description = "Raw partition key bytes in hex")
            private String keyHex;

            @Option(names = { "--buckets" }, description = "Number of buckets, 2 to 64 (default layout.default_buckets)")
            private int buckets = 0;

            @Override
            protected void execute(NodeProbe probe)
            {
                checkArgument(buckets == 0 || (buckets >= 2 && buckets <= 64), "--buckets must be between 2 and 64");
                Map<String, String> record = proxy(probe).splitPartition(keyspace, table, keyHex, buckets);
                printRecords(output.out, List.of(record));
            }
        }

        @Command(name = "abort", description = "Abort the split of a partition; refused past its commit point")
        public static class Abort extends AbstractCommand
        {
            @Parameters(index = "0", arity = "1", paramLabel = "keyspace", description = "Keyspace of the table")
            private String keyspace;

            @Parameters(index = "1", arity = "1", paramLabel = "table", description = "Table")
            private String table;

            @Parameters(index = "2", arity = "1", paramLabel = "key-hex", description = "Raw partition key bytes in hex")
            private String keyHex;

            @Override
            protected void execute(NodeProbe probe)
            {
                output.out.println(proxy(probe).abortSplit(keyspace, table, keyHex));
            }
        }

        @Command(name = "resume", description = "Take over a split that has no live action: abort it before its commit point, finish it after")
        public static class Resume extends AbstractCommand
        {
            @Parameters(index = "0", arity = "1", paramLabel = "keyspace", description = "Keyspace of the table")
            private String keyspace;

            @Parameters(index = "1", arity = "1", paramLabel = "table", description = "Table")
            private String table;

            @Parameters(index = "2", arity = "1", paramLabel = "key-hex", description = "Raw partition key bytes in hex")
            private String keyHex;

            @Override
            protected void execute(NodeProbe probe)
            {
                Map<String, String> record = proxy(probe).resumeSplit(keyspace, table, keyHex);
                printRecords(output.out, List.of(record));
            }
        }
    }
}
