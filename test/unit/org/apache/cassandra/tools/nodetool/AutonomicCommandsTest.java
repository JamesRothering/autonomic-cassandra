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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class AutonomicCommandsTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
        // the daemon starts the layer at startup; disabled by default, start() only registers the MBean
        AutonomicService.instance.start();
    }

    @Test
    public void statusOnDefaultNodeIsDisabled()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("autonomic", "status");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("State: DISABLED");
    }

    @Test
    public void dryRunWithoutArgumentPrintsCurrentValue()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("autonomic", "dryrun");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Dry run: ");
    }

    @Test
    public void clusterEnableWithoutYesPrintsWarningAndChangesNothing()
    {
        ToolRunner.ToolResult enable = ToolRunner.invokeNodetool("autonomic", "cluster", "enable");
        enable.assertOnCleanExit();
        assertThat(enable.getStdout()).contains("WARNING").contains("--yes");

        ToolRunner.invokeNodetool("autonomic", "status")
                  .asserts().success().stdoutContains("Cluster actions enabled: false");
    }

    @Test
    public void clusterWithoutSubcommandPrintsClusterStatus()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("autonomic", "cluster");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("Claims:");
    }

    @Test
    public void historyWithNoActionsIsEmpty()
    {
        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("autonomic", "history");
        tool.assertOnCleanExit();
        assertThat(tool.getStdout()).contains("No records.");
    }
}
