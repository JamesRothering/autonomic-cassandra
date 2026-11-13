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

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AutonomicLifecycleTest extends TestBaseImpl
{
    private static Set<String> threads(IInvokableInstance node)
    {
        return node.callOnInstance(() -> Thread.getAllStackTraces().keySet().stream()
                                               .map(Thread::getName)
                                               .filter(n -> n.contains("Autonomic"))
                                               .map(n -> n.replaceAll("^.*(Autonomic\\w+?)(:\\d+)?$", "$1"))
                                               .collect(Collectors.toSet()));
    }

    @Test
    public void disabledByDefault() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            assertTrue(threads(cluster.get(1)).isEmpty());
        }
    }

    @Test
    public void enabledCreatesThreeThreadsAndShutsDownCleanly() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.set("autonomic.enabled", true))
                                           .start()))
        {
            assertEquals(Set.of("AutonomicMonitor", "AutonomicControl", "AutonomicActuator"), threads(cluster.get(1)));
            cluster.get(1).shutdown().get(1, TimeUnit.MINUTES);
        }
    }
}
