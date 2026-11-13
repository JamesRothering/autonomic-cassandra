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
package org.apache.cassandra.autonomic.monitor.sensors;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Sensors must never call APIs that reset shared state (overview 3.5, ADR-013). */
public class NoResettingApiTest
{
    @Test
    public void noSensorReferencesGetAndResetStats() throws Exception
    {
        Path dir = Paths.get("src/java/org/apache/cassandra/autonomic/monitor/sensors");
        List<Path> sources;
        try (Stream<Path> s = Files.list(dir))
        {
            sources = s.filter(p -> p.toString().endsWith(".java")).collect(Collectors.toList());
        }
        assertFalse("no sensor sources found under " + new File(".").getAbsolutePath(), sources.isEmpty());
        for (Path source : sources)
        {
            String text = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
            assertEquals(source + " references getAndResetStats", -1, text.indexOf("getAndResetStats"));
        }
    }
}
