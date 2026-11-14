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

import java.lang.reflect.Method;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Hook H9: the ledger serializers are registered by the static initializer of {@code CustomTransformation} alone,
 * and reading or writing a ledger entry needs nothing that a started daemon provides. The metadata log is replayed
 * before any service starts, on nodes where the layer is disabled, and by offline tools.
 *
 * <p>This test never initialises {@code DatabaseDescriptor} (every test class runs in its own JVM) and must not
 * reference it or any service class directly: it asserts that they were not even loaded.
 */
public class AutonomicTransformationsRegistrationTest
{
    private static final String[] MUST_NOT_BE_LOADED = { "org.apache.cassandra.config.DatabaseDescriptor",
                                                         "org.apache.cassandra.autonomic.AutonomicService",
                                                         "org.apache.cassandra.autonomic.AutonomicConfig",
                                                         "org.apache.cassandra.autonomic.coord.ClusterCoordinator",
                                                         "org.apache.cassandra.autonomic.coord.TcmEnvironment",
                                                         "org.apache.cassandra.service.StorageService",
                                                         "org.apache.cassandra.tcm.ClusterMetadataService",
                                                         "org.apache.cassandra.schema.Schema" };

    private static boolean loaded(String className) throws Exception
    {
        Method find = ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
        find.setAccessible(true);
        for (ClassLoader loader = AutonomicTransformationsRegistrationTest.class.getClassLoader(); loader != null; loader = loader.getParent())
            if (find.invoke(loader, className) != null)
                return true;
        return false;
    }

    @Test
    public void logEntriesAreReadableWithoutAStartedDaemon() throws Exception
    {
        // the probe itself works
        assertTrue(loaded(AutonomicTransformationsRegistrationTest.class.getName()));

        NodeId owner = new NodeId(3);
        UUID id = new UUID(5, 6);
        Claim claim = new Claim(id, "scale_out", "CLUSTER", owner, "CLAIMED", 0, 10, 20, ImmutableMap.of("k", "v"));
        LedgerTransformation[] all = { new SetClusterActionsEnabled(true),
                                       new ClaimAction(Lane.TOPOLOGY, claim, 10, 0),
                                       new AdvanceAction(id, owner, 0, "P1", ImmutableMap.of("k", "v2"), 11, 10),
                                       new ReleaseAction(id, owner, "COMMITTED", 12) };

        // nothing but the static initializer of CustomTransformation has registered anything
        for (LedgerTransformation t : all)
        {
            assertNotNull(t.name(), CustomTransformation.getSerializer(t.name()));
            DataOutputBuffer out = new DataOutputBuffer();
            CustomTransformation.serializer.serialize(t.wrap(), out, Version.V10);
            CustomTransformation read = CustomTransformation.serializer.deserialize(new DataInputBuffer(out.buffer(), false), Version.V10);
            assertEquals(t, read.child());
        }

        // the ledger value as a snapshot stores it: key (with the value's class name), then the value
        ClusterLedger ledger = ClusterLedger.EMPTY.withEnabled(true).ledger.withClaim(Lane.TOPOLOGY, claim, 10, 0, false).ledger;
        ClusterLedgerValue value = new ClusterLedgerValue(ledger);
        value.withLastModified(Epoch.create(42));
        DataOutputBuffer out = new DataOutputBuffer();
        ExtensionKey.serializer.serialize(ClusterLedgerValue.KEY, out, Version.V10);
        value.serialize(out, Version.V10);
        assertEquals(out.getLength(), ExtensionKey.serializer.serializedSize(ClusterLedgerValue.KEY, Version.V10) + value.serializedSize(Version.V10));

        DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
        ExtensionKey<?, ?> key = ExtensionKey.serializer.deserialize(in, Version.V10);
        assertEquals(ClusterLedgerValue.KEY, key);
        ExtensionValue<?> read = key.newValue();
        read.deserialize(in, Version.V10);
        assertEquals(value, read);
        assertEquals(ledger, read.getValue());
        assertEquals(Epoch.create(42), read.lastModified());

        for (String className : MUST_NOT_BE_LOADED)
            assertTrue(className + " was loaded by reading ledger entries", !loaded(className));
    }
}
