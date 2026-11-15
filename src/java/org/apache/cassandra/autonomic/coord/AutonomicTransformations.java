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

import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.FinishKey;
import org.apache.cassandra.autonomic.layout.RegisterKey;
import org.apache.cassandra.autonomic.layout.RemoveKey;
import org.apache.cassandra.autonomic.layout.RemoveOrphanLayout;
import org.apache.cassandra.autonomic.layout.ReportBackfill;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

/**
 * Registers the serializers of the ledger transformations with {@code CustomTransformation} (hook H9).
 *
 * <p>Called only from the static initializer of {@code CustomTransformation}, never at runtime: the registry is a
 * plain static map. It runs on every node of this fork, whether or not the autonomic layer is enabled, and in
 * offline tools, because the metadata log is replayed before any service starts and is shared by the whole cluster.
 * This class and everything it references must therefore depend only on serializer classes: not on
 * {@code AutonomicService}, {@code DatabaseDescriptor}, or anything else that needs a started daemon.
 */
public final class AutonomicTransformations
{
    private AutonomicTransformations()
    {
    }

    public static void register()
    {
        CustomTransformation.registerExtension(SetClusterActionsEnabled.NAME, SetClusterActionsEnabled.serializer);
        CustomTransformation.registerExtension(ClaimAction.NAME, ClaimAction.serializer);
        CustomTransformation.registerExtension(AdvanceAction.NAME, AdvanceAction.serializer);
        CustomTransformation.registerExtension(ReleaseAction.NAME, ReleaseAction.serializer);
        // adaptive schema (D6): the layout registry
        CustomTransformation.registerExtension(RegisterKey.NAME, RegisterKey.serializer);
        CustomTransformation.registerExtension(AdvanceLayout.NAME, AdvanceLayout.serializer);
        CustomTransformation.registerExtension(ReportBackfill.NAME, ReportBackfill.serializer);
        CustomTransformation.registerExtension(RemoveKey.NAME, RemoveKey.serializer);
        CustomTransformation.registerExtension(FinishKey.NAME, FinishKey.serializer);
        CustomTransformation.registerExtension(RemoveOrphanLayout.NAME, RemoveOrphanLayout.serializer);
    }
}
