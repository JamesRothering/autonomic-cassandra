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
package org.apache.cassandra.distributed.test.autonomic.layout;

import java.util.Map;

import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.distributed.api.IInstanceConfig;

/**
 * T6.17j: the T6.12 catalogue over the reverse migration. The key is brought to STEADY by hand as in the forward suite,
 * released, and then moved through the reverse phases (REVERSE_ANNOUNCED to REVERSE_CLEANUP, and the rollback) with
 * the catalogue, the workload and the paging states around every commit. The real reverse passes run (the copy of the
 * buckets to base, the reconcile of the cleanup, and in slow mode the replicas' purge of the base partition deletion);
 * only the owner's decisions are made by the script.
 */
abstract class LayoutReverseSuite extends LayoutCompatibilitySuite
{
    @Override
    void configure(IInstanceConfig cfg)
    {
        cfg.set("autonomic.safety", Map.of("cooldown_after_commit", "1s", "cooldown_after_rollback", "1s"));
    }

    /** The forward migration to STEADY without the catalogue, and the release of the key. */
    static Step[] split()
    {
        return new Step[]{ Step.reach(Phase.ANNOUNCED), Step.reach(Phase.DUAL_WRITE), Step.reach(Phase.BACKFILL), Step.reach(Phase.VERIFY),
                           Step.reach(Phase.CUTOVER), Step.reach(Phase.NEW_ONLY), Step.reach(Phase.CLEANUP), Step.reach(Phase.STEADY), Step.release() };
    }

    static Step[] script(Step[] split, Step... reverse)
    {
        Step[] all = new Step[split.length + reverse.length];
        System.arraycopy(split, 0, all, 0, split.length);
        System.arraycopy(reverse, 0, all, split.length, reverse.length);
        return all;
    }
}
