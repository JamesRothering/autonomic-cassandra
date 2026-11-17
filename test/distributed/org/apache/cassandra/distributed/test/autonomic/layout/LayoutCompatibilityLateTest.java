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

import org.junit.Test;

import org.apache.cassandra.autonomic.layout.Phase;

/**
 * T6.12: the key is brought to VERIFY without running the catalogue, then held in CUTOVER, NEW_ONLY, CLEANUP and
 * STEADY, with the workload, the paging states and the commit of each next phase in between (see
 * {@link LayoutCompatibilitySuite}). Data written before the split must have been copied by the backfill.
 */
public class LayoutCompatibilityLateTest extends LayoutCompatibilitySuite
{
    @Test
    public void catalogueInEveryLatePhase() throws Throwable
    {
        run(
            Step.reach(Phase.ANNOUNCED),
            Step.reach(Phase.DUAL_WRITE),
            Step.reach(Phase.BACKFILL),
            Step.reach(Phase.VERIFY),
            Step.to(Phase.CUTOVER),
            Step.to(Phase.NEW_ONLY),
            Step.to(Phase.CLEANUP),
            Step.to(Phase.STEADY));
    }
}
