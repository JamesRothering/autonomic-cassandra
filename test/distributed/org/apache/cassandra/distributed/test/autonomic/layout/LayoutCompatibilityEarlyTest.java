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
 * T6.12: the catalogue with the key not registered, then held in ANNOUNCED, DUAL_WRITE, BACKFILL and VERIFY, with the
 * workload, the paging states and the commit of each next phase in between (see {@link LayoutCompatibilitySuite}).
 * The commit of CUTOVER is the last step; CUTOVER itself is held by {@link LayoutCompatibilityLateTest}.
 */
public class LayoutCompatibilityEarlyTest extends LayoutCompatibilitySuite
{
    @Test
    public void catalogueInEveryEarlyPhase() throws Throwable
    {
        run(
            Step.start(),
            Step.to(Phase.ANNOUNCED),
            Step.to(Phase.DUAL_WRITE),
            Step.to(Phase.BACKFILL),
            Step.to(Phase.VERIFY),
            Step.to(Phase.CUTOVER).noHold());
    }
}
