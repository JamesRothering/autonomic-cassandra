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
 * T6.12: the forward chain with node 2 held one class behind across every commit, from ANNOUNCED to STEADY: node 1
 * is in the next phase and node 2 in this one, the catalogue runs through both, and rule O1 is asserted at each
 * commit (see {@link LayoutCompatibilitySuite}).
 */
public class LayoutCompatibilityOneBehindTest extends LayoutCompatibilitySuite
{
    @Test
    public void catalogueWithACoordinatorOneClassBehind() throws Throwable
    {
        run(
            Step.start(),
            Step.to(Phase.ANNOUNCED).behind().noHold(),
            Step.to(Phase.DUAL_WRITE).behind().noHold(),
            Step.to(Phase.BACKFILL).behind().noHold(),
            Step.to(Phase.VERIFY).behind().noHold(),
            Step.to(Phase.CUTOVER).behind().noHold(),
            Step.to(Phase.NEW_ONLY).behind().noHold(),
            Step.to(Phase.CLEANUP).behind().noHold(),
            Step.to(Phase.STEADY).behind().noHold());
    }
}
