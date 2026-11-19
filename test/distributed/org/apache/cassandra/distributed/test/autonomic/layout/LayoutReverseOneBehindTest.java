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
 * T6.17j: the fast reverse with node 2 held one class behind across every commit from REVERSE_ANNOUNCED to the
 * removal of the key: node 1 is in the next phase and node 2 in this one, the catalogue runs through both, and rule O1 is
 * asserted at each commit (see {@link LayoutCompatibilitySuite}).
 */
public class LayoutReverseOneBehindTest extends LayoutReverseSuite
{
    @Test
    public void catalogueWithACoordinatorOneClassBehind() throws Throwable
    {
        run(script(split(),
                   Step.to(Phase.REVERSE_ANNOUNCED).behind().noHold(),
                   Step.to(Phase.REVERSE_DUAL_WRITE).behind().noHold(),
                   Step.to(Phase.REVERSE_BACKFILL).behind().noHold(),
                   Step.to(Phase.REVERSE_VERIFY).behind().noHold(),
                   Step.to(Phase.CUTBACK).behind().noHold(),
                   Step.to(Phase.BASE_ONLY).behind().noHold(),
                   Step.to(Phase.REVERSE_CLEANUP).behind().noHold(),
                   Step.removed().behind().noHold()));
    }
}
