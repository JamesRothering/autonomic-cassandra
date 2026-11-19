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

import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.Phase;

/**
 * T6.17j: as {@link LayoutReverseFastTest} for a slow reverse: the base partition holds a deletion at {@code Tt}, the
 * table has {@code gc_grace_seconds = 2} (with {@code Eligibility.setMinGcGraceSecondsForTesting}) until the replicas
 * have purged it, and the grace is then restored for the passes of the rest of the reverse.
 */
public class LayoutReverseSlowTest extends LayoutReverseSuite
{
    @Override
    KeyState.ReverseMode reverseMode()
    {
        return KeyState.ReverseMode.SLOW;
    }

    @Test
    public void catalogueInEveryReversePhase() throws Throwable
    {
        run(script(split(),
                   Step.to(Phase.REVERSE_ANNOUNCED),
                   Step.to(Phase.REVERSE_DUAL_WRITE),
                   Step.to(Phase.REVERSE_BACKFILL),
                   Step.to(Phase.REVERSE_VERIFY),
                   Step.to(Phase.CUTBACK),
                   Step.to(Phase.BASE_ONLY),
                   Step.to(Phase.REVERSE_CLEANUP),
                   Step.removed()));
    }
}
