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
 * T6.12: the abort path, with node 2 held one class behind across each commit: CUTOVER, ROLLBACK_DUAL, ABORTING and
 * the key removed again (see {@link LayoutCompatibilitySuite}). Everything written while the key was split must still
 * be read back from the base table after the abort.
 */
public class LayoutCompatibilityRollbackTest extends LayoutCompatibilitySuite
{
    @Test
    public void catalogueAcrossAnAbort() throws Throwable
    {
        run(
            Step.reach(Phase.ANNOUNCED),
            Step.reach(Phase.DUAL_WRITE),
            Step.reach(Phase.BACKFILL),
            Step.reach(Phase.VERIFY),
            Step.to(Phase.CUTOVER),
            Step.to(Phase.ROLLBACK_DUAL).behind(),
            Step.to(Phase.ABORTING).behind(),
            Step.removed().behind());
    }
}
