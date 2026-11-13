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
package org.apache.cassandra.autonomic.execute;

/** Action states of the state machine in overview 7.2. */
public enum ActionState
{
    PROPOSED, REJECTED, APPROVED, APPLYING, VERIFYING, COMMITTED, ROLLING_BACK, ROLLED_BACK, SUPERSEDED, FAILED;

    public boolean isTerminal()
    {
        return this == REJECTED || this == COMMITTED || this == ROLLED_BACK || this == SUPERSEDED || this == FAILED;
    }
}
