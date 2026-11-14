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
package org.apache.cassandra.autonomic.knowledge;

import java.util.Collection;
import java.util.OptionalDouble;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;

/** Read-only view of knowledge, safe to call from any autonomic thread. */
public interface KnowledgeView
{
    OptionalDouble latest(SignalId signal, Subject subject);

    /** Points with {@code time >= nowMillis() - lookbackMillis}, oldest first. */
    Series series(SignalId signal, Subject subject, long lookbackMillis);

    Collection<Subject> subjects(SignalId signal);

    Collection<Diagnosis> activeDiagnoses();

    /** Local actions. */
    Collection<ActionRecord> recentActions(long lookbackMillis);

    /** The cluster ledger in the metadata this node has; {@code ClusterLedger.EMPTY} if absent. */
    ClusterLedger ledger();


    long nowMillis();
}
