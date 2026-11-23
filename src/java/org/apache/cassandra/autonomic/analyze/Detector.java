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
package org.apache.cassandra.autonomic.analyze;

import java.util.List;
import java.util.Set;

import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;

/** Analyze. Pure function of knowledge. No side effects, no I/O: state from elsewhere is read in the background. */
public interface Detector
{
    String name();

    /**
     * Every signal this detector reads from knowledge, for its condition or its evidence. A signal it derives itself
     * is not listed. {@code InputCoverageTest} checks that a sensor emits each one and that the code reads no other.
     */
    Set<SignalId> requires();

    /** Returns the conditions currently true, after hysteresis. */
    List<Diagnosis> analyze(KnowledgeView knowledge);

    /** Called when the layer starts, before the first {@link #analyze}; a detector that reads state in the background starts here. */
    default void start()
    {
    }

    /** Called when the layer stops; must cancel anything {@link #start} scheduled. */
    default void stop()
    {
    }
}
