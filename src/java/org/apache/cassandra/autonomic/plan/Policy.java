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
package org.apache.cassandra.autonomic.plan;

import java.util.Optional;
import java.util.Set;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;

/** Plan. Pure function. Returns empty if no action is appropriate. */
public interface Policy
{
    String name();

    /** Diagnosis types this policy handles. */
    Set<String> handles();

    Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge);

    /** Direction of an actuation of an actuator that is not a knob. */
    String APPLY = "APPLY";

    /**
     * Every signal this policy reads from knowledge, reads as diagnosis evidence, or names in the verify specs it
     * builds. {@code InputCoverageTest} checks that a sensor emits each one and that the code reads no other.
     */
    default Set<SignalId> inputs()
    {
        return Set.of();
    }

    /**
     * Every actuator move this policy may propose, each from {@link #actuation}. {@code InputCoverageTest} checks
     * that every direction of every actuator is proposed by some policy, or recorded as not used.
     */
    default Set<String> actuations()
    {
        return Set.of();
    }

    /** @param direction {@code UP} or {@code DOWN} for a knob, {@link #APPLY} otherwise */
    static String actuation(String actuator, String direction)
    {
        return actuator + '/' + direction;
    }
}
