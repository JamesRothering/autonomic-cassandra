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

import java.util.Map;

import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.utils.TimeUUID;

/** The trivial in-memory knob actuator passes the contract; also the template for concrete actuator tests. */
public class FakeKnobActuatorTest extends AbstractActuatorContractTest
{
    private final FakeKnobActuator actuator = new FakeKnobActuator("fake_knob");
    private long ids;

    @Override
    protected Actuator actuator()
    {
        return actuator;
    }

    @Override
    protected Action newAction()
    {
        return new Action(TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L + ids++, 42L), actuator.name(), ActionScope.NODE, Subject.node(),
                          Map.of("value", String.valueOf(actuator.value + 16)), null);
    }

    @Override
    protected Object snapshot()
    {
        return actuator.value;
    }

    @Override
    protected void operatorOverride()
    {
        actuator.value = actuator.value + 1000;
    }
}
