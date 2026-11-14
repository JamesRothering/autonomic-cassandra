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
package org.apache.cassandra.autonomic.execute.actuators;

import java.util.UUID;

import org.apache.cassandra.autonomic.execute.AbstractActuatorContractTest;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.utils.TimeUUID;

/** Contract test of one knob over a {@link FakeHandle}; the action moves the knob one step in {@link #direction()}. */
public abstract class AbstractKnobContractTest extends AbstractActuatorContractTest
{
    private long ids;

    protected abstract KnobActuator knob();

    protected abstract FakeHandle handle();

    /** Direction in which the knob has headroom. */
    protected KnobActuator.Direction direction()
    {
        return KnobActuator.Direction.UP;
    }

    @Override
    protected Actuator actuator()
    {
        return knob();
    }

    @Override
    protected Action newAction()
    {
        double target = knob().nextTarget(direction(), 0.25, null).orElseThrow(AssertionError::new);
        UUID id = TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L + ids++, 42L);
        return new Action(id, knob().name(), ActionScope.NODE, Subject.node(), KnobActuator.params(target, null), null);
    }

    @Override
    protected Object snapshot()
    {
        return handle().value;
    }

    @Override
    protected void operatorOverride()
    {
        handle().value = handle().value + 1;
    }
}
