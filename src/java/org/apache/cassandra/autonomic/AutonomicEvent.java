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
package org.apache.cassandra.autonomic;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.diag.DiagnosticEventService;

/**
 * Diagnostic event of the autonomic layer (overview 10.3). Published only when diagnostic events are enabled and
 * someone subscribes to the event type.
 */
public final class AutonomicEvent extends DiagnosticEvent
{
    public enum AutonomicEventType
    {
        DIAGNOSIS_OPENED, DIAGNOSIS_CLEARED, ACTION_TRANSITION, LAYER_STATE
    }

    private final AutonomicEventType type;
    private final Map<String, Serializable> details;

    private AutonomicEvent(AutonomicEventType type, Map<String, Serializable> details)
    {
        this.type = type;
        this.details = details;
    }

    static void publishDiagnosis(AutonomicEventType type, Diagnosis d)
    {
        Map<String, Serializable> details = new HashMap<>();
        details.put("diagnosis_id", d.id.toString());
        details.put("type", d.type);
        details.put("subject", String.valueOf(d.subject));
        details.put("severity", d.severity.name());
        details.put("summary", d.summary == null ? "" : d.summary);
        publish(type, details);
    }

    static void publishAction(ActionRecord r, ActionState from, String detail)
    {
        Map<String, Serializable> details = new HashMap<>();
        details.put("action_id", r.id().toString());
        details.put("actuator", r.action.actuator);
        details.put("from", from == null ? "" : from.name());
        details.put("to", r.state.name());
        details.put("reason", r.stateReason == null ? "" : r.stateReason);
        details.put("detail", detail == null ? "" : detail);
        publish(AutonomicEventType.ACTION_TRANSITION, details);
    }

    static void publishLayerState(AutonomicService.State state)
    {
        Map<String, Serializable> details = new HashMap<>();
        details.put("state", state.name());
        publish(AutonomicEventType.LAYER_STATE, details);
    }

    private static void publish(AutonomicEventType type, Map<String, Serializable> details)
    {
        DiagnosticEventService service = DiagnosticEventService.instance();
        if (service.isEnabled(AutonomicEvent.class, type))
            service.publish(new AutonomicEvent(type, details));
    }

    @Override
    public Enum<AutonomicEventType> getType()
    {
        return type;
    }

    @Override
    public HashMap<String, Serializable> toMap()
    {
        return new HashMap<>(details);
    }
}
