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

import java.util.Map;

/**
 * Operator interface of the autonomic layer. Further operations (kill, revert, approve, history, ...) are added by
 * later tasks.
 */
public interface AutonomicServiceMBean
{
    String MBEAN_NAME = "org.apache.cassandra.db:type=AutonomicService";

    /** One of DISABLED, STARTING, RUNNING, PAUSED. */
    String getState();

    /** State, gates and armed flag as strings. */
    Map<String, String> getStatus();

    boolean isEnabled();

    /** Starts or shuts down the layer at runtime. */
    void setEnabled(boolean enabled);

    boolean isDryRun();

    void setDryRun(boolean dryRun);

    /** Stops approval of new actions; sensors and detectors keep running. */
    void pause(String reason);

    void resume();
}
