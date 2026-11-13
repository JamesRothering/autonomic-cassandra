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

import org.apache.cassandra.autonomic.knowledge.KnowledgeView;

/**
 * Seam between the control loop and execution. Called on the control thread for every plan produced while the layer
 * is RUNNING. Must not block.
 * TODO T2.6: the implementation hands each action to the SafetyGovernor and the ActionExecutor.
 */
public interface PlanSink
{
    void accept(Plan plan, KnowledgeView knowledge);
}
