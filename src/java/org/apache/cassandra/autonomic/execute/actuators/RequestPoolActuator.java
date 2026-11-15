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

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.service.StorageService;

/**
 * Maximum size of one request stage pool ({@code ReadStage}, {@code MutationStage}, {@code CounterMutationStage},
 * {@code ViewMutationStage}) through {@code StorageService.setConcurrency}. One instance per stage, each with its
 * own options: envelope {@code min_factor} (default 0.5) to {@code max_factor} (default 2.0) times the configured
 * size, never above eight times the available processors and never below one thread.
 *
 * <p>Only the maximum is written: the core size of these stages cannot be resized ({@code SEPExecutor} throws), and
 * {@code setConcurrency} skips it when it is negative.
 */
public class RequestPoolActuator extends KnobActuator
{
    public static final String READ = "read_pool";
    public static final String MUTATION = "mutation_pool";
    public static final String COUNTER_MUTATION = "counter_mutation_pool";
    public static final String VIEW_MUTATION = "view_mutation_pool";
    /** Pool name, as in the {@code pool.*} signals, of each actuator. */
    public static final Map<String, String> STAGES = ImmutableMap.of(READ, "ReadStage", MUTATION, "MutationStage",
                                                                  COUNTER_MUTATION, "CounterMutationStage", VIEW_MUTATION, "ViewMutationStage");
    public static final int MAX_PROCESSOR_FACTOR = 8;

    private final String stage;

    public RequestPoolActuator(String name, Handle handle, Map<String, Object> options, int processors)
    {
        super(name, handle, Math.max(1, handle.get() * option(options, "min_factor", 0.5)),
              Math.min(handle.get() * option(options, "max_factor", 2.0), (double) MAX_PROCESSOR_FACTOR * processors), 1, true);
        this.stage = STAGES.get(name);
    }

    public String stage()
    {
        return stage;
    }

    public static RequestPoolActuator create(String name, Map<String, Object> options)
    {
        String stage = STAGES.get(name);
        return new RequestPoolActuator(name, new Handle()
        {
            public double get()
            {
                return StorageService.instance.getConcurrency(List.of(stage)).get(stage).get(1);
            }

            public void set(double value)
            {
                StorageService.instance.setConcurrency(stage, -1, (int) Math.round(value));
            }
        }, options, Runtime.getRuntime().availableProcessors());
    }
}
