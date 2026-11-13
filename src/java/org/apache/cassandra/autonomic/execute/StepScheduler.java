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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The actuator thread as seen by {@link ActionExecutor}: a single thread that runs tasks sequentially. Delays are
 * real time (the dead-man deadline must not depend on anything a test or a stalled loop controls), so tests either
 * use the real executor with short windows or a manual implementation.
 */
public interface StepScheduler
{
    /** Runs the task on the actuator thread as soon as possible, after the tasks already queued. */
    void execute(Runnable task);

    /** Runs the task on the actuator thread after the delay; running the returned handle cancels it if not started. */
    Runnable schedule(Runnable task, long delayMillis);

    static StepScheduler of(ScheduledExecutorService executor)
    {
        return new StepScheduler()
        {
            @Override
            public void execute(Runnable task)
            {
                executor.execute(task);
            }

            @Override
            public Runnable schedule(Runnable task, long delayMillis)
            {
                ScheduledFuture<?> future = executor.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
                return () -> future.cancel(false);
            }
        };
    }
}
