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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/** Test-only actuator "thread": tasks and timers run when the test says so, on the test thread. */
public class ManualScheduler implements StepScheduler
{
    public static final class Timer
    {
        public final Runnable task;
        public final long delayMillis;
        public boolean cancelled;
        public boolean fired;

        Timer(Runnable task, long delayMillis)
        {
            this.task = task;
            this.delayMillis = delayMillis;
        }
    }

    private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
    public final List<Timer> timers = new ArrayList<>();
    public boolean rejectTasks;

    @Override
    public synchronized void execute(Runnable task)
    {
        if (rejectTasks)
            throw new java.util.concurrent.RejectedExecutionException("shut down");
        queue.add(task);
    }

    @Override
    public synchronized Runnable schedule(Runnable task, long delayMillis)
    {
        Timer timer = new Timer(task, delayMillis);
        timers.add(timer);
        return () -> timer.cancelled = true;
    }

    public synchronized int pending()
    {
        return queue.size();
    }

    /** Runs queued tasks, including ones they enqueue, until none is left. */
    public void runAll()
    {
        Runnable task;
        while ((task = poll()) != null)
            task.run();
    }

    private synchronized Runnable poll()
    {
        return queue.poll();
    }

    /** Fires every timer that was neither cancelled nor fired, as if its delay had elapsed. Returns how many fired. */
    public int fireTimers()
    {
        int n = 0;
        for (Timer timer : new ArrayList<>(timers))
        {
            if (timer.cancelled || timer.fired)
                continue;
            timer.fired = true;
            timer.task.run();
            n++;
        }
        return n;
    }

    public int activeTimers()
    {
        int n = 0;
        for (Timer timer : timers)
            if (!timer.cancelled && !timer.fired)
                n++;
        return n;
    }
}
