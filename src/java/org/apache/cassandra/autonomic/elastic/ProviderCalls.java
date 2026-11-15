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
package org.apache.cassandra.autonomic.elastic;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.Clock;

/**
 * Runs a provider call on the calling thread, which must be the actuator thread, bounded by a timeout: a watchdog
 * interrupts the caller when the call overruns. Whatever the call throws becomes a {@link ProviderException}; only
 * one created with {@link ProviderException#rejected} says that nothing was done. A call that ignores the interrupt
 * cannot be stopped; {@link #stalled} reports it, and the layer then pauses itself as for any stalled actuator
 * thread (overview 7.5).
 */
public final class ProviderCalls
{
    /** Runs a task once after a delay on another thread; returns its cancellation. */
    public interface Watchdog
    {
        Runnable schedule(Runnable task, long delayMillis);
    }

    public interface Call<T>
    {
        T call() throws Exception;
    }

    private static final class Guard
    {
        boolean done;
        boolean fired;
    }

    private final Watchdog watchdog;
    private final LongSupplier clock;
    private volatile String running;
    private volatile long runningSinceMillis;

    public ProviderCalls(Watchdog watchdog)
    {
        this(watchdog, Clock.Global::currentTimeMillis);
    }

    public ProviderCalls(Watchdog watchdog, LongSupplier clock)
    {
        this.watchdog = watchdog;
        this.clock = clock;
    }

    /** The call that has been running for longer than the timeout, or null. Any thread. */
    public String stalled(long timeoutMillis)
    {
        String what = running;
        return what != null && clock.getAsLong() - runningSinceMillis > timeoutMillis ? what : null;
    }

    /** The watchdog only interrupts, so it may use the executor of short non-blocking tasks. */
    public static ProviderCalls production()
    {
        return new ProviderCalls((task, delayMillis) -> {
            java.util.concurrent.Future<?> f = ScheduledExecutors.scheduledFastTasks.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
            return () -> f.cancel(false);
        });
    }

    public <T> T call(String what, long timeoutMillis, Call<T> call) throws ProviderException
    {
        Thread caller = Thread.currentThread();
        Guard guard = new Guard();
        Runnable cancel = watchdog.schedule(() -> {
            synchronized (guard)
            {
                if (!guard.done)
                {
                    guard.fired = true;
                    caller.interrupt();
                }
            }
        }, Math.max(1, timeoutMillis));
        boolean interruptedByOthers = false;
        runningSinceMillis = clock.getAsLong();
        running = what;
        try
        {
            return call.call();
        }
        catch (Throwable t)
        {
            ScaleOutProviders.inspect(t);
            boolean timedOut;
            synchronized (guard)
            {
                timedOut = guard.fired;
            }
            if (timedOut)
                throw new ProviderException(what + " timed out after " + timeoutMillis + "ms", t);
            interruptedByOthers = t instanceof InterruptedException;
            if (t instanceof ProviderException)
                throw (ProviderException) t;
            throw new ProviderException(what + " failed: " + t, t);
        }
        finally
        {
            running = null;
            boolean fired;
            synchronized (guard)
            {
                guard.done = true;
                fired = guard.fired;
            }
            cancel.run();
            // the watchdog's interrupt must not leak into the next thing the actuator thread does
            if (fired)
                Thread.interrupted();
            else if (interruptedByOthers)
                caller.interrupt();
        }
    }
}
