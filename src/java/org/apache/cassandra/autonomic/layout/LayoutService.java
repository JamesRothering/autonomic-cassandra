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

package org.apache.cassandra.autonomic.layout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.listeners.ChangeListener;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * The data plane of adaptive schema (D6), started by {@code AutonomicService.start()} on every node whether or not
 * {@code autonomic.enabled} is set (FC1), because a node that does not run the layer must still follow a layout
 * registry written by one that does.
 *
 * <p>It registers one {@link ChangeListener} and reacts to changes of the registry only. It creates no thread while
 * the registry holds no key in a phase that needs backfill (BACKFILL or CLEANUP); the {@code LayoutBackfill} thread
 * is created lazily when one appears and released when none is left. What the thread does is a hook (T6.7).
 */
public final class LayoutService implements ChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(LayoutService.class);

    public static final String BACKFILL_THREAD = "LayoutBackfill";

    public static final LayoutService instance = new LayoutService();

    private ClusterMetadataService registered;
    private ExecutorService backfill;
    /** Threads released without an interrupt that may still be finishing a step; interrupted at shutdown. */
    private final List<ExecutorService> released = new ArrayList<>();
    private volatile Consumer<LayoutRegistry> backfillHook = BackfillTask::onRegistryChange;

    private volatile boolean active;

    @VisibleForTesting
    LayoutService()
    {
    }

    /** Registers the listener; idempotent. Starts no thread unless the current registry already needs one. */
    public synchronized void start()
    {
        ClusterMetadataService service = ClusterMetadataService.instance();
        if (service == null || registered == service)
            return;
        if (registered != null)
            registered.log().removeListener(this);
        service.log().addListener(this);
        registered = service;
        active = true;
        react(LayoutRegistry.of(ClusterMetadata.current()));
    }

    /** Removes the listener and releases the backfill thread. */
    public synchronized void shutdownBlocking()
    {
        if (registered != null)
            registered.log().removeListener(this);
        registered = null;
        active = false;
        stopBackfill(true);
    }

    /**
     * The work of the backfill thread, run on it once per registry change while it exists (T6.7); {@link BackfillTask}
     * unless replaced, and nothing if set to null. A call may run long: it must follow the current metadata itself
     * and return when it has nothing left to do, because the thread is not interrupted when the registry stops
     * needing it (see {@link #stopBackfill}).
     */
    public void setBackfillHook(Consumer<LayoutRegistry> hook)
    {
        backfillHook = hook == null ? registry -> {} : hook;
    }

    /** Test seam: reacts to notifications without a metadata log. */
    @VisibleForTesting
    synchronized void activate()
    {
        active = true;
    }

    @VisibleForTesting
    public synchronized boolean hasBackfillThread()
    {
        return backfill != null;
    }

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        LayoutRegistry after = LayoutRegistry.of(next);
        // the registry is an immutable value replaced on change: anything else in the metadata is not our business
        if (after.equals(LayoutRegistry.of(prev)))
            return;
        synchronized (this)
        {
            if (active)
                react(after);
        }
    }

    /** Called with the monitor held. */
    private void react(LayoutRegistry registry)
    {
        if (!needsBackfill(registry))
        {
            stopBackfill(false);
            return;
        }
        if (backfill == null)
            backfill = executorFactory().sequential(BACKFILL_THREAD);
        Consumer<LayoutRegistry> hook = backfillHook;
        backfill.execute(() -> {
            try
            {
                hook.accept(registry);
            }
            catch (Throwable t)
            {
                logger.error("Layout backfill hook failed", t);
            }
        });
    }

    static boolean needsBackfill(LayoutRegistry registry)
    {
        for (TableLayout layout : registry.byLogical.values())
            for (KeyState key : layout.keys.values())
                if (key.phase == Phase.BACKFILL || key.phase == Phase.CLEANUP)
                    return true;
        return false;
    }

    /**
     * Releases the backfill thread. Only process shutdown interrupts it: the thread may be reading sstables, and an
     * interrupt delivered during a read of a file channel closes that channel for every other reader of the sstable.
     * Otherwise the work in progress ends by itself, when it sees that its key left the phase it was started for.
     */
    private void stopBackfill(boolean interrupt)
    {
        ExecutorService e = backfill;
        backfill = null;
        released.removeIf(ExecutorService::isTerminated);
        if (e != null)
        {
            e.shutdown();
            released.add(e);
        }
        if (interrupt)
        {
            released.forEach(ExecutorService::shutdownNow);
            released.clear();
        }
    }
}
