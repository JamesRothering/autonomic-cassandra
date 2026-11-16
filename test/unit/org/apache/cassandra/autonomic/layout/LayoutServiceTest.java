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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** T6.6: the layout service reacts to registry changes only and owns no thread while there is nothing to backfill. */
public class LayoutServiceTest
{
    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    private static LayoutService active(AtomicInteger calls, CountDownLatch... latch)
    {
        LayoutService service = new LayoutService();
        service.activate();
        service.setBackfillHook(r -> {
            calls.incrementAndGet();
            for (CountDownLatch l : latch)
                l.countDown();
        });
        return service;
    }

    @Test
    public void emptyRegistryCreatesNoThread()
    {
        AtomicInteger calls = new AtomicInteger();
        LayoutService service = active(calls);
        ClusterMetadata prev = LayoutTransformationTest.empty();
        // a metadata change that is not a registry change
        ClusterMetadata next = LayoutTransformationTest.start();
        service.notifyPostCommit(prev, next, false);
        assertFalse(service.hasBackfillThread());
        assertEquals(0, calls.get());
    }

    @Test
    public void unrelatedChangesDoNotReact() throws Exception
    {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch first = new CountDownLatch(1);
        LayoutService service = active(calls, first);
        ClusterMetadata backfill = LayoutTransformationTest.keyIn(Phase.BACKFILL);
        service.notifyPostCommit(LayoutTransformationTest.start(), backfill, false);
        assertTrue(first.await(10, TimeUnit.SECONDS));
        assertTrue(service.hasBackfillThread());

        // same registry, later epoch: ignored
        ClusterMetadata later = backfill.transformer().build().metadata;
        service.notifyPostCommit(backfill, later, false);
        Thread.sleep(200);
        assertEquals(1, calls.get());
        service.shutdownBlocking();
        assertFalse(service.hasBackfillThread());
    }

    @Test
    public void threadFollowsThePhasesThatNeedBackfill() throws Exception
    {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch first = new CountDownLatch(1);
        LayoutService service = active(calls, first);
        ClusterMetadata start = LayoutTransformationTest.start();
        ClusterMetadata dual = LayoutTransformationTest.keyIn(Phase.DUAL_WRITE);
        service.notifyPostCommit(start, dual, false);
        assertFalse("DUAL_WRITE needs no backfill", service.hasBackfillThread());

        ClusterMetadata backfill = LayoutTransformationTest.keyIn(Phase.BACKFILL, 1, ImmutableSet.of(new NodeId(1)));
        service.notifyPostCommit(dual, backfill, false);
        assertTrue(first.await(10, TimeUnit.SECONDS));
        assertTrue(service.hasBackfillThread());

        ClusterMetadata verify = LayoutTransformationTest.keyIn(Phase.VERIFY);
        service.notifyPostCommit(backfill, verify, false);
        assertFalse("VERIFY needs none", service.hasBackfillThread());

        ClusterMetadata cleanup = LayoutTransformationTest.keyIn(Phase.CLEANUP);
        service.notifyPostCommit(verify, cleanup, false);
        assertTrue(service.hasBackfillThread());
        ClusterMetadata gone = LayoutTransformationTest.start();
        service.notifyPostCommit(cleanup, gone, false);
        assertFalse("an empty registry releases the thread", service.hasBackfillThread());
    }

    @Test
    public void releasedThreadIsInterruptedOnlyAtShutdown() throws Exception
    {
        // T6.7: the hook may be reading sstables, which an interrupt would close for every reader
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        LayoutService service = new LayoutService();
        service.activate();
        service.setBackfillHook(r -> {
            running.countDown();
            try
            {
                Thread.sleep(60_000);
            }
            catch (InterruptedException e)
            {
                interrupted.countDown();
            }
        });
        ClusterMetadata backfill = LayoutTransformationTest.keyIn(Phase.BACKFILL);
        service.notifyPostCommit(LayoutTransformationTest.start(), backfill, false);
        assertTrue(running.await(10, TimeUnit.SECONDS));
        service.notifyPostCommit(backfill, LayoutTransformationTest.keyIn(Phase.VERIFY), false);
        assertFalse(service.hasBackfillThread());
        assertFalse("released, not interrupted", interrupted.await(500, TimeUnit.MILLISECONDS));
        service.shutdownBlocking();
        assertTrue("shutdown interrupts the released thread too", interrupted.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void inactiveServiceIgnoresNotifications()
    {
        LayoutService service = new LayoutService();
        service.notifyPostCommit(LayoutTransformationTest.start(), LayoutTransformationTest.keyIn(Phase.BACKFILL), false);
        assertFalse(service.hasBackfillThread());
    }

    @Test
    public void listensToPostCommitOnly() throws Exception
    {
        // the pre-commit callback must stay the default no-op: the registry of `next` is not yet current there
        assertEquals(ChangeListener.class,
                     LayoutService.class.getMethod("notifyPreCommit", ClusterMetadata.class, ClusterMetadata.class, boolean.class).getDeclaringClass());
    }

    // ---- O1 core ----

    @Test
    public void everyRequiredNodeMustAnswerAtOrBeyondTheTarget()
    {
        NodeId a = new NodeId(1), b = new NodeId(2);
        Epoch e5 = Epoch.create(5), e4 = Epoch.create(4);
        assertTrue(EpochWatermarks.satisfied(ImmutableSet.of(a, b), ImmutableMap.of(a, e5, b, Epoch.create(9)), e5));
        assertFalse("a lower epoch", EpochWatermarks.satisfied(ImmutableSet.of(a, b), ImmutableMap.of(a, e5, b, e4), e5));
        assertFalse("no answer is not an acknowledgement", EpochWatermarks.satisfied(ImmutableSet.of(a, b), ImmutableMap.of(a, e5), e5));
        assertTrue("answers of nodes that are not required do not matter", EpochWatermarks.satisfied(ImmutableSet.of(a), ImmutableMap.of(a, e5, b, e4), e5));
    }
}
