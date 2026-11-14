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
package org.apache.cassandra.autonomic.analyze.detectors;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class RepairOverdueDetectorTest
{
    private static final long HOUR = 3_600_000;
    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-00000000000a");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-00000000000b");

    /** Holds scheduled tasks until the test runs them. */
    private static final class FakeScheduler implements RepairOverdueDetector.Scheduler
    {
        final Deque<CompletableFuture<?>> futures = new ArrayDeque<>();
        final Deque<Runnable> tasks = new ArrayDeque<>();
        final Deque<Long> delays = new ArrayDeque<>();

        public synchronized Future<?> schedule(Runnable task, long delayMillis)
        {
            CompletableFuture<Object> f = new CompletableFuture<>();
            futures.add(f);
            tasks.add(() -> {
                if (!f.isCancelled())
                    task.run();
            });
            delays.add(delayMillis);
            return f;
        }

        /** Runs the oldest scheduled task on the calling thread. */
        void runNext()
        {
            Runnable task;
            synchronized (this)
            {
                task = tasks.poll();
            }
            task.run();
        }

        synchronized int pending()
        {
            return tasks.size();
        }
    }

    private final Map<UUID, Long> finished = new HashMap<>();
    private final FakeScheduler scheduler = new FakeScheduler();
    private boolean enabled = true;
    private boolean unreadable;
    private volatile Thread readThread;
    private volatile CountDownLatch entered;
    private volatile CountDownLatch release;
    private int reads;
    private long now = 100 * HOUR;
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(8, 8), () -> now);

    private final RepairOverdueDetector.History history = new RepairOverdueDetector.History()
    {
        public boolean enabled(RepairType type)
        {
            return enabled;
        }

        public long intervalMillis(RepairType type)
        {
            return HOUR;
        }

        public Map<UUID, Long> lastFinishMillis(RepairType type, long freshAfterMillis) throws Exception
        {
            readThread = Thread.currentThread();
            reads++;
            if (entered != null)
            {
                entered.countDown();
                release.await(30, TimeUnit.SECONDS);
            }
            if (unreadable)
                throw new Exception("unavailable");
            return new HashMap<>(finished);
        }
    };

    @Before
    public void setUp()
    {
        finished.put(A, now - HOUR);
        finished.put(B, now - HOUR);
    }

    private RepairOverdueDetector detector()
    {
        RepairOverdueDetector d = new RepairOverdueDetector(new RepairOverdueDetector.Settings(2.0, 1.6, 2, true, 30_000, 100), history,
                                                            RepairType.FULL, 4.0, 30_000, scheduler, () -> now);
        d.start();
        return d;
    }

    /** One control tick: 15 s later, after the background read that was waiting has run. */
    private List<Diagnosis> tick(RepairOverdueDetector d)
    {
        now += 15_000;
        if (scheduler.pending() > 0)
            scheduler.runNext();
        return d.analyze(view);
    }

    @Test
    public void opensForTheOverdueHostOnlyAfterTheConfiguredReads()
    {
        RepairOverdueDetector d = detector();
        assertTrue(tick(d).isEmpty());
        finished.put(B, now - 3 * HOUR);
        assertTrue("first read above the threshold", tick(d).isEmpty());
        List<Diagnosis> out = tick(d);
        assertEquals(1, out.size());
        Diagnosis x = out.get(0);
        assertEquals("repair_overdue", x.type);
        assertEquals(Subject.peer(B.toString()), x.subject);
        assertEquals(Severity.WARN, x.severity);
        assertEquals("FULL", x.evidence.get("repair_type"));
        assertEquals("true", x.evidence.get("auto_repair_enabled"));
        assertTrue(x.evidence.get("repair.overdue_ratio").startsWith("3.0"));
    }

    @Test
    public void aSnapshotThatWasAlreadyCountedDoesNotCountAgain()
    {
        RepairOverdueDetector d = detector();
        finished.put(B, now - 3 * HOUR);
        scheduler.runNext();
        now += 1000;
        assertTrue(d.analyze(view).isEmpty());
        now += 1000;
        assertTrue("same read: no second evaluation", d.analyze(view).isEmpty());
        assertTrue(tick(d).size() == 1);
    }

    @Test
    public void criticalFromTheCriticalMultipleAndNeverRepairedCounts()
    {
        RepairOverdueDetector d = detector();
        finished.put(A, 0L);
        tick(d);
        Diagnosis x = tick(d).get(0);
        assertEquals(Severity.CRITICAL, x.severity);
        assertEquals("never", x.evidence.get("last_repair_finish_ms"));
    }

    @Test
    public void clearsAfterARepair()
    {
        RepairOverdueDetector d = detector();
        finished.put(B, now - 3 * HOUR);
        tick(d);
        assertEquals(1, tick(d).size());
        finished.put(B, now + 15_000);
        assertEquals("one read below clear does not close it", 1, tick(d).size());
        assertTrue(tick(d).isEmpty());
    }

    @Test
    public void disabledAutoRepairIsReportedAtInfoWithTheFlagThePolicyReads()
    {
        enabled = false;
        finished.put(B, 0L);
        RepairOverdueDetector d = detector();
        tick(d);
        Diagnosis x = tick(d).get(0);
        assertEquals(Severity.INFO, x.severity);
        assertEquals("false", x.evidence.get("auto_repair_enabled"));
        assertTrue(x.summary, x.summary.contains("disabled"));
    }

    @Test
    public void analyzeNeverTouchesTheHistoryAndDoesNotWaitForAReadInFlight() throws Exception
    {
        RepairOverdueDetector d = detector();
        assertEquals("start schedules the first read and does not run it", 0, reads);
        assertEquals(0L, (long) scheduler.delays.peek());
        entered = new CountDownLatch(1);
        release = new CountDownLatch(1);
        Thread reader = new Thread(scheduler::runNext);
        reader.start();
        assertTrue(entered.await(30, TimeUnit.SECONDS));
        // the read is blocked inside the history source: analyze still returns at once, without reading
        now += 15_000;
        assertTrue(d.analyze(view).isEmpty());
        assertEquals(1, reads);
        assertNotEquals(Thread.currentThread(), readThread);
        // one read in flight at most: the next one is not scheduled before this one completed
        assertEquals(0, scheduler.pending());
        release.countDown();
        reader.join(30_000);
        assertEquals(1, scheduler.pending());
        assertEquals(30_000L, (long) scheduler.delays.peekLast());
        assertEquals(1, reads);
    }

    @Test
    public void aFailedReadKeepsTheOldSnapshotUntilItAgesOut()
    {
        RepairOverdueDetector d = detector();
        finished.put(B, now - 3 * HOUR);
        tick(d);
        assertEquals(1, tick(d).size());
        unreadable = true;
        // reads fail from here on; the snapshot of 15 s ago is still younger than max_signal_age_ms (30 s)
        assertEquals(1, tick(d).size());
        assertEquals(3, reads);
        assertEquals(1, tick(d).size());
        // now older than 30 s: no measurement, so the diagnosis clears after two evaluations
        assertEquals(1, tick(d).size());
        assertTrue(tick(d).isEmpty());
        assertEquals("a failed read still schedules the next", 1, scheduler.pending());
    }

    @Test
    public void stopCancelsThePendingReadAndSchedulesNothingFurther() throws Exception
    {
        RepairOverdueDetector d = detector();
        d.stop();
        scheduler.runNext();
        assertEquals("the cancelled task did not read", 0, reads);
        assertEquals(0, scheduler.pending());

        // a read that is running when the detector stops finishes, is dropped, and does not reschedule
        RepairOverdueDetector e = detector();
        entered = new CountDownLatch(1);
        release = new CountDownLatch(1);
        Thread reader = new Thread(scheduler::runNext);
        reader.start();
        assertTrue(entered.await(30, TimeUnit.SECONDS));
        e.stop();
        release.countDown();
        reader.join(30_000);
        assertEquals(0, scheduler.pending());
        assertTrue(e.analyze(view).isEmpty());
    }

    @Test
    public void builtFromOptionsWithDefaultsAndRegisteredByName()
    {
        RepairOverdueDetector d = RepairOverdueDetector.fromConfig(Map.of("interval_multiple", 3, "refresh_ms", 5), history, scheduler, () -> now);
        assertEquals(3.0, d.settings().raise, 0);
        assertEquals(2.4, d.settings().clear, 1e-9);
        assertTrue(d.requires().isEmpty());
        assertEquals("nothing is scheduled before start", 0, scheduler.pending());
    }
}
