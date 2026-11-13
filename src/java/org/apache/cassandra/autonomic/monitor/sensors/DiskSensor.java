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
package org.apache.cassandra.autonomic.monitor.sensors;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.emit;

/**
 * Space of the file stores holding the data directories. Uses {@link FileStore} directly rather than
 * {@code DiskUsageMonitor.getDiskUsage()}, which walks the data directories to size them (disk I/O) and whose periodic
 * state refresh only runs when the disk usage guardrail is enabled. The set of file stores is resolved on the first
 * collection and kept, so a collection costs one statvfs per store.
 */
public class DiskSensor implements Sensor
{
    private static final Set<SignalId> SIGNALS = Set.of(Signals.DISK_USED_FRACTION, Signals.DISK_FREE_BYTES, Signals.DISK_TOTAL_BYTES);

    private Set<FileStore> stores;

    @Override
    public String name()
    {
        return "disk";
    }

    @Override
    public Set<SignalId> signals()
    {
        return SIGNALS;
    }

    @Override
    public long minIntervalMillis()
    {
        return 30_000;
    }

    @Override
    public void collect(ObservationSink sink, long now) throws IOException
    {
        if (stores == null)
        {
            Set<FileStore> found = new LinkedHashSet<>();
            for (String dir : DatabaseDescriptor.getAllDataFileLocations())
                found.add(Files.getFileStore(Paths.get(dir)));
            stores = found;
        }
        double total = 0;
        double usable = 0;
        for (FileStore store : stores)
        {
            total += store.getTotalSpace();
            usable += store.getUsableSpace();
        }
        if (total <= 0)
            return;
        Subject node = Subject.node();
        emit(sink, Signals.DISK_USED_FRACTION, node, now, Math.max(0, Math.min(1, 1 - usable / total)));
        emit(sink, Signals.DISK_FREE_BYTES, node, now, usable);
        emit(sink, Signals.DISK_TOTAL_BYTES, node, now, total);
    }
}
