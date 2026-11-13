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

import java.nio.ByteBuffer;

import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Encoding of a partition key in a {@code PARTITION} {@link Subject} (adaptive-schema FC6). The subject detail is the
 * raw serialized partition key as lowercase hex without prefix, so the exact bytes are always recoverable. A detector
 * emits that string as evidence {@code key_hex}; {@link #cql} gives a human readable form when the table still exists.
 */
public final class PartitionKeys
{
    private PartitionKeys()
    {
    }

    public static Subject subject(String keyspace, String table, ByteBuffer key)
    {
        return Subject.partition(keyspace, table, ByteBufferUtil.bytesToHex(key.duplicate()));
    }

    /** The key hex of a PARTITION subject. */
    public static String hex(Subject subject)
    {
        if (subject.kind != Subject.Kind.PARTITION || subject.detail == null)
            throw new IllegalArgumentException("not a partition subject: " + subject);
        return subject.detail;
    }

    /** The raw partition key bytes of a PARTITION subject. */
    public static ByteBuffer bytes(Subject subject)
    {
        return ByteBufferUtil.hexToBytes(hex(subject));
    }

    /** Human readable key in the table's partition key type, or the hex if the table is gone. */
    public static String cql(Subject subject)
    {
        try
        {
            TableMetadata metadata = Schema.instance.getTableMetadata(subject.keyspace, subject.table);
            if (metadata != null)
                return metadata.partitionKeyType.getString(bytes(subject));
        }
        catch (RuntimeException e)
        {
            // fall through to hex
        }
        return hex(subject);
    }
}
