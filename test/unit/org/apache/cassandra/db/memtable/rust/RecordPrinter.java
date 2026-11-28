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

package org.apache.cassandra.db.memtable.rust;

import java.nio.ByteBuffer;

/** Renders partition records as text, for failure messages. */
final class RecordPrinter
{
    private RecordPrinter()
    {
    }

    static String describe(ByteBuffer records)
    {
        ByteBuffer in = records.duplicate();
        StringBuilder sb = new StringBuilder();
        try
        {
            while (in.hasRemaining())
            {
                sb.append("partition ").append(hex(in)).append(" del=").append(deletion(in));
                int rows = in.getInt();
                for (int r = 0; r < rows; r++)
                {
                    sb.append("\n    row ").append(hex(in));
                    sb.append(" live=(").append(in.getLong()).append(',').append(in.getInt()).append(',').append(in.getLong()).append(')');
                    sb.append(" del=").append(deletion(in)).append(in.get() == 1 ? " shadowable" : "");
                    int cells = in.getInt();
                    for (int c = 0; c < cells; c++)
                        sb.append("\n        cell col=").append(in.getInt()).append(" ts=").append(in.getLong())
                          .append(" ttl=").append(in.getInt()).append(" ldt=").append(in.getLong()).append(" v=").append(hex(in));
                }
                sb.append('\n');
            }
        }
        catch (RuntimeException e)
        {
            sb.append(" <malformed: ").append(e).append('>');
        }
        return sb.toString();
    }

    private static String deletion(ByteBuffer in)
    {
        long markedForDeleteAt = in.getLong();
        long localDeletionTime = in.getLong();
        return markedForDeleteAt == Long.MIN_VALUE ? "live" : "(" + markedForDeleteAt + ',' + localDeletionTime + ')';
    }

    private static String hex(ByteBuffer in)
    {
        int n = in.getInt();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++)
            sb.append(String.format("%02x", in.get() & 0xff));
        return sb.length() == 0 ? "[]" : sb.toString();
    }
}
