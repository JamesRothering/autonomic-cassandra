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
package org.apache.cassandra.autonomic.knowledge;

import java.util.Arrays;

/** Copy of a time window of one series, oldest first. */
public final class Series
{
    public static final Series EMPTY = new Series(new long[0], new double[0]);

    public final long[] times;
    public final double[] values;

    public Series(long[] times, double[] values)
    {
        if (times.length != values.length)
            throw new IllegalArgumentException("length mismatch");
        this.times = times;
        this.values = values;
    }

    public int size()
    {
        return times.length;
    }

    public boolean isEmpty()
    {
        return times.length == 0;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof Series && Arrays.equals(times, ((Series) o).times) && Arrays.equals(values, ((Series) o).values);
    }

    @Override
    public int hashCode()
    {
        return 31 * Arrays.hashCode(times) + Arrays.hashCode(values);
    }

    @Override
    public String toString()
    {
        return "Series" + Arrays.toString(values);
    }
}
