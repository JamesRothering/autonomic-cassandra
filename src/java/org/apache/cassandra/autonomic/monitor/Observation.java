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
package org.apache.cassandra.autonomic.monitor;

import java.util.Objects;

public final class Observation
{
    public final SignalId signal;
    public final Subject subject;
    public final long atMillis;
    public final double value;

    public Observation(SignalId signal, Subject subject, long atMillis, double value)
    {
        this.signal = Objects.requireNonNull(signal);
        this.subject = Objects.requireNonNull(subject);
        this.atMillis = atMillis;
        this.value = value;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Observation))
            return false;
        Observation that = (Observation) o;
        return atMillis == that.atMillis && Double.compare(value, that.value) == 0
               && signal.equals(that.signal) && subject.equals(that.subject);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signal, subject, atMillis, value);
    }

    @Override
    public String toString()
    {
        return signal + "@" + subject + "[" + atMillis + "]=" + value;
    }
}
