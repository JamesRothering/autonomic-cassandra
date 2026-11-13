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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class ActionOutcome
{
    public final ActionState finalState;
    /** Nullable if never verified. */
    public final Verdict verdict;
    public final String detail;
    public final Map<String, Double> baseline;
    public final Map<String, Double> result;
    public final long startedMillis;
    public final long endedMillis;

    public ActionOutcome(ActionState finalState, Verdict verdict, String detail, Map<String, Double> baseline,
                         Map<String, Double> result, long startedMillis, long endedMillis)
    {
        this.finalState = Objects.requireNonNull(finalState);
        this.verdict = verdict;
        this.detail = detail;
        this.baseline = Collections.unmodifiableMap(new LinkedHashMap<>(baseline));
        this.result = Collections.unmodifiableMap(new LinkedHashMap<>(result));
        this.startedMillis = startedMillis;
        this.endedMillis = endedMillis;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof ActionOutcome))
            return false;
        ActionOutcome a = (ActionOutcome) o;
        return finalState == a.finalState && verdict == a.verdict && Objects.equals(detail, a.detail)
               && baseline.equals(a.baseline) && result.equals(a.result)
               && startedMillis == a.startedMillis && endedMillis == a.endedMillis;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(finalState, verdict, detail, baseline, result, startedMillis, endedMillis);
    }

    @Override
    public String toString()
    {
        return "ActionOutcome{" + finalState + " " + verdict + " " + detail + '}';
    }
}
