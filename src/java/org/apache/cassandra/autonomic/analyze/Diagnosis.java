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
package org.apache.cassandra.autonomic.analyze;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.autonomic.monitor.Subject;

/** A condition currently believed true. The id is stable while the condition persists. */
public final class Diagnosis
{
    public final UUID id;
    /** For example {@code hot_partition}; self-diagnoses use the {@code autonomic.} prefix. */
    public final String type;
    public final Subject subject;
    public final Severity severity;
    /** 0..1 */
    public final double confidence;
    public final long firstSeenMillis;
    public final long lastSeenMillis;
    public final String summary;
    /** Signal name to formatted value and threshold. */
    public final Map<String, String> evidence;
    /** Nullable; set by the correlator. */
    public final UUID rootCause;

    public Diagnosis(UUID id, String type, Subject subject, Severity severity, double confidence,
                     long firstSeenMillis, long lastSeenMillis, String summary, Map<String, String> evidence, UUID rootCause)
    {
        this.id = Objects.requireNonNull(id);
        this.type = Objects.requireNonNull(type);
        this.subject = Objects.requireNonNull(subject);
        this.severity = Objects.requireNonNull(severity);
        this.confidence = confidence;
        this.firstSeenMillis = firstSeenMillis;
        this.lastSeenMillis = lastSeenMillis;
        this.summary = summary;
        this.evidence = Collections.unmodifiableMap(new LinkedHashMap<>(evidence));
        this.rootCause = rootCause;
    }

    public Diagnosis withRootCause(UUID rootCause)
    {
        return new Diagnosis(id, type, subject, severity, confidence, firstSeenMillis, lastSeenMillis, summary, evidence, rootCause);
    }

    public Diagnosis withLastSeen(long lastSeenMillis)
    {
        return new Diagnosis(id, type, subject, severity, confidence, firstSeenMillis, lastSeenMillis, summary, evidence, rootCause);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Diagnosis))
            return false;
        Diagnosis d = (Diagnosis) o;
        return id.equals(d.id) && type.equals(d.type) && subject.equals(d.subject) && severity == d.severity
               && Double.compare(confidence, d.confidence) == 0 && firstSeenMillis == d.firstSeenMillis
               && lastSeenMillis == d.lastSeenMillis && Objects.equals(summary, d.summary)
               && evidence.equals(d.evidence) && Objects.equals(rootCause, d.rootCause);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, type, subject, severity, confidence, firstSeenMillis, lastSeenMillis, summary, evidence, rootCause);
    }

    @Override
    public String toString()
    {
        return "Diagnosis{" + type + " " + subject + " " + severity + " id=" + id + " " + summary + '}';
    }
}
