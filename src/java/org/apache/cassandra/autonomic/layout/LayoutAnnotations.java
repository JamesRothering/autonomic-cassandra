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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.autonomic.monitor.Subject;

/**
 * Why the policy did not split a hot partition (design 7.2: {@code layout=not_applicable(<reason>)}). The policy
 * writes, the {@code hot_partition} detector copies the text into the evidence of the diagnosis it raises on its next
 * evaluation, so the operator sees it in the diagnosis. In memory and bounded; a stale entry is only ever shown on a
 * diagnosis that is still open.
 */
public final class LayoutAnnotations
{
    public static final String KEY = "layout";
    private static final int MAX = 1000;

    private static final Map<Subject, String> NOTES = new ConcurrentHashMap<>();

    private LayoutAnnotations()
    {
    }

    public static String notApplicable(String reason)
    {
        return "not_applicable(" + reason + ')';
    }

    public static void set(Subject partition, String note)
    {
        if (NOTES.size() >= MAX && !NOTES.containsKey(partition))
            NOTES.clear();
        NOTES.put(partition, note);
    }

    /** The note of the partition, or null. */
    public static String get(Subject partition)
    {
        return NOTES.get(partition);
    }

    public static void clear(Subject partition)
    {
        NOTES.remove(partition);
    }

    public static void clearAll()
    {
        NOTES.clear();
    }
}
