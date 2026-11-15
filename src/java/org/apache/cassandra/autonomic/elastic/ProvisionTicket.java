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
package org.apache.cassandra.autonomic.elastic;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The provider's handle of one requested node. It is stored in the claim of the scale-out action in the cluster
 * ledger, so it must be small: the id and all attributes together have to fit the claim state (16 entries and 4 KiB
 * shared with the action's own state, which leaves room for four attributes and about 3 KiB). A ticket that does
 * not fit is cancelled at once and the action rolled back.
 */
public final class ProvisionTicket
{
    public final String id;
    public final Map<String, String> attributes;

    public ProvisionTicket(String id, Map<String, String> attributes)
    {
        this.id = Objects.requireNonNull(id);
        this.attributes = attributes == null ? Collections.emptyMap() : Collections.unmodifiableMap(new TreeMap<>(attributes));
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof ProvisionTicket && ((ProvisionTicket) o).id.equals(id) && ((ProvisionTicket) o).attributes.equals(attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, attributes);
    }

    @Override
    public String toString()
    {
        return "ProvisionTicket{" + id + (attributes.isEmpty() ? "" : " " + attributes) + '}';
    }
}
