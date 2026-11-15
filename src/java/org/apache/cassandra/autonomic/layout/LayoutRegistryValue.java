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

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * The layout registry as it is held in {@code ClusterMetadata.extensions} (design 2.3). Same constraints as the
 * cluster ledger value (overview 6.2): an {@code ExtensionValue}, public no-argument constructor,
 * class never renamed, the registry of an initialised value cannot be replaced, and a transformation always puts a
 * new value object because {@code build()} sets the last-modified epoch in place.
 */
public final class LayoutRegistryValue extends LayoutValueBase<LayoutRegistry>
{
    public static final String ID = "org.apache.cassandra.autonomic.layouts";
    public static final ExtensionKey<LayoutRegistry, LayoutRegistryValue> KEY = new ExtensionKey<>(ID, LayoutRegistryValue.class);

    private volatile LayoutRegistry registry;

    /** For deserialization only. */
    public LayoutRegistryValue()
    {
    }

    public LayoutRegistryValue(LayoutRegistry registry)
    {
        this.registry = Objects.requireNonNull(registry);
    }

    @Override
    public void setValue(LayoutRegistry value)
    {
        if (registry != null)
            throw new IllegalStateException("The registry of a LayoutRegistryValue is immutable; create a new value");
        registry = Objects.requireNonNull(value);
    }

    @Override
    public LayoutRegistry getValue()
    {
        return registry;
    }

    @Override
    public void serialize(DataOutputPlus out, Version version) throws IOException
    {
        LayoutRegistry.serialize(registry, out, version);
        Epoch.serializer.serialize(lastModified(), out, version);
    }

    @Override
    public void deserialize(DataInputPlus in, Version version) throws IOException
    {
        setValue(LayoutRegistry.deserialize(in, version));
        withLastModified(Epoch.serializer.deserialize(in, version));
    }

    @Override
    public long serializedSize(Version version)
    {
        return LayoutRegistry.serializedSize(registry, version) + Epoch.serializer.serializedSize(lastModified(), version);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof LayoutRegistryValue))
            return false;
        LayoutRegistryValue that = (LayoutRegistryValue) o;
        return Objects.equals(lastModified(), that.lastModified()) && Objects.equals(registry, that.registry);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastModified(), registry);
    }

    @Override
    public String toString()
    {
        return "LayoutRegistryValue{" + registry + " lastModified=" + lastModified() + '}';
    }
}
