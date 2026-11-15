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
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.utils.JVMStabilityInspector;

/** Builds the provider named by {@code autonomic.scale_out.provider}. Never throws: any failure yields {@link NoopProvider}. */
public final class ScaleOutProviders
{
    private static final Logger logger = LoggerFactory.getLogger(ScaleOutProviders.class);

    private ScaleOutProviders()
    {
    }

    /** Errors go to the stability inspector, which must not turn a failed provider into a failed start. */
    static void inspect(Throwable t)
    {
        if (!(t instanceof Error))
            return;
        try
        {
            JVMStabilityInspector.inspectThrowable(t);
        }
        catch (Throwable ignored)
        {
            // the inspector may rethrow
        }
    }

    public static ScaleOutProvider load(AutonomicConfig.ScaleOutConfig config)
    {
        String className = config == null || config.provider == null ? "" : config.provider.trim();
        if (className.isEmpty() || className.equals(NoopProvider.class.getName()))
            return new NoopProvider();
        try
        {
            Class<?> cls = Class.forName(className);
            if (!ScaleOutProvider.class.isAssignableFrom(cls))
                throw new IllegalArgumentException(className + " does not implement " + ScaleOutProvider.class.getName());
            Map<String, String> parameters = config.parameters == null ? Collections.emptyMap()
                                                                       : Collections.unmodifiableMap(new LinkedHashMap<>(config.parameters));
            ScaleOutProvider provider;
            try
            {
                provider = (ScaleOutProvider) cls.getConstructor(Map.class).newInstance(parameters);
            }
            catch (NoSuchMethodException e)
            {
                provider = (ScaleOutProvider) cls.getConstructor().newInstance();
            }
            // read here so that a provider that cannot even describe itself is not used
            if (provider.name() == null || provider.capabilities() == null)
                throw new IllegalArgumentException(className + " returned no name or capabilities");
            return provider;
        }
        catch (Throwable t)
        {
            inspect(t);
            logger.error("Autonomic scale-out provider {} could not be created; scale-out is unavailable (using {})",
                         className, NoopProvider.class.getSimpleName(), t);
            return new NoopProvider();
        }
    }
}
