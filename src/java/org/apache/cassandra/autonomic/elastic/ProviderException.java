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

/**
 * A provider call failed. By default the outcome is unknown: the provider may have acted on the call. A
 * {@link #rejected} exception states that it definitely did not.
 */
public class ProviderException extends Exception
{
    private final boolean rejected;

    public ProviderException(String message)
    {
        this(message, null);
    }

    public ProviderException(String message, Throwable cause)
    {
        this(message, cause, false);
    }

    private ProviderException(String message, Throwable cause, boolean rejected)
    {
        super(message, cause);
        this.rejected = rejected;
    }

    /** The provider refused the call and nothing was done or will be done for it. */
    public static ProviderException rejected(String message)
    {
        return new ProviderException(message, null, true);
    }

    public boolean isRejected()
    {
        return rejected;
    }
}
