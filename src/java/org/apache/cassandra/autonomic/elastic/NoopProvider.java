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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default provider: it cannot add nodes. No scale-out is proposed with it; a request that reaches it anyway
 * gets a ticket whose status is FAILED("no provider configured"), so the action ends as a clean rollback.
 */
public class NoopProvider implements ScaleOutProvider
{
    private static final Logger logger = LoggerFactory.getLogger(NoopProvider.class);

    public static final String NAME = "noop";
    public static final String REASON = "no provider configured";

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Capabilities capabilities()
    {
        return Capabilities.NONE;
    }

    @Override
    public ProvisionTicket requestNode(ScaleRequest request)
    {
        logger.info("Autonomic scale-out request {} ignored: {}", request, REASON);
        return new ProvisionTicket("noop-" + request.actionId, Collections.emptyMap());
    }

    @Override
    public ProvisionStatus status(ProvisionTicket ticket)
    {
        return ProvisionStatus.failed(REASON);
    }

    @Override
    public void cancel(ProvisionTicket ticket)
    {
    }

    @Override
    public void releaseNode(String hostId, String actionId)
    {
    }
}
