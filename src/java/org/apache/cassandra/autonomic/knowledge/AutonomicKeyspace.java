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

import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.schema.Views;

import static org.apache.cassandra.schema.SchemaConstants.AUTONOMIC_KEYSPACE_NAME;

/**
 * The node-local {@code system_autonomic} keyspace (overview 5.1). It is always defined, whether or not the
 * autonomic layer is enabled (overview 5.2).
 */
public final class AutonomicKeyspace
{
    private AutonomicKeyspace()
    {
    }

    public static final String DIAGNOSES = "diagnoses";
    public static final String ACTIONS = "actions";
    public static final String ACTION_EVENTS = "action_events";
    public static final String LOCAL_STATE = "local_state";

    /** Default TTL of the history tables, 30 days; writes use the configured {@code history_ttl} explicitly. */
    public static final int DEFAULT_HISTORY_TTL_SECONDS = 2592000;

    // Deviation from overview 5.1: diagnoses.id is uuid, not timeuuid, because Diagnosis ids are plain UUIDs.
    private static final TableMetadata Diagnoses =
        parse(DIAGNOSES, "autonomic diagnoses",
              "CREATE TABLE %s ("
              + "day date,"
              + "id uuid,"
              + "type text,"
              + "subject text,"
              + "severity text,"
              + "confidence double,"
              + "state text,"
              + "first_seen timestamp,"
              + "last_seen timestamp,"
              + "summary text,"
              + "evidence map<text, text>,"
              + "root_cause uuid,"
              + "PRIMARY KEY ((day), id)"
              + ") WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = " + DEFAULT_HISTORY_TTL_SECONDS);

    private static final TableMetadata Actions =
        parse(ACTIONS, "autonomic actions",
              "CREATE TABLE %s ("
              + "day date,"
              + "id timeuuid,"
              + "diagnosis_id uuid,"
              + "policy text,"
              + "actuator text,"
              + "scope text,"
              + "target text,"
              + "params map<text, text>,"
              + "dry_run boolean,"
              + "state text,"
              + "state_reason text,"
              + "undo map<text, text>,"
              + "baseline map<text, double>,"
              + "result map<text, double>,"
              + "verdict text,"
              + "proposed_at timestamp,"
              + "updated_at timestamp,"
              + "PRIMARY KEY ((day), id)"
              + ") WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = " + DEFAULT_HISTORY_TTL_SECONDS);

    private static final TableMetadata ActionEvents =
        parse(ACTION_EVENTS, "autonomic action audit trail",
              "CREATE TABLE %s ("
              + "action_id timeuuid,"
              + "at timeuuid,"
              + "from_state text,"
              + "to_state text,"
              + "detail text,"
              + "PRIMARY KEY ((action_id), at)"
              + ") WITH default_time_to_live = " + DEFAULT_HISTORY_TTL_SECONDS);

    private static final TableMetadata LocalState =
        parse(LOCAL_STATE, "autonomic durable flags",
              "CREATE TABLE %s ("
              + "key text PRIMARY KEY,"
              + "value text,"
              + "updated_at timestamp)");

    private static TableMetadata parse(String name, String description, String cql)
    {
        return CreateTableStatement.parse(String.format(cql, name), AUTONOMIC_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(AUTONOMIC_KEYSPACE_NAME, name))
                                   .comment(description)
                                   .build();
    }

    public static Tables tables()
    {
        return Tables.of(Diagnoses, Actions, ActionEvents, LocalState);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(AUTONOMIC_KEYSPACE_NAME, KeyspaceParams.local(), tables(), Views.none(), Types.none(), UserFunctions.none());
    }
}
