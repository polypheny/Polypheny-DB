/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.workflow.dag.settings;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import lombok.Value;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;

@Value
public class QueryValue implements SettingValue {

    String query;
    String queryLanguage;


    public static QueryValue of( JsonNode node ) {
        JsonNode queryNode = node.get( "query" );
        JsonNode queryLanguage = node.get( "queryLanguage" );
        try {
            return new QueryValue( Objects.requireNonNull( queryNode.textValue() ), Objects.requireNonNull( queryLanguage.textValue() ) );
        } catch ( NullPointerException e ) {
            throw new IllegalArgumentException( node + " does not represent a query" );
        }
    }


    @Override
    public JsonNode toJson( ObjectMapper mapper ) {
        return mapper.createObjectNode()
                .put( "query", query )
                .put( "queryLanguage", queryLanguage );
    }


    public CheckpointQuery getCheckpointQuery() {
        return CheckpointQuery.builder()
                .query( query )
                .queryLanguage( queryLanguage )
                .build();
    }


    public QueryLanguage getLanguage() {
        return QueryLanguage.from( queryLanguage );
    }

}
