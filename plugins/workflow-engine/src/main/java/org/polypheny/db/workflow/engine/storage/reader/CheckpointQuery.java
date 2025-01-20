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

package org.polypheny.db.workflow.engine.storage.reader;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.engine.storage.QueryUtils;


@Builder
public class CheckpointQuery {

    public static final String ENTITY_L = "{?";
    public static final String ENTITY_R = "?}";

    @Getter
    private final String query;

    @Getter
    private final String queryLanguage;
    @Singular
    private final Map<Integer, Pair<@NotNull AlgDataType, PolyValue>> parameters;


    public String getQueryWithPlaceholdersReplaced( LogicalEntity entity ) {
        return query.replace( ENTITY(), QueryUtils.quotedIdentifier( entity ) );
    }


    public String getQueryWithPlaceholdersReplaced( List<LogicalEntity> entities ) {
        String replaced = query;
        for ( int i = 0; i < entities.size(); i++ ) {
            replaced = replaced.replace( ENTITY( i ), QueryUtils.quotedIdentifier( entities.get( i ) ) );
        }
        return replaced;
    }


    /**
     * Get the input-entity placeholder to be used to construct the query.
     * Example Query String: {@code String query = "SELECT * FROM " + CheckpointQuery.ENTITY() }.
     * In the case that there are multiple entities (query goes over several checkpoints), the placeholder for the first input is returned.
     *
     * @return A string to be used as an entity placeholder
     */
    public static String ENTITY() {
        return ENTITY_L + 0 + ENTITY_R;
    }


    /**
     * Get the entity placeholder to be used to construct the query for the specified input index.
     * Example Query String: {@code String query = "SELECT * FROM " + CheckpointQuery.ENTITY(0) + ", " + CheckpointQuery.ENTITY(1) }
     *
     * @param inputIdx the index of the input entity whose placeholder will be returned
     * @return A string to be used as a target entity placeholder
     */
    public static String ENTITY( int inputIdx ) {
        return ENTITY_L + inputIdx + ENTITY_R;
    }


    public Map<Long, @NotNull AlgDataType> getParameterTypes() {
        Map<Long, @NotNull AlgDataType> types = new HashMap<>();
        for ( Map.Entry<Integer, Pair<@NotNull AlgDataType, PolyValue>> entry : parameters.entrySet() ) {
            Integer key = entry.getKey();
            Pair<@NotNull AlgDataType, PolyValue> value = entry.getValue();
            types.put( key.longValue(), value.getLeft() );
        }
        return types;
    }


    public Map<Long, PolyValue> getParameterValues() {
        Map<Long, PolyValue> values = new HashMap<>();
        for ( Map.Entry<Integer, Pair<@NotNull AlgDataType, PolyValue>> entry : parameters.entrySet() ) {
            Integer key = entry.getKey();
            Pair<@NotNull AlgDataType, PolyValue> value = entry.getValue();
            values.put( key.longValue(), value.getRight() );
        }
        return values;
    }


    public boolean hasParams() {
        return !parameters.isEmpty();
    }

}
