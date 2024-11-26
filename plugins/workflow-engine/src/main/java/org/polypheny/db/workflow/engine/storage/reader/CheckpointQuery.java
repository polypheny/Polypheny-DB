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
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;


@Builder
public class CheckpointQuery {

    public static final String ENTITY = "{???}"; // entity name placeholder

    @Getter
    private final String query;

    @Getter
    private final String queryLanguage;
    @Singular
    private final Map<Integer, Pair<@NotNull AlgDataType, PolyValue>> parameters;


    public String getQueryWithPlaceholderReplaced( LogicalEntity entity ) {
        return query.replace( ENTITY, getEntityName( entity ) );
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


    private String getEntityName( LogicalEntity entity ) {
        if ( entity instanceof LogicalTable table ) {
            return "\"" + table.getNamespaceName() + "\".\"" + table.getName() + "\"";
        } else if ( entity instanceof LogicalCollection collection ) {
            return "\"" + collection.getNamespaceName() + "\".\"" + collection.getName() + "\"";
        } else if ( entity instanceof LogicalGraph graph ) {
            return "\"" + graph.getNamespaceName() + "\"";
        }
        throw new IllegalArgumentException( "Encountered unknown entity type" );
    }

}
