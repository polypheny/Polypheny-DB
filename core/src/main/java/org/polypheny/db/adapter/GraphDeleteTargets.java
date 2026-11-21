/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;


public record GraphDeleteTargets(Set<PolyString> nodes, Set<PolyString> edges, long rowCount) {

    public static boolean supportsType( PolyType type ) {
        return type == PolyType.NODE || type == PolyType.EDGE || type == PolyType.PATH || type == PolyType.ANY || type == PolyType.NULL;
    }


    public static boolean mayDeleteNodes( PolyType type ) {
        return type == PolyType.NODE || type == PolyType.PATH || type == PolyType.ANY;
    }


    public static GraphDeleteTargets collect( List<Map<Long, PolyValue>> rows, int targetCount ) {
        Set<PolyString> nodes = new LinkedHashSet<>();
        Set<PolyString> edges = new LinkedHashSet<>();
        long count = 0;
        for ( Map<Long, PolyValue> row : rows ) {
            boolean hasTarget = false;
            for ( int i = 0; i < targetCount; i++ ) {
                PolyValue value = row.get( (long) i );
                if ( value == null || value.isNull() ) {
                    continue;
                }
                hasTarget = true;
                if ( value instanceof PolyNode node ) {
                    nodes.add( node.id );
                } else if ( value instanceof PolyEdge edge ) {
                    edges.add( edge.id );
                } else if ( value instanceof PolyPath path ) {
                    path.getNodes().forEach( node -> nodes.add( node.id ) );
                    path.getEdges().forEach( edge -> edges.add( edge.id ) );
                } else {
                    throw new GenericRuntimeException( "Graph deletes require a node, relationship, or path binding" );
                }
            }
            if ( hasTarget ) {
                count++;
            }
        }
        return new GraphDeleteTargets( nodes, edges, count );
    }

}
