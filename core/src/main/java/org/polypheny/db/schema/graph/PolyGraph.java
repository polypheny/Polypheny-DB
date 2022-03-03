/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.schema.graph;

import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.runtime.PolyCollections.PolyMap;

@Getter
public class PolyGraph extends GraphObject implements Comparable<PolyGraph> {

    private final PolyMap<Long, PolyNode> nodes;
    private final PolyMap<Long, PolyRelationship> relationships;


    public PolyGraph( @NonNull PolyMap<Long, PolyNode> nodes, @NonNull PolyMap<Long, PolyRelationship> relationships ) {
        this( idBuilder.getAndIncrement(), nodes, relationships );
    }


    public PolyGraph( long id, @NonNull PolyMap<Long, PolyNode> nodes, @NonNull PolyMap<Long, PolyRelationship> relationships ) {
        super( id, GraphObjectType.GRAPH );
        this.nodes = nodes;
        this.relationships = relationships;
    }


    @Override
    public int compareTo( PolyGraph o ) {

        if ( this.nodes.size() > o.nodes.size() ) {
            return 1;
        }
        if ( this.nodes.size() < o.nodes.size() ) {
            return -1;
        }

        if ( this.nodes.keySet().equals( o.nodes.keySet() ) && this.relationships.values().equals( o.relationships.values() ) ) {
            return 0;
        }
        return -1;
    }

}
