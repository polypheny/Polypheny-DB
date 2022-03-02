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

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.runtime.PolyCollections;

@Getter
public class PolyRelationship extends GraphPropertyHolder implements Comparable<PolyRelationship> {

    private final long leftId;
    private final long rightId;
    private final ImmutableList<String> labels;
    private final RelationshipDirection direction;


    public PolyRelationship( @NonNull PolyCollections.PolyDirectory properties, ImmutableList<String> labels, long leftId, long rightId, RelationshipDirection direction ) {
        this( idBuilder.getAndIncrement(), properties, labels, leftId, rightId, direction );
    }


    public PolyRelationship( long id, @NonNull PolyCollections.PolyDirectory properties, ImmutableList<String> labels, long leftId, long rightId, RelationshipDirection direction ) {
        super( id, GraphObjectType.RELATIONSHIP, properties );
        this.leftId = leftId;
        this.rightId = rightId;
        this.direction = direction;
        this.labels = labels;
    }


    @Override
    public int compareTo( PolyRelationship other ) {
        if ( leftId < other.leftId || rightId < other.rightId ) {
            return -1;
        }
        if ( leftId > other.rightId || rightId > other.rightId ) {
            return 1;
        }
        return this.getProperties().compareTo( other.getProperties() );
    }


    public enum RelationshipDirection {
        LEFT_TO_RIGHT,
        RIGHT_TO_LEFT,
        NONE;
    }

}
