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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.runtime.PolyCollections;
import org.polypheny.db.runtime.PolyCollections.PolyDirectory;
import org.polypheny.db.runtime.PolyCollections.PolyList;

@Getter
public class PolyEdge extends GraphPropertyHolder implements Comparable<PolyEdge> {

    public final long leftId;
    public final long rightId;
    public final RelationshipDirection direction;


    public PolyEdge( @NonNull PolyCollections.PolyDirectory properties, List<String> labels, long leftId, long rightId, RelationshipDirection direction ) {
        this( idBuilder.getAndIncrement(), properties, labels, leftId, rightId, direction );
    }


    public PolyEdge( long id, @NonNull PolyCollections.PolyDirectory properties, List<String> labels, long leftId, long rightId, RelationshipDirection direction ) {
        super( id, GraphObjectType.RELATIONSHIP, properties, labels );
        this.leftId = leftId;
        this.rightId = rightId;
        this.direction = direction;
    }


    @Override
    public int compareTo( PolyEdge other ) {
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
        NONE
    }


    @Override
    public String toString() {
        return "PolyEdge{" +
                "id=" + id +
                ", properties=" + properties +
                ", labels=" + labels +
                ", leftId=" + leftId +
                ", rightId=" + rightId +
                ", direction=" + direction +
                '}';
    }


    public static class PolyEdgeSerializer extends Serializer<PolyEdge> {

        @Override
        public void write( Kryo kryo, Output output, PolyEdge object ) {
            kryo.writeClassAndObject( output, object.id );
            kryo.writeClassAndObject( output, object.properties );
            kryo.writeClassAndObject( output, object.labels );
            kryo.writeClassAndObject( output, object.leftId );
            kryo.writeClassAndObject( output, object.rightId );
            kryo.writeClassAndObject( output, object.direction );
        }


        @Override
        public PolyEdge read( Kryo kryo, Input input, Class<? extends PolyEdge> type ) {
            long id = (long) kryo.readClassAndObject( input );
            PolyDirectory properties = (PolyDirectory) kryo.readClassAndObject( input );
            PolyList<String> labels = (PolyList<String>) kryo.readClassAndObject( input );
            long leftId = (long) kryo.readClassAndObject( input );
            long rightId = (long) kryo.readClassAndObject( input );
            RelationshipDirection direction = (RelationshipDirection) kryo.readClassAndObject( input );
            return new PolyEdge( id, properties, labels, leftId, rightId, direction );
        }


    }
}
