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
public class PolyNode extends GraphPropertyHolder implements Comparable<PolyNode> {


    public PolyNode( @NonNull PolyCollections.PolyDirectory properties, List<String> labels ) {
        this( idBuilder.getAndIncrement(), properties, labels );
    }


    public PolyNode( long id, @NonNull PolyCollections.PolyDirectory properties, List<String> labels ) {
        super( id, GraphObjectType.NODE, properties, labels );
    }


    @Override
    public int compareTo( PolyNode o ) {
        return getProperties().compareTo( o.getProperties() );
    }


    @Override
    public String toString() {
        return "PolyNode{" +
                "id=" + id +
                ", properties=" + properties +
                ", labels=" + labels +
                '}';
    }


    public boolean isBlank() {
        // MATCH (n) -> true, MATCH (n{name: 'Max'}) -> false, MATCH (n:Person) -> false
        return (properties == null || properties.isEmpty()) && (labels == null || labels.isEmpty());
    }


    static public class PolyNodeSerializer extends Serializer<PolyNode> {

        @Override
        public void write( Kryo kryo, Output output, PolyNode object ) {
            kryo.writeClassAndObject( output, object.id );
            kryo.writeClassAndObject( output, object.properties );
            kryo.writeClassAndObject( output, object.labels );
        }


        @Override
        public PolyNode read( Kryo kryo, Input input, Class<? extends PolyNode> type ) {
            long id = (long) kryo.readClassAndObject( input );
            PolyDirectory properties = (PolyDirectory) kryo.readClassAndObject( input );
            PolyList<String> labels = (PolyList<String>) kryo.readClassAndObject( input );
            return new PolyNode( id, properties, labels );
        }

    }

}
