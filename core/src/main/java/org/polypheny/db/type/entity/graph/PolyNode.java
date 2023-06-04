/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.type.entity.graph;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


@Getter
public class PolyNode extends GraphPropertyHolder {

    @Getter
    @Setter
    @Accessors(fluent = true)
    private boolean isVariable = false;


    public PolyNode( @NonNull PolyDictionary properties, List<PolyString> labels, PolyString variableName ) {
        this( PolyString.of( UUID.randomUUID().toString() ), properties, labels, variableName );
    }


    public PolyNode( PolyString id, @NonNull PolyDictionary properties, List<PolyString> labels, PolyString variableName ) {
        super( id, PolyType.NODE, properties, labels, variableName );
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


    @Override
    public Expression asExpression() {
        return Expressions.call( Expressions.convert_(
                Expressions.new_(
                        PolyNode.class,
                        id.asExpression(),
                        properties.asExpression(),
                        labels.asExpression(),
                        getVariableName() == null ? Expressions.constant( null ) : getVariableName().asExpression() ),
                PolyNode.class
        ), "isVariable", Expressions.constant( true ) );
    }


    @Override
    public void setLabels( PolyList<PolyString> labels ) {
        this.labels.addAll( labels );
    }


    public PolyNode copyNamed( PolyString variableName ) {
        if ( variableName == null ) {
            // no copy needed
            return this;
        }
        return new PolyNode( id, properties, labels, variableName );

    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isNode() ) {
            return -1;
        }

        return id.compareTo( o.asNode().id );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyNode.class );
    }


    public static class PolyNodeSerializerDef extends SimpleSerializerDef<PolyNode> {

        @Override
        protected BinarySerializer<PolyNode> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyNode item ) {

                }


                @Override
                public PolyNode decode( BinaryInput in ) throws CorruptedDataException {
                    return null;
                }
            };
        }

    }


    public static class PolyNodeTypeAdapter extends TypeAdapter<PolyNode> {

        @Override
        public void write( JsonWriter out, PolyNode value ) throws IOException {
            out.beginObject();
            out.name( "id" );
            out.value( value.id.value );
            out.name( "props" );
            out.value( PolyValue.GSON.toJson( value.properties ) );
            out.name( "labels" );
            out.value( PolyValue.GSON.toJson( value.labels ) );
            out.name( "var" );
            out.value( PolyValue.GSON.toJson( value.variableName ) );
            out.endObject();
        }


        @Override
        public PolyNode read( JsonReader in ) throws IOException {
            in.beginObject();
            in.nextName();
            PolyString name = PolyString.of( in.nextString() );
            in.nextName();
            PolyDictionary props = PolyValue.GSON.fromJson( in.nextString(), PolyDictionary.class );
            in.nextName();
            List<PolyString> labels = PolyValue.GSON.fromJson( in.nextString(), PolyList.class );
            in.nextName();
            PolyString var = PolyValue.GSON.fromJson( in.nextString(), PolyString.class );
            in.endObject();
            return new PolyNode( name, props, labels, var );
        }

    }

}
