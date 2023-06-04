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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.SimpleSerializerDef;
import java.lang.reflect.Type;
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


    public static class PolyNodeSerializer implements JsonSerializer<PolyNode>, JsonDeserializer<PolyNode> {

        @Override
        public JsonElement serialize( PolyNode src, Type typeOfSrc, JsonSerializationContext context ) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty( "id", src.id.value );
            jsonObject.addProperty( "properties", PolyValue.GSON.toJson( src.properties ) );
            jsonObject.addProperty( "labels", PolyValue.GSON.toJson( src.labels ) );
            jsonObject.addProperty( "var", PolyValue.GSON.toJson( src.variableName ) );
            return jsonObject;
        }


        @Override
        public PolyNode deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            PolyString id = PolyString.of( jsonObject.get( "id" ).getAsString() );
            PolyDictionary props = PolyValue.GSON.fromJson( jsonObject.get( "properties" ).getAsString(), PolyDictionary.class );
            List<PolyString> labels = PolyValue.GSON.fromJson( jsonObject.get( "labels" ).getAsString(), PolyList.class );
            PolyString var = PolyValue.GSON.fromJson( jsonObject.get( "var" ).getAsString(), PolyString.class );
            return new PolyNode( id, props, labels, var );
        }

    }

}
