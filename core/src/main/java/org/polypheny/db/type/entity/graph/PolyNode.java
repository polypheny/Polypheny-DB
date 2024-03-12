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

package org.polypheny.db.type.entity.graph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.def.SimpleSerializerDef;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.relational.PolyMap;


@Setter
@Getter
public class PolyNode extends GraphPropertyHolder {

    @Accessors(fluent = true)
    private boolean isVariable = false;


    public PolyNode( @NonNull PolyDictionary properties, List<PolyString> labels, PolyString variableName ) {
        this( PolyString.of( UUID.randomUUID().toString() ), properties, labels, variableName );
    }


    @JsonCreator
    public PolyNode( @JsonProperty("id") PolyString id, @JsonProperty("properties") @NonNull PolyDictionary properties, @JsonProperty("labels") List<PolyString> labels, @JsonProperty("variableName") PolyString variableName ) {
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


    @Override
    public String toJson() {
        return "{\"id\":" + id.toQuotedJson() + ", \"properties\":" + properties.toJson() + ", \"labels\":" + labels.toJson() + "}";
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


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public Object toJava() {
        return this;
    }


    public static class PolyNodeSerializerDef extends SimpleSerializerDef<PolyNode> {

        @Override
        protected BinarySerializer<PolyNode> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyNode item ) {
                    out.writeUTF8( item.id.value );
                    out.writeUTF8( item.labels.serialize() );
                    out.writeBoolean( item.isVariable );
                    if ( item.isVariable ) {
                        out.writeUTF8Nullable( item.variableName.value );
                    }
                    out.writeUTF8( item.properties.serialize() );
                }


                @Override
                public PolyNode decode( BinaryInput in ) throws CorruptedDataException {
                    String id = in.readUTF8();
                    PolyList<PolyString> labels = PolyValue.deserialize( in.readUTF8() ).asList();
                    boolean isVariable = in.readBoolean();
                    PolyString variableName = null;
                    if ( isVariable ) {
                        variableName = PolyString.of( in.readUTF8Nullable() );
                    }
                    PolyMap<PolyValue, PolyValue> properties = PolyValue.deserialize( in.readUTF8() ).asMap();
                    return new PolyNode( PolyString.of( id ), properties.asDictionary(), labels, variableName );
                }
            };
        }

    }


}
