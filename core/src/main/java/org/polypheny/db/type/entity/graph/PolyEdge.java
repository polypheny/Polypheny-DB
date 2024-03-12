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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.def.SimpleSerializerDef;
import java.util.List;
import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Setter;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


@EqualsAndHashCode(callSuper = true)
@Value
public class PolyEdge extends GraphPropertyHolder {

    @JsonProperty
    public PolyString source;
    @JsonProperty
    public PolyString target;
    @JsonProperty
    public EdgeDirection direction;

    @Setter
    @NonFinal
    @Accessors(fluent = true)
    @JsonProperty
    public Pair<Integer, Integer> fromTo;


    public PolyEdge(
            @NonNull PolyDictionary properties,
            List<PolyString> labels,
            PolyString source,
            PolyString target,
            EdgeDirection direction,
            PolyString variableName ) {
        this( PolyString.of( UUID.randomUUID().toString() ), properties, labels, source, target, direction, variableName );
    }


    public PolyEdge(
            @JsonProperty("id") PolyString id,
            @JsonProperty("properties") @NonNull PolyDictionary properties,
            @JsonProperty("labels") List<PolyString> labels,
            @JsonProperty("source") PolyString source,
            @JsonProperty("target") PolyString target,
            @JsonProperty("direction") EdgeDirection direction,
            @JsonProperty("variableName") PolyString variableName ) {
        super( id, PolyType.EDGE, properties, labels, variableName );
        this.source = source;
        this.target = target;
        this.direction = direction;
    }


    public int getVariants() {
        if ( fromTo == null ) {
            return 1;
        }
        return fromTo.right - fromTo.left + 1;
    }


    public PolyEdge from( PolyString left, PolyString right ) {
        return new PolyEdge( id, properties, labels, left == null ? this.source : left, right == null ? this.target : right, direction, null );
    }


    @Override
    public void setLabels( PolyList<PolyString> labels ) {
        this.labels.clear();
        this.labels.add( labels.get( 0 ) );
    }


    private boolean isVariable() {
        return variableName != null;
    }


    public PolyEdge copyNamed( PolyString newName ) {
        if ( newName == null ) {
            // no copy needed
            return this;
        }
        return new PolyEdge( id, properties, labels, source, target, direction, newName );
    }


    public boolean isRange() {
        if ( fromTo == null ) {
            return false;
        }
        return !fromTo.left.equals( fromTo.right );
    }


    public int getMinLength() {
        if ( fromTo == null ) {
            return 1;
        }
        return fromTo.left;
    }


    public String getRangeDescriptor() {
        if ( fromTo == null ) {
            return "";
        }
        String range = "*";

        if ( fromTo.left != null && fromTo.right != null ) {
            if ( fromTo.left.equals( fromTo.right ) ) {
                return range + fromTo.right;
            }
            return range + fromTo.left + ".." + fromTo.right;
        }
        if ( fromTo.right != null ) {
            return fromTo.right.toString();
        }

        if ( fromTo.left != null ) {
            return fromTo.left.toString();
        }
        return range;
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !this.isSameType( o ) ) {
            return -1;
        }
        return this.equals( o.asEdge() ) ? 0 : -1;

    }


    @Override
    public Expression asExpression() {
        Expression expression =
                Expressions.convert_(
                        Expressions.new_(
                                PolyEdge.class,
                                id.asExpression(),
                                properties.asExpression(),
                                labels.asExpression(),
                                source.asExpression(),
                                target.asExpression(),
                                Expressions.constant( direction ),
                                getVariableName() == null ? Expressions.constant( null ) : getVariableName().asExpression() ),
                        PolyEdge.class );
        if ( fromTo != null ) {
            expression = Expressions.call( expression, "fromTo",
                    Expressions.call( BuiltInMethod.PAIR_OF.method, Expressions.constant( fromTo.left ), Expressions.constant( fromTo.right ) ) );
        }
        return expression;
    }


    @Override
    public String toJson() {
        return "{\"id\":" + id.toQuotedJson() +
                ", \"properties\":" + properties.toJson() +
                ", \"labels\":" + labels.toJson() +
                ", \"source\":" + source.toQuotedJson() +
                ", \"target\":" + target.toQuotedJson() +
                ", \"direction\":\"" + direction.name() + "\"" +
                "}";
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyLong.class );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public Object toJava() {
        return this;
    }


    public enum EdgeDirection {
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
                ", leftId=" + source +
                ", rightId=" + target +
                ", direction=" + direction +
                '}';
    }


    public static class PolyEdgeSerializerDef extends SimpleSerializerDef<PolyEdge> {

        @Override
        protected BinarySerializer<PolyEdge> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyEdge item ) {
                    out.writeUTF8( item.id.value );
                    out.writeUTF8( item.labels.serialize() );
                    out.writeBoolean( item.isVariable() );
                    if ( item.isVariable() ) {
                        out.writeUTF8Nullable( item.variableName.value );
                    }
                    out.writeUTF8( item.direction.name() );
                    out.writeUTF8( item.source.value );
                    out.writeUTF8( item.target.value );
                    out.writeUTF8( item.properties.serialize() );
                }


                @Override
                public PolyEdge decode( BinaryInput in ) throws CorruptedDataException {
                    String id = in.readUTF8();
                    PolyList<PolyString> labels = PolyValue.deserialize( in.readUTF8() ).asList();
                    boolean isVariable = in.readBoolean();
                    PolyString variableName = null;
                    if ( isVariable ) {
                        variableName = PolyString.of( in.readUTF8Nullable() );
                    }

                    EdgeDirection direction = EdgeDirection.valueOf( in.readUTF8() );
                    String source = in.readUTF8();
                    String target = in.readUTF8();
                    PolyMap<PolyValue, PolyValue> properties = PolyValue.deserialize( in.readUTF8() ).asMap();
                    return new PolyEdge( PolyString.of( id ), properties.asDictionary(), labels, PolyString.of( source ), PolyString.of( target ), direction, variableName );
                }
            };
        }

    }


}
