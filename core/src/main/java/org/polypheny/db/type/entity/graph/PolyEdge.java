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
import java.lang.reflect.Type;
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
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


@EqualsAndHashCode(callSuper = true)
@Value
public class PolyEdge extends GraphPropertyHolder {

    public PolyString source;
    public PolyString target;
    public EdgeDirection direction;

    @Setter
    @NonFinal
    @Accessors(fluent = true)
    public Pair<Integer, Integer> fromTo;


    public PolyEdge( @NonNull PolyDictionary properties, List<PolyString> labels, PolyString source, PolyString target, EdgeDirection direction, PolyString variableName ) {
        this( PolyString.of( UUID.randomUUID().toString() ), properties, labels, source, target, direction, variableName );
    }


    public PolyEdge( PolyString id, @NonNull PolyDictionary properties, List<PolyString> labels, PolyString source, PolyString target, EdgeDirection direction, PolyString variableName ) {
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
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyLong.class );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
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


    public static class PolyEdgeSerializer implements JsonSerializer<PolyEdge>, JsonDeserializer<PolyEdge> {

        @Override
        public PolyEdge deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            PolyString id = PolyString.of( jsonObject.get( "id" ).getAsString() );
            PolyDictionary props = PolyValue.GSON.fromJson( jsonObject.get( "properties" ).getAsString(), PolyDictionary.class );
            List<PolyString> labels = PolyValue.GSON.fromJson( jsonObject.get( "labels" ).getAsString(), PolyList.class );
            PolyString source = PolyString.of( jsonObject.get( "source" ).getAsString() );
            PolyString target = PolyString.of( jsonObject.get( "target" ).getAsString() );
            EdgeDirection dir = EdgeDirection.valueOf( jsonObject.get( "direction" ).getAsString() );
            PolyString var = PolyValue.GSON.fromJson( jsonObject.get( "var" ).getAsString(), PolyString.class );
            return new PolyEdge( id, props, labels, source, target, dir, var );
        }


        @Override
        public JsonElement serialize( PolyEdge src, Type typeOfSrc, JsonSerializationContext context ) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty( "id", src.id.value );
            jsonObject.addProperty( "properties", PolyValue.GSON.toJson( src.properties ) );
            jsonObject.addProperty( "labels", PolyValue.GSON.toJson( src.labels ) );
            jsonObject.addProperty( "source", src.source.value );
            jsonObject.addProperty( "target", src.target.value );
            jsonObject.addProperty( "direction", src.direction.name() );
            jsonObject.addProperty( "var", PolyValue.GSON.toJson( src.variableName ) );
            return jsonObject;
        }

    }


}
