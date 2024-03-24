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

package org.polypheny.db.type.entity;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonToken;
import io.activej.serializer.BinaryInput;
import io.activej.serializer.BinaryOutput;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.CompatibilityLevel;
import io.activej.serializer.CorruptedDataException;
import io.activej.serializer.def.SimpleSerializerDef;
import java.math.BigDecimal;
import java.util.Map;
import lombok.NonNull;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.type.entity.relational.PolyMap.MapType;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;

public class PolyNull extends PolyValue {

    @JsonValue
    public static PolyNull NULL = new PolyNull();


    /**
     * Creates a PolyNull, which is a PolyValue wrapper fo {@code null},
     * this value accepts all {@code PolyValue::is[...]} methods.
     */
    public PolyNull() {
        super( PolyType.NULL );
    }


    @Override
    public @Nullable String toJson() {
        return JsonToken.VALUE_NULL.asString();
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        return o.isNull() ? 0 : -1;
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return 1L;
    }


    @Override
    public boolean isBoolean() {
        return true;
    }


    @Override
    public @NotNull PolyBoolean asBoolean() {
        return PolyBoolean.of( null );
    }


    @Override
    public boolean isInteger() {
        return true;
    }


    @Override
    public @NotNull PolyInteger asInteger() {
        return PolyInteger.of( null );
    }


    @Override
    public boolean isDocument() {
        return false;
    }


    @Override
    public @NotNull PolyDocument asDocument() {
        return PolyDocument.ofDocument( null );
    }


    @Override
    public boolean isList() {
        return true;
    }


    @Override
    public @NotNull <T extends PolyValue> PolyList<T> asList() {
        return PolyList.of();
    }


    @Override
    public boolean isString() {
        return true;
    }


    @Override
    public @NotNull PolyString asString() {
        return PolyString.of( null );
    }


    @Override
    public boolean isBinary() {
        return true;
    }


    @Override
    public @NotNull PolyBinary asBinary() {
        return PolyBinary.of( (ByteString) null );
    }


    @Override
    public boolean isBigDecimal() {
        return true;
    }


    @Override
    public @NotNull PolyBigDecimal asBigDecimal() {
        return PolyBigDecimal.of( (BigDecimal) null );
    }


    @Override
    public boolean isFloat() {
        return true;
    }


    @Override
    public @NotNull PolyFloat asFloat() {
        return PolyFloat.of( null );
    }


    @Override
    public boolean isDouble() {
        return true;
    }


    @Override
    public @NotNull PolyDouble asDouble() {
        return PolyDouble.of( null );
    }


    @Override
    public boolean isLong() {
        return true;
    }


    @Override
    public @NotNull PolyLong asLong() {
        return PolyLong.of( (Long) null );
    }


    @Override
    public boolean isTemporal() {
        return true;
    }


    @Override
    public PolyTemporal asTemporal() {
        return PolyDate.ofNullable( (Long) null );
    }


    @Override
    public boolean isDate() {
        return true;
    }


    @Override
    public @NonNull PolyDate asDate() {
        return PolyDate.ofNullable( (Long) null );
    }


    @Override
    public boolean isTime() {
        return true;
    }


    @Override
    public @NonNull PolyTime asTime() {
        return PolyTime.ofNullable( (Long) null );
    }


    @Override
    public boolean isTimestamp() {
        return true;
    }


    @Override
    public @NonNull PolyTimestamp asTimestamp() {
        return PolyTimestamp.of( (Long) null );
    }


    @Override
    public boolean isMap() {
        return true;
    }


    @Override
    public @NonNull PolyMap<PolyValue, PolyValue> asMap() {
        return PolyMap.of( null, MapType.MAP );
    }


    @Override
    public boolean isEdge() {
        return true;
    }


    @Override
    public @NonNull PolyEdge asEdge() {
        return new PolyEdge( new PolyDictionary(), null, null, null, null, null );
    }


    @Override
    public boolean isNode() {
        return true;
    }


    @Override
    public @NonNull PolyNode asNode() {
        return new PolyNode( new PolyDictionary(), null, null );
    }


    @Override
    public boolean isPath() {
        return true;
    }


    @Override
    public @NonNull PolyPath asPath() {
        return new PolyPath( null, null, null, null, null );
    }


    @Override
    public boolean isGraph() {
        return true;
    }


    @Override
    public @NonNull PolyGraph asGraph() {
        return new PolyGraph( null, PolyMap.of( Map.of() ), PolyMap.of( Map.of() ) );
    }


    @Override
    public boolean isNumber() {
        return true;
    }


    @Override
    public @NotNull PolyNumber asNumber() {
        return PolyInteger.of( null );
    }


    @Override
    public boolean isInterval() {
        return true;
    }


    @Override
    public @NotNull PolyInterval asInterval() {
        return PolyInterval.of( 0L, (Long) null );
    }


    @Override
    public boolean isSymbol() {
        return true;
    }


    @Override
    public PolySymbol asSymbol() {
        return PolySymbol.of( null );
    }


    @Override
    public boolean isBlob() {
        return true;
    }


    @Override
    public @NotNull PolyUserDefinedValue asUserDefinedValue() {
        return new PolyUserDefinedValue( null, null );
    }


    @Override
    public Object toJava() {
        return null;
    }


    @Override
    public String toString() {
        return null;
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyNull.class );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyNull.class );
    }


    public static class PolyNullSerializerDef extends SimpleSerializerDef<PolyNull> {

        @Override
        protected BinarySerializer<PolyNull> createSerializer( int version, CompatibilityLevel compatibilityLevel ) {
            return new BinarySerializer<>() {
                @Override
                public void encode( BinaryOutput out, PolyNull item ) {
                    out.writeUTF8Nullable( null );
                }


                @Override
                public PolyNull decode( BinaryInput in ) throws CorruptedDataException {
                    in.readUTF8Nullable();
                    return PolyNull.NULL;
                }
            };
        }

    }


}
