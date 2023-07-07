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

package org.polypheny.db.type.entity;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyTemporal;

@Value
@EqualsAndHashCode(callSuper = true)
public class PolyTimeStamp extends PolyTemporal {

    @Getter
    public Long sinceEpoch;


    public PolyTimeStamp( Long sinceEpoch ) {
        super( PolyType.TIMESTAMP );
        this.sinceEpoch = sinceEpoch;
    }


    public static PolyTimeStamp of( Number number ) {
        return new PolyTimeStamp( number.longValue() );
    }


    public static PolyTimeStamp ofNullable( Number number ) {
        return number == null ? null : of( number );
    }


    public static PolyTimeStamp ofNullable( Time value ) {
        return value == null ? null : PolyTimeStamp.of( value );
    }


    public static PolyTimeStamp of( long value ) {
        return new PolyTimeStamp( value );
    }


    public static PolyTimeStamp of( Long value ) {
        return new PolyTimeStamp( value );
    }


    public static PolyTimeStamp of( Timestamp value ) {
        return new PolyTimeStamp( Functions.toLongOptional( value ) );
    }


    public static PolyTimeStamp of( Date date ) {
        return new PolyTimeStamp( date.getTime() );
    }


    public Timestamp asSqlTimestamp() {
        return new Timestamp( sinceEpoch );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        return Long.compare( sinceEpoch, o.asTimeStamp().sinceEpoch );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyTimeStamp.class, Expressions.constant( sinceEpoch ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyTimeStamp.class );
    }


    public static PolyTimeStamp convert( PolyValue value ) {
        if ( value.isNumber() ) {
            return PolyTimeStamp.of( value.asNumber().longValue() );
        } else if ( value.isTemporal() ) {
            return PolyTimeStamp.of( value.asTemporal().getSinceEpoch() );
        }
        throw new NotImplementedException( "convert " + PolyTimeStamp.class.getSimpleName() );
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return 16L;
    }


    public static class PolyTimeStampSerializer implements JsonSerializer<PolyTimeStamp>, JsonDeserializer<PolyTimeStamp> {

        @Override
        public PolyTimeStamp deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            return PolyTimeStamp.of( json.getAsLong() );
        }


        @Override
        public JsonElement serialize( PolyTimeStamp src, Type typeOfSrc, JsonSerializationContext context ) {
            return new JsonPrimitive( src.sinceEpoch );
        }

    }

}
