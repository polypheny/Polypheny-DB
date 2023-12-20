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

package org.polypheny.db.type.entity.temporal;

import com.fasterxml.jackson.core.JsonToken;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
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
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.util.TimestampString;

@Getter
@Value
@EqualsAndHashCode(callSuper = true)
public class PolyTimestamp extends PolyTemporal {

    public static final DateFormat dateFormat = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );

    public Long milliSinceEpoch; // normalized to UTC


    public PolyTimestamp( Long milliSinceEpoch ) {
        super( PolyType.TIMESTAMP );
        this.milliSinceEpoch = milliSinceEpoch;
    }


    public static PolyTimestamp of( Number number ) {
        return new PolyTimestamp( number.longValue() );
    }


    public static PolyTimestamp ofNullable( Number number ) {
        return number == null ? null : of( number );
    }


    public static PolyTimestamp ofNullable( Time value ) {
        return value == null ? null : PolyTimestamp.of( value );
    }


    public static PolyTimestamp of( long value ) {
        return new PolyTimestamp( value );
    }


    public static PolyTimestamp of( Long value ) {
        return new PolyTimestamp( value );
    }


    public static PolyTimestamp of( Timestamp value ) {
        return new PolyTimestamp( Functions.toLongOptional( value ) );
    }


    public static PolyTimestamp of( Date date ) {
        return new PolyTimestamp( date.getTime() );
    }


    public Timestamp asSqlTimestamp() {
        return new Timestamp( milliSinceEpoch );
    }


    @Override
    public String toJson() {
        return milliSinceEpoch == null ? JsonToken.VALUE_NULL.asString() : TimestampString.fromMillisSinceEpoch( milliSinceEpoch ).toString();
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        return Long.compare( milliSinceEpoch, o.asTimestamp().milliSinceEpoch );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyTimestamp.class, Expressions.constant( milliSinceEpoch ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyTimestamp.class );
    }


    public static PolyTimestamp convert( PolyValue value ) {
        if ( value.isNumber() ) {
            return PolyTimestamp.of( value.asNumber().longValue() );
        } else if ( value.isTemporal() ) {
            return PolyTimestamp.of( value.asTemporal().getMilliSinceEpoch() );
        }
        throw new NotImplementedException( "convert " + PolyTimestamp.class.getSimpleName() );
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return 16L;
    }

}