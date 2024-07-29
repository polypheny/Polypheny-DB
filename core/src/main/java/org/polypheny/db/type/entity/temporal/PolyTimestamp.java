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

package org.polypheny.db.type.entity.temporal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonToken;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
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

    public static final DateFormat dateMilliFormat = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );


    static {
        dateFormat.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
        dateMilliFormat.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
    }


    @JsonProperty
    @Nullable
    public Long millisSinceEpoch; // normalized to UTC


    @JsonCreator
    public PolyTimestamp( @JsonProperty("millisSinceEpoch") @Nullable Long millisSinceEpoch ) {
        super( PolyType.TIMESTAMP );
        this.millisSinceEpoch = millisSinceEpoch;
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


    public static PolyTimestamp ofNullable( Timestamp value ) {
        return value == null ? null : PolyTimestamp.of( value );
    }


    public static PolyTimestamp of( long value ) {
        return new PolyTimestamp( value );
    }


    public static PolyTimestamp of( Long value ) {
        return new PolyTimestamp( value );
    }


    public static PolyTimestamp of( Timestamp value ) {
        return new PolyTimestamp( value.toInstant().toEpochMilli() );
    }


    public static PolyTimestamp of( Date date ) {
        return new PolyTimestamp( date.getTime() );
    }


    @Nullable
    public Timestamp asSqlTimestamp() {
        return millisSinceEpoch == null ? null : new Timestamp( millisSinceEpoch );
    }


    @Override
    public String toJson() {
        return millisSinceEpoch == null ? JsonToken.VALUE_NULL.asString() : TimestampString.fromMillisSinceEpoch( millisSinceEpoch ).toString();
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        } else if ( !o.isTimestamp() ) {
            return -1;
        } else if ( millisSinceEpoch == null && o.asTimestamp().millisSinceEpoch == null ) {
            return 0;
        } else if ( millisSinceEpoch == null ) {
            return -1;
        } else if ( o.asTimestamp().millisSinceEpoch == null ) {
            return 1;
        }

        return Long.compare( millisSinceEpoch, o.asTimestamp().millisSinceEpoch );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyTimestamp.class, Expressions.constant( millisSinceEpoch ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyTimestamp.class );
    }


    public static PolyTimestamp convert( @Nullable PolyValue value ) {
        if ( value == null ) {
            return null;
        }

        if ( value.isNumber() ) {
            return PolyTimestamp.of( value.asNumber().longValue() );
        } else if ( value.isTemporal() ) {
            return PolyTimestamp.of( value.asTemporal().getMillisSinceEpoch() );
        }
        throw new GenericRuntimeException( getConvertError( value, PolyTimestamp.class ) );
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return 16L;
    }


    @Override
    public Object toJava() {
        return getMillisSinceEpoch();
    }


    @Override
    public String toString() {
        if ( millisSinceEpoch == null ) {
            return null;
        }
        Date date = new Date( millisSinceEpoch );

        String dateString = dateMilliFormat.format( date );

        if ( dateString.endsWith( ".000" ) ) {
            return dateString.substring( 0, dateString.length() - 4 );
        }
        return dateString;
    }


}
