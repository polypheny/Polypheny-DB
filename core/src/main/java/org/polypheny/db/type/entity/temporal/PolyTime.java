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
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.util.TimeString;

@EqualsAndHashCode(callSuper = true)
@Value
public class PolyTime extends PolyTemporal {

    @JsonProperty
    @Nullable
    public Integer ofDay;


    @JsonCreator
    public PolyTime( @JsonProperty("ofDay") @Nullable Integer ofDay ) {
        super( PolyType.TIME );
        this.ofDay = ofDay;
    }


    public static PolyTime of( PolyNumber value ) {
        return new PolyTime( value.intValue() );
    }


    public static PolyTime of( Number value ) {
        return new PolyTime( value.intValue() );
    }


    public static PolyTime ofNullable( Number value ) {
        return value == null ? new PolyTime( null ) : of( value );
    }


    public static PolyTime ofNullable( Time value ) {
        return value == null ? PolyTime.of( (Integer) null ) : of( value );
    }


    public static PolyTime of( Integer value ) {
        return new PolyTime( value );
    }


    public static PolyTime of( Time value ) {
        return new PolyTime( value.toLocalTime().toSecondOfDay() * 1000 );
    }


    public static PolyTime convert( @Nullable PolyValue value ) {
        if ( value == null ) {
            return null;
        }

        if ( value.isTime() ) {
            return value.asTime();
        }

        throw new GenericRuntimeException( getConvertError( value, PolyTime.class ) );
    }


    public Time asSqlTime() {
        return new Time( ofDay );
    }


    @Override
    public String toJson() {
        return ofDay == null ? JsonToken.VALUE_NULL.asString() : TimeString.fromMillisOfDay( ofDay ).toString();
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isTime() ) {
            return -1;
        }

        return Long.compare( ofDay, o.asTime().ofDay );
    }


    @Override
    public String toString() {
        return ofDay == null ? "null" : TimeString.fromMillisOfDay( ofDay ).toString();
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyTime.class, Expressions.constant( ofDay ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyTime.class );
    }


    @Override
    public Long getMillisSinceEpoch() {
        return ofDay == null ? null : Long.valueOf( ofDay );
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return 16L;
    }


    @Override
    public Object toJava() {
        return getOfDay();
    }


}
