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
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyTemporal;

@EqualsAndHashCode(callSuper = true)
@Value
public class PolyTime extends PolyTemporal {

    public Integer ofDay;

    public TimeUnit timeUnit;


    public PolyTime( int ofDay, TimeUnit timeUnit ) {
        super( PolyType.TIME );
        this.ofDay = ofDay;
        this.timeUnit = timeUnit;
    }


    public static PolyTime of( Number value ) {
        return new PolyTime( value.intValue(), TimeUnit.MILLISECOND );
    }


    public static PolyTime ofNullable( Number value ) {
        return value == null ? null : of( value );
    }


    public static PolyTime of( Integer value ) {
        return new PolyTime( value, TimeUnit.MILLISECOND );
    }


    public static PolyTime of( Time value ) {
        return new PolyTime( (int) value.getTime(), TimeUnit.MILLISECOND );
    }


    public static PolyTime of( Time value ) {
        return new PolyTime( value.getTime(), TimeUnit.MILLISECOND );
    }


    public Time asSqlTime() {
        return new Time( ofDay );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isTime() ) {
            return -1;
        }

        return Long.compare( ofDay, o.asTime().ofDay );
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
    public Long getSinceEpoch() {
        return Long.valueOf( ofDay );
    }


    @Override
    public @NotNull Long deriveByteSize() {
        return 16L + timeUnit.name().getBytes().length;
    }


    public static class PolyTimeSerializer implements JsonSerializer<PolyTime>, JsonDeserializer<PolyTime> {


        @Override
        public JsonElement serialize( PolyTime src, Type typeOfSrc, JsonSerializationContext context ) {
            return new JsonPrimitive( src.ofDay );
        }


        @Override
        public PolyTime deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            return PolyTime.of( json.getAsInt() );
        }

    }

}
