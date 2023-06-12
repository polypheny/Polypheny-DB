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
import java.time.LocalDate;
import java.util.Date;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyTemporal;

@Value
@EqualsAndHashCode(callSuper = true)
public class PolyDate extends PolyTemporal {

    @Getter
    public Long sinceEpoch;


    public PolyDate( long sinceEpoch ) {
        super( PolyType.DATE );
        this.sinceEpoch = sinceEpoch;
    }


    public static PolyDate of( Number number ) {
        return new PolyDate( number.longValue() );
    }


    public Date asDefaultDate() {
        return new Date( sinceEpoch );
    }


    public java.sql.Date asSqlDate() {
        return java.sql.Date.valueOf( LocalDate.ofEpochDay( sinceEpoch ) );
    }


    public static PolyDate of( Date date ) {
        return new PolyDate( (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY) );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isDate() ) {
            return -1;
        }

        return Long.compare( sinceEpoch, o.asDate().sinceEpoch );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyLong.class, Expressions.constant( sinceEpoch ) );
    }


    public static class PolyDateSerializer implements JsonSerializer<PolyDate>, JsonDeserializer<PolyDate> {

        @Override
        public JsonElement serialize( PolyDate src, Type typeOfSrc, JsonSerializationContext context ) {
            return new JsonPrimitive( src.sinceEpoch );
        }


        @Override
        public PolyDate deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            return PolyDate.of( json.getAsLong() );
        }

    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyDate.class );
    }

}
