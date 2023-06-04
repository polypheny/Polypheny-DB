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
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;

@EqualsAndHashCode(callSuper = true)
@Value
public class PolyInterval extends PolyValue {


    public BigDecimal value;

    public TimeUnitRange unitRange;


    public PolyInterval( BigDecimal value, PolyType type ) {
        super( type );
        this.value = value;
        this.unitRange = TimeUnitRange.DAY; // todo adjust
    }


    public static PolyInterval of( BigDecimal value, PolyType type ) {
        return new PolyInterval( value, type );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }
        return 0;
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyInterval.class, Expressions.constant( value ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyInterval.class );
    }


    public static class PolyIntervalSerializer implements JsonSerializer<PolyInterval>, JsonDeserializer<PolyInterval> {


        public static final String DATA = "data";
        public static final String TYPE = "type";


        @Override
        public PolyInterval deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            JsonObject object = json.getAsJsonObject();
            return PolyInterval.of( object.get( DATA ).getAsBigDecimal(), PolyType.get( object.get( TYPE ).getAsString() ) );
        }


        @Override
        public JsonElement serialize( PolyInterval src, Type typeOfSrc, JsonSerializationContext context ) {
            JsonObject object = new JsonObject();
            object.addProperty( TYPE, src.type.getName() );
            object.addProperty( DATA, src.value );
            return object;
        }

    }

}
