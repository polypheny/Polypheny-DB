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
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.type.PolySerializable;

@EqualsAndHashCode(callSuper = true)
@Value
@Slf4j
public class PolyInterval extends PolyValue {


    public BigDecimal value;
    public IntervalQualifier qualifier;


    public PolyInterval( BigDecimal value, IntervalQualifier qualifier ) {
        super( qualifier.typeName() );
        this.value = value;
        this.qualifier = qualifier;
    }


    public static PolyInterval of( BigDecimal value, IntervalQualifier type ) {
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
        return Expressions.new_( PolyInterval.class, Expressions.constant( value ), qualifier.asExpression() );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyInterval.class );
    }


    public Long getMonths() {
        log.warn( "might adjust" );
        switch ( qualifier.getTimeUnitRange() ) {
            case YEAR:
                return value.longValue();
            case MONTH:
                return value.longValue();
        }
        throw new NotImplementedException( "since Epoch" );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    public static class PolyIntervalSerializer implements JsonSerializer<PolyInterval>, JsonDeserializer<PolyInterval> {


        public static final String DATA = "data";
        public static final String QUALIFIER = "qualifier";


        @Override
        public PolyInterval deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            JsonObject object = json.getAsJsonObject();
            return PolyInterval.of( object.get( DATA ).getAsBigDecimal(), context.deserialize( object.get( QUALIFIER ), IntervalQualifier.class ) );
        }


        @Override
        public JsonElement serialize( PolyInterval src, Type typeOfSrc, JsonSerializationContext context ) {
            JsonObject object = new JsonObject();
            object.add( QUALIFIER, context.serialize( src.qualifier ) );
            object.addProperty( DATA, src.value );
            return object;
        }

    }

}
