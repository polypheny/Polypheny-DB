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

    public long value;

    public TimeUnit timeUnit;


    public PolyTime( long value, TimeUnit timeUnit ) {
        super( PolyType.TIME );
        this.value = value;
        this.timeUnit = timeUnit;
    }


    public static PolyTime of( long value ) {
        return new PolyTime( value, TimeUnit.MILLISECOND );
    }


    public Time asSqlTime() {
        return new Time( value );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isTime() ) {
            return -1;
        }

        return Long.compare( value, o.asTime().value );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyTime.class, Expressions.constant( value ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyTime.class );
    }

}
