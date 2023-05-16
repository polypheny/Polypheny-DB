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

import java.util.Date;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;

@EqualsAndHashCode(callSuper = true)
@Value(staticConstructor = "of")
public class PolyDate extends PolyValue {


    public long value;


    public PolyDate( long sinceEpoch ) {
        super( PolyType.DATE, true );
        this.value = sinceEpoch;
    }


    public Date asDefaultDate() {
        return new Date( value );
    }


    public java.sql.Date asSqlDate() {
        return new java.sql.Date( value );
    }


    public static PolyDate of( Date date ) {
        return new PolyDate( (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY) );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isDate() ) {
            return -1;
        }

        return Long.compare( value, o.asDate().value );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyLong.class, Expressions.constant( value ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyDate.class );
    }

}
