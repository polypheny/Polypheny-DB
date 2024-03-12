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

package org.polypheny.db.type.entity;

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


    /**
     * Creates a PolyInterval.
     *
     * @param value The amount of the range
     * @param qualifier The unit qualifier, e.g. YEAR, MONTH, DAY, etc.
     */
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
        return switch ( qualifier.getTimeUnitRange() ) {
            case YEAR -> value.longValue();
            case MONTH -> value.longValue();
            default -> throw new NotImplementedException( "since Epoch" );
        };
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public Object toJava() {
        return value;
    }


    public long getMillis() {
        log.warn( "might adjust" );
        return switch ( qualifier.getTimeUnitRange() ) {
            case YEAR -> value.longValue() * 24 * 60 * 60 * 1000;
            case MONTH -> value.longValue();
            default -> throw new NotImplementedException( "since Epoch" );
        };
    }

}
