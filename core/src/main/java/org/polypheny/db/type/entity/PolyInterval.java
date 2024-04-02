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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.util.temporal.TimeUnit;

@EqualsAndHashCode(callSuper = true)
@Value
@Slf4j
@NonFinal
public class PolyInterval extends PolyValue {


    @NotNull
    public Long millis;

    @Getter
    @NotNull
    public Long months;


    /**
     * Creates a PolyInterval, which includes a millis and a month value, to allow for combinations like 7-1 Month to Day (e.g. used in SQL).
     * Which is 7 months and 1 day (represented as PolyInterval with 7 months and 1*24h*60min*60s*1000ms).
     *
     * @param millis millis since Epoch.
     * @param months months since Epoch.
     */
    public PolyInterval( @NotNull Long millis, @NotNull Long months ) {
        super( PolyType.INTERVAL );
        this.millis = millis;
        this.months = months;
    }


    private static MonthsMilliseconds normalize( Long value, TimeUnit unit ) {
        if ( unit == TimeUnit.YEAR ) {
            return new MonthsMilliseconds( value * 12, 0 );
        } else if ( unit == TimeUnit.MONTH ) {
            return new MonthsMilliseconds( value, 0 );
        } else if ( unit == TimeUnit.DAY ) {
            return new MonthsMilliseconds( 0, value * 24 * 60 * 60 * 1000 );
        } else if ( unit == TimeUnit.HOUR ) {
            return new MonthsMilliseconds( 0, value * 60 * 60 * 1000 );
        } else if ( unit == TimeUnit.MINUTE ) {
            return new MonthsMilliseconds( 0, value * 60 * 1000 );
        } else if ( unit == TimeUnit.SECOND ) {
            return new MonthsMilliseconds( 0, value * 1000 );
        } else if ( unit == TimeUnit.MILLISECOND ) {
            return new MonthsMilliseconds( 0, value );
        } else {
            throw new GenericRuntimeException( "Normalization is not supported" );

        }

    }


    private static MonthsMilliseconds normalize( Long value, IntervalQualifier qualifier ) {
        return switch ( qualifier.getTimeUnitRange() ) {
            case DOW -> new MonthsMilliseconds( 0L, value * 24 * 60 * 60 * 1000 );
            case DOY -> new MonthsMilliseconds( 0L, value * 24 * 60 * 60 * 1000 );
            case QUARTER -> new MonthsMilliseconds( value * 3, 0L );
            case YEAR -> new MonthsMilliseconds( value * 12, 0L );
            case MONTH -> new MonthsMilliseconds( value, 0L );
            case DAY -> new MonthsMilliseconds( 0L, value * 24 * 60 * 60 * 1000 );
            case HOUR -> new MonthsMilliseconds( 0L, value * 60 * 60 * 1000 );
            case MINUTE -> new MonthsMilliseconds( 0L, value * 60 * 1000 );
            case SECOND -> new MonthsMilliseconds( 0L, value * 1000 );
            case MILLISECOND -> new MonthsMilliseconds( 0L, value );
            case WEEK -> new MonthsMilliseconds( 0L, value * 7 * 24 * 60 * 60 * 1000 );
            case MINUTE_TO_SECOND -> new MonthsMilliseconds( 0L, value * 60 * 60 * 1000 );
            default -> throw new NotImplementedException( "since Epoch" );
        };
    }


    public static PolyInterval of( Long millis, Long months ) {
        return new PolyInterval( millis, months );
    }


    public static PolyInterval of( Long value, TimeUnit type ) {
        MonthsMilliseconds millisMonths = normalize( value, type );
        return new PolyInterval( millisMonths.milliseconds, millisMonths.months );
    }


    public static PolyInterval of( Long value, IntervalQualifier qualifier ) {
        MonthsMilliseconds millisMonths = normalize( value, qualifier );
        return new PolyInterval( millisMonths.milliseconds, millisMonths.months );
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
        return Expressions.new_( PolyInterval.class, Expressions.constant( millis ), Expressions.constant( months ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyInterval.class );
    }


    @Override
    public @Nullable Long deriveByteSize() {
        return null;
    }


    @Override
    public Object toJava() {
        return millis;
    }


    public PolyNumber getLeap( IntervalQualifier intervalQualifier ) {
        switch ( intervalQualifier.getTimeUnitRange() ) {
            case YEAR, QUARTER, MONTH, YEAR_TO_MONTH -> {
                return PolyLong.of( months );
            }
            case DAY, DOW, DOY, HOUR, MINUTE, SECOND, MILLISECOND, MINUTE_TO_SECOND, HOUR_TO_MINUTE, WEEK, DAY_TO_HOUR, DAY_TO_MINUTE, DAY_TO_MILLISECOND, DAY_TO_SECOND, HOUR_TO_SECOND -> {
                return PolyLong.of( millis );
            }
            default -> throw new NotImplementedException( "get Leap" );
        }
    }


    public record MonthsMilliseconds(long months, long milliseconds) {

    }

}
