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

package org.polypheny.db.type.entity.category;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.experimental.NonFinal;
import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.temporal.DateTimeUtils;

@NonFinal
public abstract class PolyTemporal extends PolyNumber {

    private static final Pattern NEGATIVE_EXPONENT = Pattern.compile( "[+-]?(?:([0-9]+)(?:\\.[0-9]*)?|\\.[0-9]+)[eE]-([0-9]+)" );

    public static Expression MILLIS_OF_DAY = PolyLong.of( DateTimeUtils.MILLIS_PER_DAY ).asExpression();


    public PolyTemporal( PolyType type ) {
        super( type );
    }


    protected static PolyBigDecimal parseEpochString( PolyString text ) {
        try {
            return PolyBigDecimal.convert( text );
        } catch ( NumberFormatException e ) {
            Matcher exponent = NEGATIVE_EXPONENT.matcher( text.value.trim() );
            if ( exponent.matches() ) {
                String whole = exponent.group( 1 );
                int wholeDigits = whole == null ? 0 : whole.length();
                String magnitude = exponent.group( 2 );
                long shift = 0;
                for ( int i = 0; i < magnitude.length(); i++ ) {
                    shift = shift * 10 + magnitude.charAt( i ) - '0';
                    if ( shift >= wholeDigits ) {
                        // The decimal point moves past all whole digits, so the integral part is zero.
                        return PolyBigDecimal.of( BigDecimal.ZERO );
                    }
                }
            }
            throw e;
        }
    }


    public abstract Long getMillisSinceEpoch();


    @SuppressWarnings("unused")
    public PolyLong getPolyMillisSinceEpoch() {
        return PolyLong.of( getMillisSinceEpoch() );
    }


    public Long getDaysSinceEpoch() {
        return getMillisSinceEpoch() == null ? null : getMillisSinceEpoch() / DateTimeUtils.MILLIS_PER_DAY;
    }


    public Calendar toCalendar() {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTimeInMillis( getMillisSinceEpoch() );
        return cal;
    }


    public Long getMillisOfDay() {
        return getMillisSinceEpoch() == null ? null : (getMillisSinceEpoch() % DateTimeUtils.MILLIS_PER_DAY);
    }


    @Override
    public boolean isNumber() {
        return true;
    }


    @Override
    public @NotNull PolyNumber asNumber() {
        return PolyLong.of( getMillisSinceEpoch() );
    }


    @Override
    public int intValue() {
        return getMillisSinceEpoch().intValue();
    }


    @Override
    public long longValue() {
        return getMillisSinceEpoch();
    }


    @Override
    public float floatValue() {
        return getMillisSinceEpoch().floatValue();
    }


    @Override
    public double doubleValue() {
        return getMillisSinceEpoch().doubleValue();
    }


    @Override
    public BigDecimal bigDecimalValue() {
        return BigDecimal.valueOf( getMillisSinceEpoch() );
    }


    @Override
    public PolyNumber increment() {
        return PolyLong.of( getMillisSinceEpoch() + 1 );
    }


    @Override
    @NotNull
    public PolyNumber divide( @NotNull PolyNumber other ) {
        return PolyBigDecimal.of( bigDecimalValue().divide( other.bigDecimalValue(), MathContext.DECIMAL64 ) );
    }


    @Override
    @NotNull
    public PolyNumber multiply( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().multiply( other.bigDecimalValue() ) ) : PolyLong.of( getMillisSinceEpoch() * other.longValue() );
    }


    @Override
    @NotNull
    public PolyNumber plus( @NotNull PolyNumber b1 ) {
        return b1.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().add( b1.bigDecimalValue() ) ) : PolyLong.of( getMillisSinceEpoch() + b1.longValue() );
    }


    @Override
    @NotNull
    public PolyNumber subtract( @NotNull PolyNumber b1 ) {
        return b1.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().subtract( b1.bigDecimalValue() ) ) : PolyLong.of( getMillisSinceEpoch() - b1.longValue() );
    }


    @Override
    public PolyNumber negate() {
        return PolyLong.of( -getMillisSinceEpoch() );
    }

}
