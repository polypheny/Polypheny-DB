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

package org.polypheny.db.type.entity.category;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Calendar;
import java.util.GregorianCalendar;
import lombok.experimental.NonFinal;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;

@NonFinal
public abstract class PolyTemporal extends PolyNumber {

    public PolyTemporal( PolyType type ) {
        super( type );
    }


    public abstract Long getMilliSinceEpoch();


    public long getDaysSinceEpoch() {
        return getMilliSinceEpoch() / DateTimeUtils.MILLIS_PER_DAY;
    }


    public Calendar toCalendar() {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTimeInMillis( getMilliSinceEpoch() );
        return cal;
    }


    public int getMillisOfDay() {
        return (int) (getMilliSinceEpoch() % DateTimeUtils.MILLIS_PER_DAY);
    }


    @Override
    public int intValue() {
        return getMilliSinceEpoch().intValue();
    }


    @Override
    public long longValue() {
        return getMilliSinceEpoch();
    }


    @Override
    public float floatValue() {
        return getMilliSinceEpoch().floatValue();
    }


    @Override
    public double doubleValue() {
        return getMilliSinceEpoch().doubleValue();
    }


    @Override
    public BigDecimal bigDecimalValue() {
        return BigDecimal.valueOf( getMilliSinceEpoch() );
    }


    @Override
    public PolyNumber increment() {
        return PolyLong.of( getMilliSinceEpoch() + 1 );
    }


    @Override
    @NotNull
    public PolyNumber divide( @NotNull PolyNumber other ) {
        return PolyBigDecimal.of( bigDecimalValue().divide( other.bigDecimalValue(), MathContext.DECIMAL64 ) );
    }


    @Override
    @NotNull
    public PolyNumber multiply( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().multiply( other.bigDecimalValue() ) ) : PolyLong.of( getMilliSinceEpoch() * other.longValue() );
    }


    @Override
    @NotNull
    public PolyNumber plus( @NotNull PolyNumber b1 ) {
        return b1.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().add( b1.bigDecimalValue() ) ) : PolyLong.of( getMilliSinceEpoch() + b1.longValue() );
    }


    @Override
    @NotNull
    public PolyNumber subtract( @NotNull PolyNumber b1 ) {
        return b1.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().subtract( b1.bigDecimalValue() ) ) : PolyLong.of( getMilliSinceEpoch() - b1.longValue() );
    }


    @Override
    public PolyNumber negate() {
        return PolyLong.of( -getMilliSinceEpoch() );
    }

}
