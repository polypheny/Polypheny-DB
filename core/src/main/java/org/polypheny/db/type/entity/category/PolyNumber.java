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
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;

@Slf4j
public abstract class PolyNumber extends PolyValue {

    public PolyNumber( PolyType type ) {
        super( type );
    }


    public static int compareTo( PolyNumber b0, PolyNumber b1 ) {
        if ( b0 == null || b1 == null || b0.isNull() || b1.isNull() ) {
            return -1;
        }
        if ( b0.isApprox() || b1.isApprox() ) {
            return b0.DoubleValue().compareTo( b1.DoubleValue() );
        }
        return Objects.compare( b0.LongValue(), b1.LongValue(), Long::compareTo );
    }


    private boolean isApprox() {
        return PolyType.APPROX_TYPES.contains( getType() ) || getType() == PolyType.DECIMAL;
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null ) {
            return false;
        }

        if ( !(o instanceof PolyValue val) ) {
            return false;
        }

        if ( val.isNull() ) {
            return false;
        }

        if ( val.isNumber() ) {
            return PolyNumber.compareTo( this, val.asNumber() ) == 0;
        }

        return false;
    }


    /**
     * Returns the value of the specified number as an {@code int}, which may involve rounding or truncation.
     *
     * @return the numeric value represented by this object after conversion to type {@code int}.
     */
    public abstract int intValue();


    public Integer IntValue() {
        if ( isNull() ) {
            return null;
        }
        return intValue();
    }


    /**
     * Returns the value of the specified number as an {@code long}, which may involve rounding or truncation.
     *
     * @return the numeric value represented by this object after conversion to type {@code long}.
     */
    public abstract long longValue();


    public Long LongValue() {
        if ( isNull() ) {
            return null;
        }
        return longValue();
    }


    /**
     * Returns the value of the specified number as an {@code float}, which may involve rounding or truncation.
     *
     * @return the numeric value represented by this object after conversion to type {@code float}.
     */
    public abstract float floatValue();


    public Float FloatValue() {
        if ( isNull() ) {
            return null;
        }
        return floatValue();
    }


    /**
     * Returns the value of the specified number as a {@code double}, which may involve rounding.
     *
     * @return the numeric value represented by this object after conversion to type {@code double}.
     */
    public abstract double doubleValue();


    public Double DoubleValue() {
        if ( isNull() ) {
            return null;
        }
        return doubleValue();
    }


    @Override
    public @NotNull PolyBoolean asBoolean() {
        return intValue() == 0 ? PolyBoolean.FALSE : PolyBoolean.TRUE;
    }


    /**
     * Returns the value of the specified number as a {@code BigDecimal}, which may involve rounding.
     *
     * @return the numeric value represented by this object after conversion to type {@code BigDecimal}.
     */
    public abstract BigDecimal bigDecimalValue();


    public BigDecimal BigDecimalValue() {
        if ( isNull() ) {
            return null;
        }
        return bigDecimalValue();
    }


    public abstract PolyNumber increment();

    @NotNull
    public abstract PolyNumber divide( @NotNull PolyNumber other );


    @NotNull
    public abstract PolyNumber multiply( @NotNull PolyNumber other );

    @NotNull
    public abstract PolyNumber plus( @NotNull PolyNumber b1 );

    @NotNull
    public abstract PolyNumber subtract( @NotNull PolyNumber b1 );


    @NotNull
    public PolyNumber floor( @NotNull PolyNumber b1 ) {
        log.warn( "optimize" );
        final BigDecimal[] bigDecimals = bigDecimalValue().divideAndRemainder( b1.bigDecimalValue() );
        BigDecimal r = bigDecimals[1];
        if ( r.signum() < 0 ) {
            r = r.add( b1.bigDecimalValue() );
        }
        return PolyBigDecimal.of( bigDecimalValue().subtract( r ) );
    }


    public boolean isDecimal() {
        return PolyType.FRACTIONAL_TYPES.contains( type );
    }


    public PolyNumber ceil( PolyNumber b1 ) {
        final BigDecimal[] bigDecimals = bigDecimalValue().divideAndRemainder( b1.bigDecimalValue() );
        BigDecimal r = bigDecimals[1];
        if ( r.signum() > 0 ) {
            r = r.subtract( b1.bigDecimalValue() );
        }
        return PolyBigDecimal.of( bigDecimalValue().subtract( r ) );
    }


    public abstract PolyNumber negate();


    public @Nullable Number NumberValue() {
        return bigDecimalValue();
    }

}
