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

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;

/**
 * Default values for Polypheny-DB {@PolyValue}s.
 */
public interface PolyDefaults {

    Map<Type, PolyValue> DEFAULTS = new HashMap<>() {{
        put( PolyLong.class, PolyLong.of( 0 ) );
        put( PolyDouble.class, PolyDouble.of( 0.0 ) );
        put( PolyFloat.class, PolyFloat.of( 0.0f ) );
        put( PolyInteger.class, PolyInteger.of( 0 ) );
        put( PolyBoolean.class, PolyBoolean.of( false ) );
        put( PolyNumber.class, PolyLong.of( 0 ) );
    }};

    Map<Type, Type> PRIMITIVES = new HashMap<>() {{
        put( PolyLong.class, long.class );
        put( PolyInteger.class, int.class );
        put( PolyFloat.class, float.class );
        put( PolyDouble.class, double.class );
        put( PolyString.class, String.class );
        put( PolyBigDecimal.class, long.class );
        put( PolyBoolean.class, boolean.class );
        put( PolyNumber.class, BigDecimal.class );
    }};

    Map<Type, PolyValue> NULLS = new HashMap<>() {{
        put( PolyNumber.class, PolyNull.NULL.asNumber() );
        put( PolyBigDecimal.class, PolyNull.NULL.asBigDecimal() );
        put( PolyLong.class, PolyNull.NULL.asLong() );
        put( PolyTimestamp.class, PolyNull.NULL.asTimestamp() );
        put( PolyInteger.class, PolyNull.NULL.asInteger() );
        put( PolyFloat.class, PolyNull.NULL.asFloat() );
        put( PolyDouble.class, PolyNull.NULL.asDouble() );
        put( PolyString.class, PolyNull.NULL.asString() );
        put( PolyBoolean.class, PolyNull.NULL.asBoolean() );
    }};

    Map<Type, Type> MAPPINGS = new HashMap<>() {{
        put( PolyNumber.class, BigDecimal.class );
        put( PolyString.class, String.class );
        put( PolyDate.class, Long.class );
        put( PolyTime.class, Integer.class );
        put( PolyTimestamp.class, Long.class );
    }};

}
