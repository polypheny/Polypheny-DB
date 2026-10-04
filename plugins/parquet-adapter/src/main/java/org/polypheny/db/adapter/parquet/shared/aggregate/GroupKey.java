/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.shared.aggregate;

import java.util.Arrays;
import java.util.Objects;


/**
 * Represents a group key consisting of multiple values. Used for group by aggregations.
 * The implementation is optimized for 1-2 columns in group by statement by reducing array allocations.
 */
public final class GroupKey {

    public final static GroupKey Empty = GroupKey.of( new Object[0] );

    private final Object first;
    private final Object second;
    private final Object[] remaining;
    private final int size;
    private final int hash;


    private GroupKey( Object first, Object second, Object[] remaining, int size ) {
        this.first = first;
        this.second = second;
        this.remaining = remaining;
        this.size = size;
        this.hash = computeHash();
    }


    public static GroupKey of( Object value ) {
        return new GroupKey( value, null, null, 1 );
    }


    public static GroupKey of( Object first, Object second ) {
        return new GroupKey( first, second, null, 2 );
    }


    public static GroupKey of( Object[] values ) {
        Object first = values.length > 0 ? values[0] : null;
        Object second = values.length > 1 ? values[1] : null;
        Object[] remaining = values.length > 2 ? Arrays.copyOfRange( values, 2, values.length ) : null;
        return new GroupKey( first, second, remaining, values.length );
    }


    private static int valueHash( Object value ) {
        return value instanceof byte[] bytes ? Arrays.hashCode( bytes ) : Objects.hashCode( value );
    }


    private static boolean valueEquals( Object left, Object right ) {
        if ( left instanceof byte[] leftBytes && right instanceof byte[] rightBytes ) {
            return Arrays.equals( leftBytes, rightBytes );
        }
        return Objects.equals( left, right );
    }


    public int size() {
        return size;
    }


    public Object value( int index ) {
        if ( index == 0 ) {
            return first;
        }
        if ( index == 1 ) {
            return second;
        }
        return remaining[index - 2];
    }


    private int computeHash() {
        int result = 1;
        result = 31 * result + valueHash( first );
        if ( size > 1 ) {
            result = 31 * result + valueHash( second );
        }
        if ( remaining != null ) {
            for ( Object value : remaining ) {
                result = 31 * result + valueHash( value );
            }
        }
        return result;
    }


    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( !(obj instanceof GroupKey other) || size != other.size || hash != other.hash ) {
            return false;
        }
        if ( !valueEquals( first, other.first ) ) {
            return false;
        }
        if ( size > 1 && !valueEquals( second, other.second ) ) {
            return false;
        }
        return Arrays.deepEquals( remaining, other.remaining );
    }


    @Override
    public int hashCode() {
        return hash;
    }

}
