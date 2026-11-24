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

package org.polypheny.db.catalog.entity.logical;

import java.io.Serializable;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;


/**
 * The definition of an identity column and the initial value or exhaustion of its current sequence.
 * Replacing the sequence key makes a restart part of the schema change while keeping allocations independent of rollback.
 */
public record IdentityColumn(
        UUID id,
        Mode mode,
        long start,
        long increment,
        long min,
        long max,
        boolean cycle,
        long initialNext,
        boolean initiallyExhausted ) implements Serializable {

    public enum Mode {
        ALWAYS,
        BY_DEFAULT
    }


    public IdentityColumn {
        Objects.requireNonNull( id );
        Objects.requireNonNull( mode );
        if ( increment == 0 ) {
            throw new GenericRuntimeException( "Identity sequence increment must be nonzero." );
        }
        if ( min >= max ) {
            throw new GenericRuntimeException( "Identity sequence minimum must be less than its maximum." );
        }
        checkValue( start, min, max, "start" );
        if ( !initiallyExhausted ) {
            checkValue( initialNext, min, max, "next value" );
        }
    }


    public static IdentityColumn create( PolyType type, Mode mode, Long start, Long increment, Long min, Long max, boolean cycle ) {
        long step = increment == null ? 1 : increment;
        long lower = min == null ? defaultMin( type, step ) : min;
        long upper = max == null ? defaultMax( type, step ) : max;
        long first = start == null ? (step > 0 ? lower : upper) : start;
        IdentityColumn identity = new IdentityColumn( UUID.randomUUID(), mode, first, step, lower, upper, cycle, first, false );
        identity.validateType( type );
        return identity;
    }


    public IdentityColumn restart( Long next ) {
        return new IdentityColumn( UUID.randomUUID(), mode, start, increment, min, max, cycle, next == null ? start : next, false );
    }


    public IdentityColumn withMode( Mode newMode ) {
        return new IdentityColumn( id, newMode, start, increment, min, max, cycle, initialNext, initiallyExhausted );
    }


    public IdentityColumn withOptions( PolyType type, OptionalLong next, Long newStart, Long newIncrement, Long newMin, Long newMax, Boolean newCycle ) {
        long configuredStart = newStart == null ? start : newStart;
        IdentityColumn identity = new IdentityColumn(
                UUID.randomUUID(), mode,
                configuredStart,
                newIncrement == null ? increment : newIncrement,
                newMin == null ? min : newMin,
                newMax == null ? max : newMax,
                newCycle == null ? cycle : newCycle,
                next.orElse( configuredStart ), next.isEmpty() );
        identity.validateType( type );
        return identity;
    }


    public void validateType( PolyType type ) {
        if ( min < lowerBound( type ) || max > upperBound( type ) ) {
            throw new GenericRuntimeException( "Identity sequence bounds exceed the range of %s.", type );
        }
    }


    public static long defaultMin( PolyType type, long increment ) {
        return increment > 0 ? 1 : lowerBound( type );
    }


    public static long defaultMax( PolyType type, long increment ) {
        return increment > 0 ? upperBound( type ) : -1;
    }


    private static long lowerBound( PolyType type ) {
        return switch ( type ) {
            case SMALLINT -> Short.MIN_VALUE;
            case INTEGER -> Integer.MIN_VALUE;
            case BIGINT -> Long.MIN_VALUE;
            default -> throw new GenericRuntimeException( "The type %s does not support identity generation.", type );
        };
    }


    private static long upperBound( PolyType type ) {
        return switch ( type ) {
            case SMALLINT -> Short.MAX_VALUE;
            case INTEGER -> Integer.MAX_VALUE;
            case BIGINT -> Long.MAX_VALUE;
            default -> throw new GenericRuntimeException( "The type %s does not support identity generation.", type );
        };
    }


    private static void checkValue( long value, long min, long max, String label ) {
        if ( value < min || value > max ) {
            throw new GenericRuntimeException( "Identity sequence %s %s is outside [%s, %s].", label, value, min, max );
        }
    }

}
