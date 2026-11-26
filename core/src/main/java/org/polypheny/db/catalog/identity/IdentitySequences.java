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

package org.polypheny.db.catalog.identity;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.logical.IdentityColumn;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.persistance.DurableFile;
import org.polypheny.db.runtime.SqlStateException;
import org.polypheny.db.type.PolyType;


/**
 * Allocates values independently of transaction rollback. Sequence states are
 * persisted before their values are returned, so committed rows cannot cause
 * an identifier to be allocated again after a restart.
 */
@Slf4j
public final class IdentitySequences {

    private static final String PREFIX = "identity-";
    private static final String SUFFIX = ".sequence";
    private static final byte LEGACY_STATE_VERSION = 1;
    private static final byte STATE_VERSION = 2;
    private static final int LEGACY_STATE_SIZE = 2 + Long.BYTES;
    private static final int STATE_SIZE = 2 + 2 * Long.BYTES;
    private static final byte NEXT_AVAILABLE = 1;
    private static final byte LAST_AVAILABLE = 2;
    private static final String FIELD_ID_FLOOR_FILE = "identity-field-id.floor";
    private static final byte FIELD_ID_FLOOR_VERSION = 1;
    private static final int FIELD_ID_FLOOR_SIZE = 1 + Long.BYTES;

    private final Path directory;
    private final Map<UUID, SequenceState> states = new HashMap<>();


    /** Uses transient sequence state, like an in-memory catalog. */
    public IdentitySequences() {
        this( null );
    }


    /** Uses the catalog's persistence directory. */
    public IdentitySequences( Path directory ) {
        this.directory = directory;
    }


    /**
     * Records a field ID before native ADD COLUMN can publish it independently of the catalog.
     * After a crash the same physical column name must not be reused.
     */
    public synchronized void preserveFieldId( long columnId ) {
        if ( directory == null ) {
            return;
        }

        long floor = Math.incrementExact( columnId );
        if ( floor <= getFieldIdFloor() ) {
            return;
        }
        byte[] encoded = ByteBuffer.allocate( FIELD_ID_FLOOR_SIZE ).put( FIELD_ID_FLOOR_VERSION ).putLong( floor ).array();
        try {
            DurableFile.replace( directory.resolve( FIELD_ID_FLOOR_FILE ), encoded );
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Cannot persist identity field ID reservation.", e );
        }
    }


    public synchronized long getFieldIdFloor() {
        if ( directory == null ) {
            return 0;
        }
        try {
            byte[] bytes = Files.readAllBytes( directory.resolve( FIELD_ID_FLOOR_FILE ) );
            if ( bytes.length == FIELD_ID_FLOOR_SIZE && bytes[0] == FIELD_ID_FLOOR_VERSION ) {
                long floor = ByteBuffer.wrap( bytes ).getLong( 1 );
                if ( floor > 0 ) {
                    return floor;
                }
            }
            throw new GenericRuntimeException( "Invalid identity field ID reservation." );
        } catch ( NoSuchFileException ignored ) {
            return 0;
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Cannot read identity field ID reservation.", e );
        }
    }


    public synchronized OptionalLong peek( IdentityColumn identity ) {
        return state( identity ).next();
    }


    private SequenceState state( IdentityColumn identity ) {
        return states.computeIfAbsent( identity.id(), ignored -> read( identity ) );
    }


    public long next( IdentityColumn identity ) {
        return reserve( identity, 1 ).get( 0 );
    }


    public synchronized List<Long> reserve( IdentityColumn identity, int count ) {
        if ( count < 0 ) {
            throw new IllegalArgumentException( "Identity sequence reservation size must be nonnegative." );
        }
        if ( count == 0 ) {
            return List.of();
        }

        OptionalLong next = state( identity ).next();
        List<Long> values = new ArrayList<>( count );
        for ( int i = 0; i < count; i++ ) {
            if ( next.isEmpty() ) {
                throw exhausted( identity );
            }
            long value = next.getAsLong();
            values.add( value );
            next = successor( value, identity.increment(), identity.min(), identity.max(), identity.cycle() );
        }
        long last = values.get( values.size() - 1 );
        if ( directory != null ) {
            byte[] state = ByteBuffer.allocate( STATE_SIZE )
                    .put( STATE_VERSION )
                    .put( (byte) (LAST_AVAILABLE | (next.isPresent() ? NEXT_AVAILABLE : 0)) )
                    .putLong( next.orElse( 0 ) )
                    .putLong( last )
                    .array();
            try {
                DurableFile.replace( path( identity.id() ), state );
            } catch ( IOException e ) {
                states.remove( identity.id() );
                throw new GenericRuntimeException( "Cannot persist identity sequence " + identity.id(), e );
            }
        }
        states.put( identity.id(), new SequenceState( next, last ) );
        return values;
    }


    /** Checks a proposed allocation without consuming values, for schema migrations. */
    public synchronized void verifyCapacity( IdentityColumn identity, long count ) {
        if ( count < 0 ) {
            throw new IllegalArgumentException( "Identity sequence reservation size must be nonnegative." );
        }
        if ( count == 0 ) {
            return;
        }
        OptionalLong next = peek( identity );
        if ( next.isEmpty() ) {
            throw exhausted( identity );
        }
        if ( identity.cycle() ) {
            return;
        }

        BigInteger first = BigInteger.valueOf( next.getAsLong() );
        BigInteger step = BigInteger.valueOf( identity.increment() );
        BigInteger last = first.add( step.multiply( BigInteger.valueOf( count - 1 ) ) );
        long limit = identity.increment() > 0 ? identity.max() : identity.min();
        if ( last.compareTo( BigInteger.valueOf( limit ) ) * step.signum() > 0 ) {
            throw exhausted( identity );
        }
    }


    private static SqlStateException exhausted( IdentityColumn identity ) {
        boolean ascending = identity.increment() > 0;
        return new SqlStateException( String.format( "Identity sequence has reached its %s value (%s).",
                ascending ? "maximum" : "minimum", ascending ? identity.max() : identity.min() ), "2200H" );
    }


    public synchronized IdentityColumn reconfigure( PolyType type, IdentityColumn identity, Long start, Long increment, Long min, Long max, Boolean cycle ) {
        SequenceState state = state( identity );
        OptionalLong next = state.next();
        Long last = state.last();
        long step = increment == null ? identity.increment() : increment;
        long lower = min == null ? identity.min() : min;
        long upper = max == null ? identity.max() : max;
        boolean reversed = Long.signum( step ) != Long.signum( identity.increment() );
        boolean hasWrapped = last != null && next.isPresent() && identity.cycle()
                && next.getAsLong() == (identity.increment() > 0 ? identity.min() : identity.max())
                && successor( last, identity.increment(), identity.min(), identity.max(), false ).isEmpty();
        // On reversal the cached next points in the old direction. A pending RESTART has no last value.
        if ( last != null && (reversed || hasWrapped || next.isEmpty() || !withinBounds( next.getAsLong(), lower, upper )) ) {
            next = successor( last, step, lower, upper, cycle == null ? identity.cycle() : cycle );
        }
        return identity.withOptions( type, next, last, start, increment, min, max, cycle );
    }


    /**
     * Called after a catalog commit or restoration. The latest durable catalog
     * determines which sequence states can be safely removed.
     */
    public synchronized void retain( Set<UUID> active ) {
        states.keySet().retainAll( active );
        if ( directory == null ) {
            return;
        }
        try ( DirectoryStream<Path> files = Files.newDirectoryStream( directory, PREFIX + "*" + SUFFIX ) ) {
            for ( Path file : files ) {
                String name = file.getFileName().toString();
                UUID id;
                try {
                    id = UUID.fromString( name.substring( PREFIX.length(), name.length() - SUFFIX.length() ) );
                } catch ( IllegalArgumentException ignored ) {
                    continue;
                }
                if ( path( id ).equals( file ) && !active.contains( id ) ) {
                    Files.deleteIfExists( file );
                }
            }
        } catch ( IOException | DirectoryIteratorException e ) {
            log.warn( "Could not clean up unused identity sequences.", e );
        }
    }


    private SequenceState read( IdentityColumn identity ) {
        if ( directory != null ) {
            try {
                byte[] bytes = Files.readAllBytes( path( identity.id() ) );
                if ( bytes.length == STATE_SIZE && bytes[0] == STATE_VERSION ) {
                    ByteBuffer state = ByteBuffer.wrap( bytes );
                    byte flags = state.get( 1 );
                    long next = state.getLong( 2 );
                    if ( (flags & LAST_AVAILABLE) != 0 && (flags & ~(NEXT_AVAILABLE | LAST_AVAILABLE)) == 0 ) {
                        long last = state.getLong( 2 + Long.BYTES );
                        if ( (flags & NEXT_AVAILABLE) == 0 ) {
                            return new SequenceState( OptionalLong.empty(), last );
                        }
                        if ( withinBounds( next, identity.min(), identity.max() ) ) {
                            return new SequenceState( OptionalLong.of( next ), last );
                        }
                    }
                } else if ( bytes.length == LEGACY_STATE_SIZE && bytes[0] == LEGACY_STATE_VERSION ) {
                    byte available = bytes[1];
                    long next = ByteBuffer.wrap( bytes ).getLong( 2 );
                    if ( available == 0 ) {
                        return new SequenceState( OptionalLong.empty(), lastReachable( identity, identity.initialNext() ) );
                    }
                    if ( available == NEXT_AVAILABLE && withinBounds( next, identity.min(), identity.max() ) ) {
                        return new SequenceState( OptionalLong.of( next ), inferLegacyLast( identity, next ) );
                    }
                }
                throw new GenericRuntimeException( "Invalid stored identity sequence " + identity.id() );
            } catch ( NoSuchFileException ignored ) {
                // First use of the sequence.
            } catch ( IOException e ) {
                throw new GenericRuntimeException( "Cannot read identity sequence " + identity.id(), e );
            }
        }
        return new SequenceState( identity.initiallyExhausted() ? OptionalLong.empty() : OptionalLong.of( identity.initialNext() ), identity.initialLast() );
    }


    private static Long inferLegacyLast( IdentityColumn identity, long next ) {
        long first = identity.increment() > 0 ? identity.min() : identity.max();
        if ( identity.cycle() && next == first ) {
            // A legacy wrapped value can follow either the initial or a later cycle.
            long initialLast = lastReachable( identity, identity.initialNext() );
            long cycleLast = lastReachable( identity, first );
            return initialLast == cycleLast ? cycleLast : null;
        }
        try {
            long last = Math.subtractExact( next, identity.increment() );
            return withinBounds( last, identity.min(), identity.max() ) ? last : null;
        } catch ( ArithmeticException ignored ) {
            return null;
        }
    }


    private static long lastReachable( IdentityColumn identity, long firstValue ) {
        BigInteger first = BigInteger.valueOf( firstValue );
        BigInteger step = BigInteger.valueOf( identity.increment() );
        BigInteger limit = BigInteger.valueOf( identity.increment() > 0 ? identity.max() : identity.min() );
        return first.add( limit.subtract( first ).divide( step ).multiply( step ) ).longValueExact();
    }


    private static boolean withinBounds( long value, long min, long max ) {
        return value >= min && value <= max;
    }


    private static OptionalLong successor( long value, long increment, long min, long max, boolean cycle ) {
        boolean ascending = increment > 0;
        boolean overflows = ascending ? value > Long.MAX_VALUE - increment : value < Long.MIN_VALUE - increment;
        if ( !overflows ) {
            long next = value + increment;
            if ( withinBounds( next, min, max ) ) {
                return OptionalLong.of( next );
            }
        }
        if ( !cycle ) {
            return OptionalLong.empty();
        }
        return OptionalLong.of( ascending ? min : max );
    }


    private Path path( UUID id ) {
        return directory.resolve( PREFIX + id + SUFFIX );
    }


    private record SequenceState(OptionalLong next, Long last) {

    }

}
