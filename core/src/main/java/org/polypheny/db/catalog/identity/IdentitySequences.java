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
    private static final byte VERSION = 1;
    private static final int STATE_SIZE = 2 + Long.BYTES;
    private static final String FIELD_ID_FLOOR_FILE = "identity-field-id.floor";
    private static final int FIELD_ID_FLOOR_SIZE = 1 + Long.BYTES;

    private final Path directory;
    private final Map<UUID, OptionalLong> states = new HashMap<>();


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
        byte[] encoded = ByteBuffer.allocate( FIELD_ID_FLOOR_SIZE ).put( VERSION ).putLong( floor ).array();
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
            if ( bytes.length == FIELD_ID_FLOOR_SIZE && bytes[0] == VERSION ) {
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

        OptionalLong next = peek( identity );
        List<Long> values = new ArrayList<>( count );
        for ( int i = 0; i < count; i++ ) {
            if ( next.isEmpty() ) {
                throw exhausted( identity );
            }
            long value = next.getAsLong();
            values.add( value );
            next = successor( identity, value );
        }
        if ( directory != null ) {
            byte[] state = ByteBuffer.allocate( STATE_SIZE )
                    .put( VERSION )
                    .put( (byte) (next.isPresent() ? 1 : 0) )
                    .putLong( next.orElse( 0 ) )
                    .array();
            try {
                DurableFile.replace( path( identity.id() ), state );
            } catch ( IOException e ) {
                states.remove( identity.id() );
                throw new GenericRuntimeException( "Cannot persist identity sequence " + identity.id(), e );
            }
        }
        states.put( identity.id(), next );
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


    private static GenericRuntimeException exhausted( IdentityColumn identity ) {
        boolean ascending = identity.increment() > 0;
        return new GenericRuntimeException( "Identity sequence has reached its %s value (%s).",
                ascending ? "maximum" : "minimum", ascending ? identity.max() : identity.min() );
    }


    public synchronized IdentityColumn reconfigure( PolyType type, IdentityColumn identity, Long start, Long increment, Long min, Long max, Boolean cycle ) {
        return identity.withOptions( type, peek( identity ), start, increment, min, max, cycle );
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


    private OptionalLong read( IdentityColumn identity ) {
        if ( directory != null ) {
            try {
                byte[] bytes = Files.readAllBytes( path( identity.id() ) );
                if ( bytes.length == STATE_SIZE ) {
                    ByteBuffer state = ByteBuffer.wrap( bytes );
                    byte version = state.get();
                    byte available = state.get();
                    long next = state.getLong();
                    if ( version == VERSION && available == 0 ) {
                        return OptionalLong.empty();
                    }
                    if ( version == VERSION && available == 1 && next >= identity.min() && next <= identity.max() ) {
                        return OptionalLong.of( next );
                    }
                }
                throw new GenericRuntimeException( "Invalid stored identity sequence " + identity.id() );
            } catch ( NoSuchFileException ignored ) {
                // First use of the sequence.
            } catch ( IOException e ) {
                throw new GenericRuntimeException( "Cannot read identity sequence " + identity.id(), e );
            }
        }
        return identity.initiallyExhausted() ? OptionalLong.empty() : OptionalLong.of( identity.initialNext() );
    }


    private static OptionalLong successor( IdentityColumn identity, long value ) {
        long increment = identity.increment();
        boolean ascending = increment > 0;
        boolean overflows = ascending ? value > Long.MAX_VALUE - increment : value < Long.MIN_VALUE - increment;
        if ( !overflows ) {
            long next = value + increment;
            if ( next >= identity.min() && next <= identity.max() ) {
                return OptionalLong.of( next );
            }
        }
        if ( !identity.cycle() ) {
            return OptionalLong.empty();
        }
        return OptionalLong.of( ascending ? identity.min() : identity.max() );
    }


    private Path path( UUID id ) {
        return directory.resolve( PREFIX + id + SUFFIX );
    }

}
