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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.activej.codegen.DefiningClassLoader;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.SerializerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.catalog.entity.LogicalDefaultValue;
import org.polypheny.db.catalog.entity.logical.IdentityColumn;
import org.polypheny.db.catalog.entity.logical.IdentityColumn.Mode;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;

class IdentitySequencesTest {

    @TempDir
    Path directory;


    @Test
    void fieldIdReservationSurvivesReloadAndIsMonotonic() {
        IdentitySequences sequences = new IdentitySequences( directory );
        assertEquals( 0, sequences.getFieldIdFloor() );
        sequences.preserveFieldId( 41 );
        sequences.preserveFieldId( 30 );
        assertEquals( 42, new IdentitySequences( directory ).getFieldIdFloor() );

        sequences.retain( Set.of() );
        assertEquals( 42, new IdentitySequences( directory ).getFieldIdFloor() );
    }


    @Test
    void persistedNextValueAndExhaustionSurviveReload() {
        IdentityColumn identity = IdentityColumn.create( PolyType.BIGINT, Mode.BY_DEFAULT, Long.MAX_VALUE - 1, null, null, null, false );
        IdentitySequences sequences = new IdentitySequences( directory );

        assertEquals( Long.MAX_VALUE - 1, sequences.next( identity ) );

        IdentitySequences reloaded = new IdentitySequences( directory );
        assertEquals( OptionalLong.of( Long.MAX_VALUE ), reloaded.peek( identity ) );
        assertEquals( Long.MAX_VALUE, reloaded.next( identity ) );

        IdentitySequences exhausted = new IdentitySequences( directory );
        assertEquals( OptionalLong.empty(), exhausted.peek( identity ) );
        assertThrows( GenericRuntimeException.class, () -> exhausted.next( identity ) );
    }


    @Test
    void restartKeepsConfiguredStartAndRetiresPreviousStateOnCommit() throws Exception {
        IdentityColumn identity = IdentityColumn.create( PolyType.INTEGER, Mode.BY_DEFAULT, 6L, 3L, null, null, false );
        IdentitySequences sequences = new IdentitySequences( directory );

        assertEquals( List.of( 6L, 9L ), sequences.reserve( identity, 2 ) );
        IdentityColumn restarted = identity.restart( 100L );
        assertNotEquals( identity.id(), restarted.id() );
        assertEquals( 100L, sequences.next( restarted ) );
        assertEquals( OptionalLong.of( 12L ), sequences.peek( identity ) );
        assertEquals( OptionalLong.of( 6L ), sequences.peek( restarted.restart( null ) ) );

        sequences.retain( Set.of( restarted.id() ) );
        assertFalse( Files.exists( directory.resolve( "identity-" + identity.id() + ".sequence" ) ) );
        assertEquals( OptionalLong.of( 103L ), new IdentitySequences( directory ).peek( restarted ) );
    }


    @Test
    void concurrentReservationsDoNotOverlap() throws Exception {
        IdentityColumn identity = IdentityColumn.create( PolyType.INTEGER, Mode.ALWAYS, null, null, null, null, false );
        IdentitySequences sequences = new IdentitySequences( directory );
        ExecutorService workers = Executors.newFixedThreadPool( 4 );
        try {
            List<Callable<List<Long>>> requests = new ArrayList<>();
            for ( int i = 0; i < 16; i++ ) {
                requests.add( () -> sequences.reserve( identity, 5 ) );
            }

            Set<Long> allocated = new HashSet<>();
            for ( Future<List<Long>> result : workers.invokeAll( requests ) ) {
                allocated.addAll( result.get() );
            }
            assertEquals( 80, allocated.size() );
            assertEquals( OptionalLong.of( 81 ), new IdentitySequences( directory ).peek( identity ) );
        } finally {
            workers.shutdownNow();
        }
    }


    @Test
    void descendingCycleAndOptionChangesPreserveNextValue() {
        IdentityColumn identity = IdentityColumn.create( PolyType.SMALLINT, Mode.BY_DEFAULT, null, -2L, -3L, null, true );
        IdentitySequences sequences = new IdentitySequences();
        assertEquals( List.of( -1L, -3L, -1L ), sequences.reserve( identity, 3 ) );

        IdentityColumn changed = sequences.reconfigure( PolyType.SMALLINT, identity, null, -1L, -4L, null, false );
        assertEquals( List.of( -3L, -4L ), sequences.reserve( changed, 2 ) );
        assertEquals( OptionalLong.empty(), sequences.peek( changed ) );

        IdentityColumn expanded = sequences.reconfigure( PolyType.SMALLINT, changed, null, null, -5L, null, true );
        assertEquals( OptionalLong.empty(), sequences.peek( expanded ) );
        assertEquals( -1L, sequences.next( expanded.restart( null ) ) );
    }


    @Test
    void oversizedReservationLeavesSequenceAtItsNextValue() {
        IdentityColumn identity = IdentityColumn.create( PolyType.INTEGER, Mode.BY_DEFAULT, 2L, null, 1L, 3L, false );
        IdentitySequences sequences = new IdentitySequences();

        assertThrows( GenericRuntimeException.class, () -> sequences.verifyCapacity( identity, 3 ) );
        sequences.verifyCapacity( identity, 2 );
        assertThrows( GenericRuntimeException.class, () -> sequences.reserve( identity, 3 ) );
        assertEquals( OptionalLong.of( 2L ), sequences.peek( identity ) );
        assertEquals( List.of( 2L, 3L ), sequences.reserve( identity, 2 ) );
    }


    @Test
    void generatedDefaultRoundTripsThroughCatalogCodec() {
        IdentityColumn identity = IdentityColumn.create( PolyType.INTEGER, Mode.ALWAYS, 7L, 4L, null, 123L, true ).restart( 101L );
        BinarySerializer<LogicalDefaultValue> codec = SerializerFactory.defaultInstance()
                .create( DefiningClassLoader.create(), LogicalDefaultValue.class );
        byte[] bytes = new byte[1024];

        codec.encode( bytes, 0, LogicalDefaultValue.forIdentity( 8, PolyType.INTEGER, identity ) );
        LogicalDefaultValue restored = codec.decode( bytes, 0 );
        assertEquals( identity, restored.identity );
        assertNull( restored.value );
    }

}
