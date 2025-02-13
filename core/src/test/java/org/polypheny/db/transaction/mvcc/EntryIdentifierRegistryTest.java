/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.transaction.mvcc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.transaction.mvcc.EntryIdentifierRegistry;

public class EntryIdentifierRegistryTest {

    private EntryIdentifierRegistry registry;


    @BeforeEach
    void setUp() {
        Entity dummy = new LogicalTable( 0, "dummy", 0, EntityType.ENTITY, 0L, true );
        registry = new EntryIdentifierRegistry( dummy, 100 );

    }


    @Test
    void testGetNextEntryIdentifierAsLongSequential() {
        long firstId = registry.getNextEntryIdentifier().entryIdentifier();
        long secondId = registry.getNextEntryIdentifier().entryIdentifier();

        assertEquals( 1, firstId );
        assertEquals( 2, secondId );
    }


    @Test
    void testGetNextEntryIdentifierAsLongUntilOverflow() {
        for ( int i = 0; i < 99; i++ ) {
            registry.getNextEntryIdentifier();
        }
        Exception exception = assertThrows( IllegalStateException.class, () -> {registry.getNextEntryIdentifier();} );
        assertEquals( "No identifiers available", exception.getMessage() );
    }


    @Test
    void testReleaseSingleIdentifierBeginning() {
        long firstIdentifier = registry.getNextEntryIdentifier().entryIdentifier();
        registry.getNextEntryIdentifier();

        registry.releaseEntryIdentifiers( Set.of( firstIdentifier ) );
        assertEquals( 1, registry.getNextEntryIdentifier().entryIdentifier() );
    }


    @Test
    void testReleaseSingleIdentifierMiddle() {
        for ( int i = 0; i < 25; i++ ) {
            registry.getNextEntryIdentifier();
        }
        long middleIdentifier = registry.getNextEntryIdentifier().entryIdentifier();
        for ( int i = 0; i < 25; i++ ) {
            registry.getNextEntryIdentifier();
        }
        registry.releaseEntryIdentifiers( Set.of( middleIdentifier ) );
        assertEquals( 26, registry.getNextEntryIdentifier().entryIdentifier() );
    }


    @Test
    void testReleaseMultipleIdentifiersConsequtive() {
        Set<Long> identifiers = new HashSet<>();
        for ( int i = 0; i < 20; i++ ) {
            registry.getNextEntryIdentifier();
        }
        for ( int i = 0; i < 20; i++ ) {
            identifiers.add( registry.getNextEntryIdentifier().entryIdentifier() );
        }
        for ( int i = 0; i < 20; i++ ) {
            registry.getNextEntryIdentifier();
        }
        registry.releaseEntryIdentifiers( identifiers );
        for ( int i = 21; i < 40; i++ ) {
            assertEquals( i, registry.getNextEntryIdentifier().entryIdentifier() );
        }
    }


    @Test
    void testReleaseMultipleIdentifiersInterleaved() {
        Set<Long> evenIdentifiers = new HashSet<>();
        Set<Long> oddIdentifiers = new HashSet<>();

        for ( int i = 0; i < 60; i++ ) {
            long id = registry.getNextEntryIdentifier().entryIdentifier();
            if ( id % 2 == 0 ) {
                evenIdentifiers.add( id );
                continue;
            }
            oddIdentifiers.add( id );
        }

        registry.releaseEntryIdentifiers( evenIdentifiers );

        for ( int i = 0; i < 30; i++ ) {
            long id = registry.getNextEntryIdentifier().entryIdentifier();
            assertEquals( 0, id % 2);
        }
    }

}
