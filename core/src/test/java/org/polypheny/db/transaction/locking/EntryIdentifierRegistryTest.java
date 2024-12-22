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

package org.polypheny.db.transaction.locking;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EntryIdentifierRegistryTest {

    private EntryIdentifierRegistry registry;


    @BeforeEach
    void setUp() {
        registry = new EntryIdentifierRegistry( 100 );
    }


    @Test
    void testGetNextEntryIdentifierSequential() {
        long firstId = registry.getNextEntryIdentifier();
        long secondId = registry.getNextEntryIdentifier();

        assertEquals( 1, firstId );
        assertEquals( 2, secondId );
    }


    @Test
    void testGetNextEntryIdentifierUntilOverflow() {
        for ( int i = 0; i < 99; i++ ) {
            registry.getNextEntryIdentifier();
        }
        Exception exception = assertThrows( IllegalStateException.class, registry::getNextEntryIdentifier );
        assertEquals( "No identifiers available", exception.getMessage() );
    }


    @Test
    void testReleaseSingleIdentifierBeginning() {
        long firstIdentifier = registry.getNextEntryIdentifier();
        registry.getNextEntryIdentifier();

        registry.releaseEntryIdentifiers( Set.of( firstIdentifier ) );
        assertEquals( 1, registry.getNextEntryIdentifier() );
    }


    @Test
    void testReleaseSingleIdentifierMiddle() {
        for ( int i = 0; i < 25; i++ ) {
            registry.getNextEntryIdentifier();
        }
        long middleIdentifier = registry.getNextEntryIdentifier();
        for ( int i = 0; i < 25; i++ ) {
            registry.getNextEntryIdentifier();
        }
        registry.releaseEntryIdentifiers( Set.of( middleIdentifier ) );
        assertEquals( 26, registry.getNextEntryIdentifier() );
    }


    @Test
    void testReleaseMultipleIdentifiersConsequtive() {
        Set<Long> identifiers = new HashSet<>();
        for ( int i = 0; i < 20; i++ ) {
            registry.getNextEntryIdentifier();
        }
        for ( int i = 0; i < 20; i++ ) {
            identifiers.add( registry.getNextEntryIdentifier() );
        }
        for ( int i = 0; i < 20; i++ ) {
            registry.getNextEntryIdentifier();
        }
        registry.releaseEntryIdentifiers( identifiers );
        for ( int i = 21; i < 40; i++ ) {
            assertEquals( i, registry.getNextEntryIdentifier() );
        }
    }


    @Test
    void testReleaseMultipleIdentifiersInterleaved() {
        Set<Long> evenIdentifiers = new HashSet<>();
        Set<Long> oddIdentifiers = new HashSet<>();

        for ( int i = 0; i < 60; i++ ) {
            long id = registry.getNextEntryIdentifier();
            if ( id % 2 == 0 ) {
                evenIdentifiers.add( id );
                continue;
            }
            oddIdentifiers.add( id );
        }

        registry.releaseEntryIdentifiers( evenIdentifiers );

        for ( int i = 0; i < 30; i++ ) {
            long id = registry.getNextEntryIdentifier();
            assertEquals( 0, id % 2);
        }
    }

}
