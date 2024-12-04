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

public class IdentifierRegistryTest {

    private IdentifierRegistry registry;


    @BeforeEach
    void setUp() {
        registry = new IdentifierRegistry( 100 );
    }


    @Test
    void testGetEntryIdentifierSequential() {
        long firstId = registry.getEntryIdentifier();
        long secondId = registry.getEntryIdentifier();

        assertEquals( 0, firstId );
        assertEquals( 1, secondId );
    }


    @Test
    void testGetEntryIdentifierUntilOverflow() {
        for ( int i = 0; i < 100; i++ ) {
            registry.getEntryIdentifier();
        }
        Exception exception = assertThrows( IllegalStateException.class, registry::getEntryIdentifier );
        assertEquals( "No identifiers available", exception.getMessage() );
    }


    @Test
    void testReleaseSingleIdentifierBeginning() {
        long firstIdentifier = registry.getEntryIdentifier();
        registry.getEntryIdentifier();

        registry.releaseEntryIdentifiers( Set.of( firstIdentifier ) );
        assertEquals( 0, registry.getEntryIdentifier() );
    }


    @Test
    void testReleaseSingleIdentifierMiddle() {
        for ( int i = 0; i < 25; i++ ) {
            registry.getEntryIdentifier();
        }
        long middleIdentifier = registry.getEntryIdentifier();
        for ( int i = 0; i < 25; i++ ) {
            registry.getEntryIdentifier();
        }
        registry.releaseEntryIdentifiers( Set.of( middleIdentifier ) );
        assertEquals( 25, registry.getEntryIdentifier() );
    }


    @Test
    void testReleaseMultipleIdentifiersConsequtive() {
        Set<Long> identifiers = new HashSet<>();
        for ( int i = 0; i < 20; i++ ) {
            registry.getEntryIdentifier();
        }
        for ( int i = 0; i < 20; i++ ) {
            identifiers.add( registry.getEntryIdentifier() );
        }
        for ( int i = 0; i < 20; i++ ) {
            registry.getEntryIdentifier();
        }
        registry.releaseEntryIdentifiers( identifiers );
        for ( int i = 20; i < 40; i++ ) {
            assertEquals( i, registry.getEntryIdentifier() );
        }
    }


    @Test
    void testReleaseMultipleIdentifiersInterleaved() {
        Set<Long> evenIdentifiers = new HashSet<>();
        Set<Long> oddIdentifiers = new HashSet<>();

        for ( int i = 0; i < 60; i++ ) {
            if ( i % 2 == 0 ) {
                evenIdentifiers.add( registry.getEntryIdentifier() );
                continue;
            }
            oddIdentifiers.add( registry.getEntryIdentifier() );
        }

        registry.releaseEntryIdentifiers( evenIdentifiers );

        for ( int i = 0; i < 30; i++ ) {
            assertEquals( 0, registry.getEntryIdentifier() % 2 );
        }
    }

}
