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

package org.polypheny.db.catalog;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.polypheny.db.cypher.CypherTestTemplate.execute;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;

public class AllocCatalogTest {

    public static String name = "allocCatalogTest";
    private TestHelper helper;


    @BeforeEach
    public void buildUp() {
        helper = TestHelper.getInstance();
    }


    @AfterEach
    public void tearDown() {
        execute( format( "DROP DATABASE %s IF EXISTS", name ) );
        helper.checkAllTrxClosed();
    }


    @Test
    public void removeGraphAllocPlacementsTest() {
        int initialAllocations = Catalog.snapshot().alloc().getAllocations().size();
        int initialPlacements = Catalog.snapshot().alloc().getPlacements().size();
        int initialPartitions = Catalog.snapshot().alloc().getPartitions().size();

        execute( format( "CREATE DATABASE %s", name ) );

        execute( format( "DROP DATABASE %s", name ) );

        assertAllocationStructure( initialAllocations, initialPlacements, initialPartitions );

    }


    @Test
    public void removeGraphAllocWithDataPlacementsTest() {
        int initialAllocations = Catalog.snapshot().alloc().getAllocations().size();
        int initialPlacements = Catalog.snapshot().alloc().getPlacements().size();
        int initialPartitions = Catalog.snapshot().alloc().getPartitions().size();

        execute( format( "CREATE DATABASE %s", name ) );

        execute( "CREATE (n:TEST {name:\"test\"})", name );

        execute( format( "DROP DATABASE %s", name ) );

        assertAllocationStructure( initialAllocations, initialPlacements, initialPartitions );

    }


    @Test
    public void overwriteGraphTest() {
        int initialAllocations = Catalog.snapshot().alloc().getAllocations().size();
        int initialPlacements = Catalog.snapshot().alloc().getPlacements().size();
        int initialPartitions = Catalog.snapshot().alloc().getPartitions().size();

        execute( format( "CREATE DATABASE %s", name ) );

        execute( format( "CREATE (n:TEST {name:\"test\"})" ), name );

        execute( format( "DROP DATABASE %s", name ) );

        execute( format( "CREATE DATABASE %s", name ) );

        execute( format( "CREATE (n:TEST {name:\"test\"})" ), name );

        execute( format( "DROP DATABASE %s", name ) );

        assertAllocationStructure( initialAllocations, initialPlacements, initialPartitions );

    }


    private static void assertAllocationStructure( int initialAllocations, int initialPlacements, int initialPartitions ) {
        assertEquals( initialAllocations, Catalog.snapshot().alloc().getAllocations().size() );
        assertEquals( initialPlacements, Catalog.snapshot().alloc().getPlacements().size() );
        assertEquals( initialPartitions, Catalog.snapshot().alloc().getPartitions().size() );
    }


}
