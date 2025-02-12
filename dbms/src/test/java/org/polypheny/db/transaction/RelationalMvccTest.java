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

package org.polypheny.db.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.type.entity.PolyValue;

public class RelationalMvccTest {

    private static TestHelper testHelper;
    private static final String NAMESPACE = "relMvccTest";

    private static final String SELECT_STAR = "SELECT * FROM " + NAMESPACE + ".Users;";
    private static final String INSERT_1 = "INSERT INTO " + NAMESPACE + ".Users (UserID, UUID) VALUES (1, 10), (2, 20); ";
    private static final String INSERT_2 = "INSERT INTO " + NAMESPACE + ".Users (UserID, UUID) VALUES (3, 30), (4, 40); ";
    private static final String UPDATE_1 = "UPDATE " + NAMESPACE + ".Users SET UUID = 100 WHERE UserID = 2;";


    @BeforeAll
    public static void setUpClass() {
        testHelper = TestHelper.getInstance();
    }


    private void setup() {
        Transaction transaction = testHelper.getTransaction();
        String createNamespaceStatement = String.format( "CREATE RELATIONAL NAMESPACE %s CONCURRENCY MVCC;", NAMESPACE );
        ConcurrencyTestUtils.executeStatement( createNamespaceStatement, "sql", transaction, testHelper );

        transaction = testHelper.getTransaction();
        String createTableStatement = String.format( "CREATE TABLE %s.Users (UserID INT PRIMARY KEY, UUID INT);", NAMESPACE );
        ConcurrencyTestUtils.executeStatement( createTableStatement, "sql", NAMESPACE, transaction, testHelper );
    }


    private void teardown() {
        Transaction transaction = testHelper.getTransaction();
        String dropTable = String.format( "DROP TABLE IF EXISTS %s.Users", NAMESPACE );
        ConcurrencyTestUtils.executeStatement( dropTable, "sql", NAMESPACE, transaction, testHelper );

        transaction = testHelper.getTransaction();
        String dropNamespace = "DROP NAMESPACE IF EXISTS " + NAMESPACE;
        ConcurrencyTestUtils.executeStatement( dropNamespace, "sql", transaction, testHelper );
    }


    private void closeAndIgnore( List<ExecutedContext> result ) {
        result.forEach( e -> e.getIterator().getAllRowsAndClose() );
    }


    @Test
    public void selectTest() {
        setup();
        Session session1 = new Session( testHelper );
        session1.startTransaction();

        session1.executeStatement( INSERT_1, "sql", NAMESPACE );

        // s2 sees own insert as uncommitted
        List<ExecutedContext> results = session1.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        List<List<PolyValue>> data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() < 0 );
        } );

        session1.commitTransaction();

        teardown();
    }


    @Test
    public void singleSessionInsert() {
        List<ExecutedContext> results;
        List<List<PolyValue>> data;

        setup();

        Session session1 = new Session( testHelper );
        Session session2 = new Session( testHelper );

        session1.startTransaction();
        session2.startTransaction();

        // s1 inserts data
        session1.executeStatement( INSERT_1, "sql", NAMESPACE );

        // s1 sees own insert as uncommitted
        results = session1.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() < 0 );
            int user_id = row.get( 2 ).asInteger().intValue();
            assertTrue( user_id == 1 || user_id == 2 );
        } );

        // s2 cannot see the insert performed by s1
        results = session2.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        assertFalse( results.get( 0 ).getIterator().hasMoreRows() );
        closeAndIgnore( results );

        session1.commitTransaction();

        // even after commit s2 cannot see the changes due to it's own snapshot
        results = session2.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        assertFalse( results.get( 0 ).getIterator().hasMoreRows() );
        closeAndIgnore( results );

        session2.commitTransaction();
        session2.startTransaction();

        // after starting a new transaction, s2 sees the data inserted by s1
        results = session2.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() > 0 );
            int user_id = row.get( 2 ).asInteger().intValue();
            assertTrue( user_id == 1 || user_id == 2 );
        } );

        session2.commitTransaction();

        teardown();
    }


    @Test
    public void multiSessionInsert() {
        List<List<PolyValue>> data;
        List<ExecutedContext> results;

        setup();

        Session session1 = new Session( testHelper );
        Session session2 = new Session( testHelper );

        session1.startTransaction();
        session2.startTransaction();

        // s1 inserts data
        session1.executeStatement( INSERT_1, "sql", NAMESPACE );

        // s2 inserts data
        session2.executeStatement( INSERT_2, "sql", NAMESPACE );

        // s1 sees own insert as uncommitted
        results = session1.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() < 0 );
            int user_id = row.get( 2 ).asInteger().intValue();
            assertTrue( user_id == 1 || user_id == 2 );
        } );

        // s2 sees own insert as uncommitted
        results = session2.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() < 0 );
            int user_id = row.get( 2 ).asInteger().intValue();
            assertTrue( user_id == 3 || user_id == 4 );
        } );

        session1.commitTransaction();
        session1.startTransaction();

        // s1 sees own insert as committed
        results = session1.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() > 0 );
            int user_id = row.get( 2 ).asInteger().intValue();
            assertTrue( user_id == 1 || user_id == 2 );
        } );

        // s2 sees own insert as uncommitted
        results = session2.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() < 0 );
            int user_id = row.get( 2 ).asInteger().intValue();
            assertTrue( user_id == 3 || user_id == 4 );
        } );

        session2.commitTransaction();
        session2.startTransaction();

        // s2 sees all inserts as committed
        results = session2.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 4, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() > 0 );
            int user_id = row.get( 2 ).asInteger().intValue();
            assertTrue( user_id > 0 && user_id < 5 );
        } );

        // s1 does not yet see the inserts of s2 due to the current snapshot
        results = session1.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() > 0 );
            int user_id = row.get( 2 ).asInteger().intValue();
            assertTrue( user_id == 1 || user_id == 2 );
        } );

        session1.commitTransaction();
        session1.startTransaction();

        // s1 sees all inserts as committed
        results = session1.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 4, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() > 0 );
            int user_id = row.get( 2 ).asInteger().intValue();
            assertTrue( user_id > 0 && user_id < 5 );
        } );

        session1.commitTransaction();
        session2.commitTransaction();

        teardown();
    }


    @Test
    public void updateCommittedWithConstant() {
        List<ExecutedContext> results;
        List<List<PolyValue>> data;

        setup();

        Session session1 = new Session( testHelper );

        session1.startTransaction();
        session1.executeStatement( INSERT_1, "sql", NAMESPACE );
        session1.commitTransaction();

        session1.startTransaction();
        session1.executeStatement( UPDATE_1, "sql", NAMESPACE );

        // s1 sees the updated uncommitted version
        results = session1.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            if ( row.get( 1 ).asLong().longValue() > 0 ) {
                assertEquals( 1, row.get( 2 ).asInteger().intValue() );
                assertEquals( 10, row.get( 3 ).asInteger().intValue() );
            } else {
                assertEquals( 2, row.get( 2 ).asInteger().intValue() );
                assertEquals( 100, row.get( 3 ).asInteger().intValue() );
            }
        } );

        session1.commitTransaction();
        session1.startTransaction();

        // s1 sees the updated uncommitted version
        results = session1.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() > 0 );
            if ( row.get( 2 ).asInteger().intValue() == 1 ) {
                assertEquals( 10, row.get( 3 ).asInteger().intValue() );
            } else {
                assertEquals( 100, row.get( 3 ).asInteger().intValue() );
            }
        } );

        session1.commitTransaction();
    }


    @Test
    public void updateUncommittedWithConstant() {
        List<ExecutedContext> results;
        List<List<PolyValue>> data;

        setup();

        Session session1 = new Session( testHelper );

        session1.startTransaction();
        session1.executeStatement( INSERT_1, "sql", NAMESPACE );
        session1.executeStatement( UPDATE_1, "sql", NAMESPACE );

        // s1 sees the updated uncommitted version
        results = session1.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            if ( row.get( 1 ).asLong().longValue() > 0 ) {
                assertEquals( 1, row.get( 2 ).asInteger().intValue() );
                assertEquals( 10, row.get( 3 ).asInteger().intValue() );
            } else {
                assertEquals( 2, row.get( 2 ).asInteger().intValue() );
                assertEquals( 100, row.get( 3 ).asInteger().intValue() );
            }
        } );

        session1.commitTransaction();
        session1.startTransaction();

        // s1 sees the updated uncommitted version
        results = session1.executeStatement( SELECT_STAR, "sql", NAMESPACE );
        assertEquals( 1, results.size() );
        data = results.get( 0 ).getIterator().getAllRowsAndClose();
        assertEquals( 2, data.size() );
        data.forEach( row -> {
            assertTrue( row.get( 1 ).asLong().longValue() > 0 );
            if ( row.get( 2 ).asInteger().intValue() == 1 ) {
                assertEquals( 10, row.get( 3 ).asInteger().intValue() );
            } else {
                assertEquals( 100, row.get( 3 ).asInteger().intValue() );
            }
        } );

        session1.commitTransaction();
    }

}
