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

package org.polypheny.db.transaction;

import static org.bson.assertions.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;

public class ConcurrencyTests {

    private static TestHelper testHelper;


    @BeforeAll
    public static void setUpClass() {
        testHelper = TestHelper.getInstance();
    }


    private void setupTables() {
        List.of(
                "CREATE TABLE accounts (id INT PRIMARY KEY, balance INT);",
                "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 100);"
        ).forEach( s -> {
            Transaction transaction = testHelper.getTransaction();
            ConcurrencyTestUtils.executeStatement( s, "sql", transaction, testHelper );
            transaction.commit();
        } );
    }


    private void setupTables2() {
        List.of(
                "CREATE TABLE coordinates (id INT PRIMARY KEY, x INT, y INT);",
                "INSERT INTO coordinates (id, x, y) VALUES (1, 100, 200);"
        ).forEach( s -> {
            Transaction transaction = testHelper.getTransaction();
            ConcurrencyTestUtils.executeStatement( s, "sql", transaction, testHelper );
            transaction.commit();
        } );
    }


    private void dropTables() {
        List.of(
                "DROP TABLE IF EXISTS accounts;"
        ).forEach( s -> {
            Transaction transaction = testHelper.getTransaction();
            ConcurrencyTestUtils.executeStatement( s, "sql", transaction, testHelper );
            transaction.commit();
        } );
    }


    private void dropTables2() {
        List.of(
                "DROP TABLE IF EXISTS coordinates;"
        ).forEach( s -> {
            Transaction transaction = testHelper.getTransaction();
            ConcurrencyTestUtils.executeStatement( s, "sql", transaction, testHelper );
            transaction.commit();
        } );
    }


    private void closeAndIgnore( List<ExecutedContext> result ) {
        result.forEach( e -> e.getIterator().getAllRowsAndClose() );
    }


    private void closeAndIgnore( Future<List<ExecutedContext>> result ) throws ExecutionException, InterruptedException, TimeoutException {
        result.get( 1, TimeUnit.MINUTES ).forEach( e -> e.getIterator().getAllRowsAndClose() );
    }


    @Test
    public void dirtyReadSimple() throws ExecutionException, InterruptedException, TimeoutException {
        setupTables();

        Session session1 = new Session( testHelper );
        Session session2 = new Session( testHelper );

        session1.startTransaction();
        session1.executeStatementIgnoreResult(
                "UPDATE accounts SET balance = 200 WHERE id = 1;",
                "sql"
        );

        session2.startTransaction();
        Future<List<ExecutedContext>> futureResult = session2.executeStatement(
                "SELECT balance FROM accounts WHERE id = 1;",
                "sql"
        );

        session1.rollbackTransaction();

        List<ExecutedContext> results = futureResult.get( 1, TimeUnit.MINUTES );
        assertEquals( 1, results.size() );
        int balance = results.get( 0 ).getIterator().getIterator().next()[0].asInteger().intValue();
        assertEquals( 100, balance );
        closeAndIgnore( results );

        session2.commitTransaction();

        session1.awaitCompletion();
        session2.awaitCompletion();

        dropTables();
    }


    @Test
    public void fuzzyReadSimple() throws ExecutionException, InterruptedException, TimeoutException {
        setupTables();

        Session session1 = new Session( testHelper );
        Session session2 = new Session( testHelper );

        session1.startTransaction();
        Future<List<ExecutedContext>> futureFirstRead = session1.executeStatement(
                "SELECT balance FROM accounts WHERE id = 1;",
                "sql"
        );

        List<ExecutedContext> firstRead = futureFirstRead.get( 1, TimeUnit.MINUTES );
        assertEquals( 1, firstRead.size() );
        int firstBalance = firstRead.get( 0 ).getIterator().getIterator().next()[0].asInteger().intValue();
        assertEquals( 100, firstBalance );
        closeAndIgnore( firstRead );

        session2.startTransaction();
        session2.executeStatementIgnoreResult(
                "UPDATE accounts SET balance = 300 WHERE id = 1;",
                "sql"
        );
        session2.commitTransaction();

        Future<List<ExecutedContext>> futureSecondRead = session1.executeStatement(
                "SELECT balance FROM accounts WHERE id = 1;",
                "sql"
        );

        List<ExecutedContext> secondRead = futureSecondRead.get( 1, TimeUnit.MINUTES );
        assertEquals( 1, secondRead.size() );
        int secondBalance = secondRead.get( 0 ).getIterator().getIterator().next()[0].asInteger().intValue();
        assertEquals( 100, secondBalance );
        closeAndIgnore( secondRead );

        session1.commitTransaction();

        session1.awaitCompletion();
        session2.awaitCompletion();

        dropTables();
    }


    @Test
    public void phantomSimple() throws ExecutionException, InterruptedException, TimeoutException {
        setupTables();

        Session session1 = new Session( testHelper );
        Session session2 = new Session( testHelper );

        session1.startTransaction();
        Future<List<ExecutedContext>> futureFirstRead = session1.executeStatement(
                "SELECT balance FROM accounts WHERE balance > 150;",
                "sql"
        );

        List<ExecutedContext> firstRead = futureFirstRead.get( 1, TimeUnit.MINUTES );
        assertEquals( 1, firstRead.size() );
        assertFalse( firstRead.get( 0 ).getIterator().hasMoreRows() );
        closeAndIgnore( firstRead );

        session2.startTransaction();
        session2.executeStatementIgnoreResult(
                "INSERT INTO accounts (id, balance) VALUES (3, 200);",
                "sql"
        );
        session2.commitTransaction();

        Future<List<ExecutedContext>> futureSecondRead = session1.executeStatement(
                "SELECT balance FROM accounts WHERE balance > 150;",
                "sql"
        );

        List<ExecutedContext> secondRead = futureSecondRead.get( 1, TimeUnit.MINUTES );
        assertEquals( 1, secondRead.size() );
        assertFalse( secondRead.get( 0 ).getIterator().hasMoreRows() );
        closeAndIgnore( secondRead );

        session1.commitTransaction();

        session1.awaitCompletion();
        session2.awaitCompletion();

        dropTables();
    }


    @Test
    public void dirtyWriteSimple() throws InterruptedException, ExecutionException, TimeoutException {
        setupTables();

        Session session1 = new Session( testHelper );
        Session session2 = new Session( testHelper );

        session1.startTransaction();
        session1.executeStatementIgnoreResult(
                "UPDATE accounts SET balance = 250 WHERE id = 1;",
                "sql"
        );

        session1.commitTransaction();

        session2.startTransaction();
        session2.executeStatementIgnoreResult(
                "UPDATE accounts SET balance = 300 WHERE id = 1;",
                "sql"
        );

        session2.commitTransaction();

        session1.awaitCompletion();
        session2.awaitCompletion();

        Session validator = new Session( testHelper );
        validator.startTransaction();
        Future<List<ExecutedContext>> futureValidation = validator.executeStatement(
                "SELECT balance FROM accounts WHERE id = 1;",
                "sql"
        );
        List<ExecutedContext> validation = futureValidation.get( 1, TimeUnit.MINUTES );
        assertEquals( 1, validation.size() );
        int balance = validation.get( 0 ).getIterator().getIterator().next()[0].asInteger().intValue();
        assertEquals( 300, balance );
        closeAndIgnore( validation );

        validator.commitTransaction();
        validator.awaitCompletion();

        dropTables();
    }


    @Test
    public void lostUpdateSimple() throws InterruptedException, ExecutionException, TimeoutException {
        setupTables();

        Session session1 = new Session( testHelper );
        Session session2 = new Session( testHelper );

        session1.startTransaction();
        Future<List<ExecutedContext>> firstSelect = session1.executeStatement(
                "SELECT balance FROM accounts WHERE id = 1;",
                "sql"
        );
        closeAndIgnore( firstSelect ); // done manually as this forces the first statement to be completed before session 2 starts

        session2.startTransaction();
        session2.executeStatementIgnoreResult(
                "UPDATE accounts SET balance = 200 WHERE id = 1;",
                "sql"
        );
        session2.commitTransaction();

        session1.executeStatementIgnoreResult(
                "UPDATE accounts SET balance = 300 WHERE id = 1;",
                "sql"
        );
        session1.commitTransaction();

        session1.awaitCompletion();
        session2.awaitCompletion();

        Session validator = new Session( testHelper );
        validator.startTransaction();
        Future<List<ExecutedContext>> futureValidation = validator.executeStatement(
                "SELECT balance FROM accounts WHERE id = 1;",
                "sql"
        );
        List<ExecutedContext> validation = futureValidation.get( 1, TimeUnit.MINUTES );
        assertEquals( 1, validation.size() );
        int balance = validation.get( 0 ).getIterator().getIterator().next()[0].asInteger().intValue();
        assertEquals( 200, balance );
        closeAndIgnore( validation );

        validator.commitTransaction();
        validator.awaitCompletion();

        dropTables();
    }


    @Test
    public void readSkewSimple() throws ExecutionException, InterruptedException, TimeoutException {
        setupTables2();

        Session session1 = new Session( testHelper );
        Session session2 = new Session( testHelper );

        session1.startTransaction();
        Future<List<ExecutedContext>> futureReadX = session1.executeStatement(
                "SELECT x FROM coordinates WHERE id = 1;",
                "sql"
        );

        List<ExecutedContext> readX = futureReadX.get( 1, TimeUnit.MINUTES );
        assertEquals( 1, readX.size() );
        int xCoordinate = readX.get( 0 ).getIterator().getIterator().next()[0].asInteger().intValue();
        assertEquals( 100, xCoordinate );
        closeAndIgnore( readX );

        session2.startTransaction();
        session2.executeStatementIgnoreResult(
                "UPDATE coordinates SET x = 300, y = 400 WHERE id = 1;",
                "sql"
        );
        session2.commitTransaction();

        Future<List<ExecutedContext>> futureReadY = session1.executeStatement(
                "SELECT y FROM coordinates WHERE id = 1;",
                "sql"
        );

        List<ExecutedContext> readY = futureReadY.get( 1, TimeUnit.MINUTES );
        assertEquals( 1, readY.size() );
        int yCoordinate = readY.get( 0 ).getIterator().getIterator().next()[0].asInteger().intValue();
        assertEquals( 200, yCoordinate );
        closeAndIgnore( readY );

        session1.commitTransaction();

        session1.awaitCompletion();
        session2.awaitCompletion();

        dropTables2();
    }


    @Test
    public void writeSkewSimple() throws ExecutionException, InterruptedException, TimeoutException {
        setupTables();

        Session session1 = new Session( testHelper );
        Session session2 = new Session( testHelper );

        session1.startTransaction();
        Future<List<ExecutedContext>> futureRead1 = session1.executeStatement(
                "SELECT balance FROM accounts WHERE id = 1 OR id = 2;",
                "sql"
        );
        closeAndIgnore( futureRead1 );

        session2.startTransaction();
        Future<List<ExecutedContext>> futureRead2 = session2.executeStatement(
                "SELECT balance FROM accounts WHERE id = 1 OR id = 2;",
                "sql"
        );
        closeAndIgnore( futureRead2 );

        session1.executeStatementIgnoreResult(
                "UPDATE accounts SET balance = balance - 200 WHERE id = 1;",
                "sql"
        );
        Future<List<ExecutedContext>> futureCheck1 = session1.executeStatement(
                "SELECT SUM(balance) AS total_balance FROM accounts;",
                "sql"
        );

        session2.executeStatementIgnoreResult(
                "UPDATE accounts SET balance = balance - 200 WHERE id = 2;",
                "sql"
        );
        Future<List<ExecutedContext>> futureCheck2 = session2.executeStatement(
                "SELECT SUM(balance) AS total_balance FROM accounts;",
                "sql"
        );


        List<ExecutedContext> result1 = futureCheck1.get( 1, TimeUnit.HOURS );
        int totalBalance1 = result1.get( 0 ).getIterator().getIterator().next()[0].asInteger().intValue();
        if ( totalBalance1 < 0 ) {
            session1.rollbackTransaction();
        } else {
            session1.commitTransaction();
        }

        List<ExecutedContext> result2 = futureCheck2.get( 1, TimeUnit.HOURS );
        int totalBalance2 = result2.get( 0 ).getIterator().getIterator().next()[0].asInteger().intValue();
        if ( totalBalance2 < 0 ) {
            session2.rollbackTransaction();
        } else {
            session2.commitTransaction();
        }

        session1.awaitCompletion();
        session2.awaitCompletion();

        Session validator = new Session( testHelper );
        validator.startTransaction();
        Future<List<ExecutedContext>> futureValidation = validator.executeStatement(
                "SELECT SUM(balance) AS total_balance FROM accounts;",
                "sql"
        );
        List<ExecutedContext> validation = futureValidation.get( 1, TimeUnit.HOURS );
        int finalTotalBalance = validation.get( 0 ).getIterator().getIterator().next()[0].asInteger().intValue();
        assertTrue( "Total balance should not be negative", finalTotalBalance >= 0 );
        closeAndIgnore( validation );

        validator.commitTransaction();
        validator.awaitCompletion();

        dropTables();
    }

}
