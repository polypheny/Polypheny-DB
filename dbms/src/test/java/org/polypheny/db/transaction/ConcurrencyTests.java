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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;

public class ConcurrencyTests {

    private static TestHelper testHelper;


    @BeforeAll
    public static void setUpClass() {
        testHelper = TestHelper.getInstance();
    }


    private void setupTables() {
        List.of(
                "CREATE TABLE a (i INT PRIMARY KEY);",
                "CREATE TABLE b (a_id INT PRIMARY KEY, a_ref INT NULL);",
                "INSERT INTO a (i) VALUES (0), (1), (2), (3);",
                "INSERT INTO b (a_id) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8);"
        ).forEach( s -> {
            Transaction transaction = testHelper.getTransaction();
            ConcurrencyTestUtils.executeStatement( s, "sql", transaction, testHelper );
            transaction.commit();
        } );
    }


    private void dropTables() {
        List.of(
                "DROP TABLE IF EXISTS b;",
                "DROP TABLE IF EXISTS a;"
        ).forEach( s -> {
            Transaction transaction = testHelper.getTransaction();
            ConcurrencyTestUtils.executeStatement( s, "sql", transaction, testHelper );
            transaction.commit();
        } );
    }


    /**
     * The test case is inspired by the following one of PostgreSQL:
     * https://github.com/postgres/postgres/blob/master/src/test/isolation/specs/alter-table-1.spec
     */

    @Test
    public void alterTableAndReadWrites() {
        Session session1 = new Session( testHelper );
        Session session2 = new Session( testHelper );
        Set<Session> sessions = Set.of( session1, session2 );

        Map<String, Runnable> operations = new HashMap<>();
        
        operations.put( "s1_1", session1::startTransaction );
        operations.put( "at1_1", () -> session1.executeStatement(
                "ALTER TABLE b ADD CONSTRAINT bfk FOREIGN KEY (a_ref) REFERENCES a (i);",
                "sql"
        ) );
        operations.put( "c1_1", session1::commitTransaction );

        operations.put( "s1_2", session1::startTransaction );
        operations.put( "at1_2", () -> session1.executeStatement(
                "ALTER TABLE b ADD INDEX bid ON a_ref;",
                "sql"
        ) );
        operations.put( "c1_2", session1::commitTransaction );

        operations.put( "s2", session2::startTransaction );
        operations.put( "s2 s2 r2_1", () -> session2.executeStatement(
                "SELECT * FROM b WHERE a_id = 1 LIMIT 1;",
                "sql"
        ) );
        operations.put( "w2", () -> session2.executeStatement(
                "INSERT INTO b (a_id, a_ref) VALUES (0, NULL);",
                "sql"
        ) );
        operations.put( "r2_2", () -> session2.executeStatement(
                "SELECT * FROM b WHERE a_id = 3 LIMIT 3;",
                "sql"
        ) );
        operations.put( "c2", session2::commitTransaction );

        List<String> executions = List.of(
                "s1_1 at1_1 c1_1 s1_2 at1_2 c1_2 s2 r2_1 w2 r2_2 c2",
                "s1_1 at1_1 c1_1 s1_2 at1_2 s2 r2_1 c1_2 w2 r2_2 c2",
                "s1_1 at1_1 c1_1 s1_2 at1_2 s2 r2_1 w2 c1_2 r2_2 c2",
                "s1_1 at1_1 c1_1 s1_2 at1_2 s2 r2_1 w2 r2_2 c1_2 c2",
                "s1_1 at1_1 c1_1 s1_2 at1_2 s2 r2_1 w2 r2_2 c2 c1_2",
                "s1_1 at1_1 c1_1 s1_2 s2 r2_1 at1_2 c1_2 w2 r2_2 c2",
                "s1_1 at1_1 c1_1 s1_2 s2 r2_1 at1_2 w2 c1_2 r2_2 c2",
                "s1_1 at1_1 c1_1 s1_2 s2 r2_1 at1_2 w2 r2_2 c1_2 c2",
                "s1_1 at1_1 c1_1 s1_2 s2 r2_1 at1_2 w2 r2_2 c2 c1_2",
                "s1_1 at1_1 c1_1 s1_2 s2 r2_1 w2 at1_2 c1_2 r2_2 c2",
                "s1_1 at1_1 c1_1 s1_2 s2 r2_1 w2 at1_2 r2_2 c1_2 c2",
                "s1_1 at1_1 c1_1 s1_2 s2 r2_1 w2 at1_2 r2_2 c2 c1_2",
                "s1_1 at1_1 c1_1 s1_2 s2 r2_1 w2 r2_2 at1_2 c1_2 c2",
                "s1_1 at1_1 c1_1 s1_2 s2 r2_1 w2 r2_2 at1_2 c2 c1_2",
                "s1_1 at1_1 c1_1 s1_2 s2 r2_1 w2 r2_2 c2 at1_2 c1_2",
                "s1_1 at1_1 c1_1 s2 r2_1 s1_2 at1_2 c1_2 w2 r2_2 c2",
                "s1_1 at1_1 c1_1 s2 r2_1 s1_2 at1_2 w2 c1_2 r2_2 c2",
                "s1_1 at1_1 c1_1 s2 r2_1 s1_2 at1_2 w2 r2_2 c1_2 c2",
                "s1_1 at1_1 c1_1 s2 r2_1 s1_2 at1_2 w2 r2_2 c2 c1_2",
                "s1_1 at1_1 c1_1 s2 r2_1 s1_2 w2 at1_2 c1_2 r2_2 c2",
                "s1_1 at1_1 c1_1 s2 r2_1 s1_2 w2 at1_2 r2_2 c1_2 c2",
                "s1_1 at1_1 c1_1 s2 r2_1 s1_2 w2 at1_2 r2_2 c2 c1_2",
                "s1_1 at1_1 c1_1 s2 r2_1 s1_2 w2 r2_2 at1_2 c1_2 c2",
                "s1_1 at1_1 c1_1 s2 r2_1 s1_2 w2 r2_2 at1_2 c2 c1_2",
                "s1_1 at1_1 c1_1 s2 r2_1 s1_2 w2 r2_2 c2 at1_2 c1_2",
                "s1_1 at1_1 c1_1 s2 r2_1 w2 s1_2 at1_2 c1_2 r2_2 c2",
                "s1_1 at1_1 c1_1 s2 r2_1 w2 s1_2 at1_2 r2_2 c1_2 c2",
                "s1_1 at1_1 c1_1 s2 r2_1 w2 s1_2 at1_2 r2_2 c2 c1_2",
                "s1_1 at1_1 c1_1 s2 r2_1 w2 s1_2 r2_2 at1_2 c1_2 c2",
                "s1_1 at1_1 c1_1 s2 r2_1 w2 s1_2 r2_2 at1_2 c2 c1_2",
                "s1_1 at1_1 c1_1 s2 r2_1 w2 s1_2 r2_2 c2 at1_2 c1_2",
                "s1_1 at1_1 c1_1 s2 r2_1 w2 r2_2 s1_2 at1_2 c1_2 c2",
                "s1_1 at1_1 c1_1 s2 r2_1 w2 r2_2 s1_2 at1_2 c2 c1_2",
                "s1_1 at1_1 c1_1 s2 r2_1 w2 r2_2 s1_2 c2 at1_2 c1_2",
                "s1_1 at1_1 c1_1 s2 r2_1 w2 r2_2 c2 s1_2 at1_2 c1_2",
                "s1_1 at1_1 s2 r2_1 c1_1 s1_2 at1_2 c1_2 w2 r2_2 c2",
                "s1_1 at1_1 s2 r2_1 c1_1 s1_2 at1_2 w2 c1_2 r2_2 c2",
                "s1_1 at1_1 s2 r2_1 c1_1 s1_2 at1_2 w2 r2_2 c1_2 c2",
                "s1_1 at1_1 s2 r2_1 c1_1 s1_2 at1_2 w2 r2_2 c2 c1_2",
                "s1_1 at1_1 s2 r2_1 c1_1 s1_2 w2 at1_2 c1_2 r2_2 c2",
                "s1_1 at1_1 s2 r2_1 c1_1 s1_2 w2 at1_2 r2_2 c1_2 c2",
                "s1_1 at1_1 s2 r2_1 c1_1 s1_2 w2 at1_2 r2_2 c2 c1_2",
                "s1_1 at1_1 s2 r2_1 c1_1 s1_2 w2 r2_2 at1_2 c1_2 c2",
                "s1_1 at1_1 s2 r2_1 c1_1 s1_2 w2 r2_2 at1_2 c2 c1_2",
                "s1_1 at1_1 s2 r2_1 c1_1 s1_2 w2 r2_2 c2 at1_2 c1_2",
                "s1_1 at1_1 s2 r2_1 c1_1 w2 s1_2 at1_2 c1_2 r2_2 c2",
                "s1_1 at1_1 s2 r2_1 c1_1 w2 s1_2 at1_2 r2_2 c1_2 c2",
                "s1_1 at1_1 s2 r2_1 c1_1 w2 s1_2 at1_2 r2_2 c2 c1_2",
                "s1_1 at1_1 s2 r2_1 c1_1 w2 s1_2 r2_2 at1_2 c1_2 c2",
                "s1_1 at1_1 s2 r2_1 c1_1 w2 s1_2 r2_2 at1_2 c2 c1_2",
                "s1_1 at1_1 s2 r2_1 c1_1 w2 s1_2 r2_2 c2 at1_2 c1_2",
                "s1_1 at1_1 s2 r2_1 c1_1 w2 r2_2 s1_2 at1_2 c1_2 c2",
                "s1_1 at1_1 s2 r2_1 c1_1 w2 r2_2 s1_2 at1_2 c2 c1_2",
                "s1_1 at1_1 s2 r2_1 c1_1 w2 r2_2 s1_2 c2 at1_2 c1_2",
                "s1_1 at1_1 s2 r2_1 c1_1 w2 r2_2 c2 s1_2 at1_2 c1_2",
                "s1_1 at1_1 s2 r2_1 w2 c1_1 s1_2 at1_2 c1_2 r2_2 c2",
                "s1_1 at1_1 s2 r2_1 w2 c1_1 s1_2 at1_2 r2_2 c1_2 c2",
                "s1_1 at1_1 s2 r2_1 w2 c1_1 s1_2 at1_2 r2_2 c2 c1_2",
                "s1_1 at1_1 s2 r2_1 w2 c1_1 s1_2 r2_2 at1_2 c1_2 c2",
                "s1_1 at1_1 s2 r2_1 w2 c1_1 s1_2 r2_2 at1_2 c2 c1_2",
                "s1_1 at1_1 s2 r2_1 w2 c1_1 s1_2 r2_2 c2 at1_2 c1_2",
                "s1_1 at1_1 s2 r2_1 w2 c1_1 r2_2 s1_2 at1_2 c1_2 c2",
                "s1_1 at1_1 s2 r2_1 w2 c1_1 r2_2 s1_2 at1_2 c2 c1_2",
                "s1_1 at1_1 s2 r2_1 w2 c1_1 r2_2 s1_2 c2 at1_2 c1_2",
                "s1_1 at1_1 s2 r2_1 w2 c1_1 r2_2 c2 s1_2 at1_2 c1_2",
                "s1_1 s2 r2_1 at1_1 c1_1 s1_2 at1_2 c1_2 w2 r2_2 c2",
                "s1_1 s2 r2_1 at1_1 c1_1 s1_2 at1_2 w2 c1_2 r2_2 c2",
                "s1_1 s2 r2_1 at1_1 c1_1 s1_2 at1_2 w2 r2_2 c1_2 c2",
                "s1_1 s2 r2_1 at1_1 c1_1 s1_2 at1_2 w2 r2_2 c2 c1_2",
                "s1_1 s2 r2_1 at1_1 c1_1 s1_2 w2 at1_2 c1_2 r2_2 c2",
                "s1_1 s2 r2_1 at1_1 c1_1 s1_2 w2 at1_2 r2_2 c1_2 c2",
                "s1_1 s2 r2_1 at1_1 c1_1 s1_2 w2 at1_2 r2_2 c2 c1_2",
                "s1_1 s2 r2_1 at1_1 c1_1 s1_2 w2 r2_2 at1_2 c1_2 c2",
                "s1_1 s2 r2_1 at1_1 c1_1 s1_2 w2 r2_2 at1_2 c2 c1_2",
                "s1_1 s2 r2_1 at1_1 c1_1 s1_2 w2 r2_2 c2 at1_2 c1_2",
                "s1_1 s2 r2_1 at1_1 c1_1 w2 s1_2 at1_2 c1_2 r2_2 c2",
                "s1_1 s2 r2_1 at1_1 c1_1 w2 s1_2 at1_2 r2_2 c1_2 c2",
                "s1_1 s2 r2_1 at1_1 c1_1 w2 s1_2 at1_2 r2_2 c2 c1_2",
                "s1_1 s2 r2_1 at1_1 c1_1 w2 s1_2 r2_2 at1_2 c1_2 c2",
                "s1_1 s2 r2_1 at1_1 c1_1 w2 s1_2 r2_2 at1_2 c2 c1_2",
                "s1_1 s2 r2_1 at1_1 c1_1 w2 s1_2 r2_2 c2 at1_2 c1_2",
                "s1_1 s2 r2_1 at1_1 c1_1 w2 r2_2 s1_2 at1_2 c1_2 c2",
                "s1_1 s2 r2_1 at1_1 c1_1 w2 r2_2 s1_2 at1_2 c2 c1_2",
                "s1_1 s2 r2_1 at1_1 c1_1 w2 r2_2 s1_2 c2 at1_2 c1_2",
                "s1_1 s2 r2_1 at1_1 c1_1 w2 r2_2 c2 s1_2 at1_2 c1_2",
                "s1_1 s2 r2_1 at1_1 w2 c1_1 s1_2 at1_2 c1_2 r2_2 c2",
                "s1_1 s2 r2_1 at1_1 w2 c1_1 s1_2 at1_2 r2_2 c1_2 c2",
                "s1_1 s2 r2_1 at1_1 w2 c1_1 s1_2 at1_2 r2_2 c2 c1_2",
                "s1_1 s2 r2_1 at1_1 w2 c1_1 s1_2 r2_2 at1_2 c1_2 c2",
                "s1_1 s2 r2_1 at1_1 w2 c1_1 s1_2 r2_2 at1_2 c2 c1_2",
                "s1_1 s2 r2_1 at1_1 w2 c1_1 s1_2 r2_2 c2 at1_2 c1_2",
                "s1_1 s2 r2_1 at1_1 w2 c1_1 r2_2 s1_2 at1_2 c1_2 c2",
                "s1_1 s2 r2_1 at1_1 w2 c1_1 r2_2 s1_2 at1_2 c2 c1_2",
                "s1_1 s2 r2_1 at1_1 w2 c1_1 r2_2 s1_2 c2 at1_2 c1_2",
                "s1_1 s2 r2_1 at1_1 w2 c1_1 r2_2 c2 s1_2 at1_2 c1_2",
                "s1_1 s2 r2_1 w2 at1_1 r2_2 c2 c1_1 s1_2 at1_2 c1_2",
                "s1_1 s2 r2_1 w2 r2_2 at1_1 c2 c1_1 s1_2 at1_2 c1_2",
                "s1_1 s2 r2_1 w2 r2_2 c2 at1_1 c1_1 s1_2 at1_2 c1_2",
                "s2 r2_1 s1_1 at1_1 c1_1 s1_2 at1_2 c1_2 w2 r2_2 c2",
                "s2 r2_1 s1_1 at1_1 c1_1 s1_2 at1_2 w2 c1_2 r2_2 c2",
                "s2 r2_1 s1_1 at1_1 c1_1 s1_2 at1_2 w2 r2_2 c1_2 c2",
                "s2 r2_1 s1_1 at1_1 c1_1 s1_2 at1_2 w2 r2_2 c2 c1_2",
                "s2 r2_1 s1_1 at1_1 c1_1 s1_2 w2 at1_2 c1_2 r2_2 c2",
                "s2 r2_1 s1_1 at1_1 c1_1 s1_2 w2 at1_2 r2_2 c1_2 c2",
                "s2 r2_1 s1_1 at1_1 c1_1 s1_2 w2 at1_2 r2_2 c2 c1_2",
                "s2 r2_1 s1_1 at1_1 c1_1 s1_2 w2 r2_2 at1_2 c1_2 c2",
                "s2 r2_1 s1_1 at1_1 c1_1 s1_2 w2 r2_2 at1_2 c2 c1_2",
                "s2 r2_1 s1_1 at1_1 c1_1 s1_2 w2 r2_2 c2 at1_2 c1_2",
                "s2 r2_1 s1_1 at1_1 c1_1 w2 s1_2 at1_2 c1_2 r2_2 c2",
                "s2 r2_1 s1_1 at1_1 c1_1 w2 s1_2 at1_2 r2_2 c1_2 c2",
                "s2 r2_1 s1_1 at1_1 c1_1 w2 s1_2 at1_2 r2_2 c2 c1_2",
                "s2 r2_1 s1_1 at1_1 c1_1 w2 s1_2 r2_2 at1_2 c1_2 c2",
                "s2 r2_1 s1_1 at1_1 c1_1 w2 s1_2 r2_2 at1_2 c2 c1_2",
                "s2 r2_1 s1_1 at1_1 c1_1 w2 s1_2 r2_2 c2 at1_2 c1_2",
                "s2 r2_1 s1_1 at1_1 c1_1 w2 r2_2 s1_2 at1_2 c1_2 c2",
                "s2 r2_1 s1_1 at1_1 c1_1 w2 r2_2 s1_2 at1_2 c2 c1_2",
                "s2 r2_1 s1_1 at1_1 c1_1 w2 r2_2 s1_2 c2 at1_2 c1_2",
                "s2 r2_1 s1_1 at1_1 c1_1 w2 r2_2 c2 s1_2 at1_2 c1_2",
                "s2 r2_1 s1_1 at1_1 w2 c1_1 s1_2 at1_2 c1_2 r2_2 c2",
                "s2 r2_1 s1_1 at1_1 w2 c1_1 s1_2 at1_2 r2_2 c1_2 c2",
                "s2 r2_1 s1_1 at1_1 w2 c1_1 s1_2 at1_2 r2_2 c2 c1_2",
                "s2 r2_1 s1_1 at1_1 w2 c1_1 s1_2 r2_2 at1_2 c1_2 c2",
                "s2 r2_1 s1_1 at1_1 w2 c1_1 s1_2 r2_2 at1_2 c2 c1_2",
                "s2 r2_1 s1_1 at1_1 w2 c1_1 s1_2 r2_2 c2 at1_2 c1_2",
                "s2 r2_1 s1_1 at1_1 w2 c1_1 r2_2 s1_2 at1_2 c1_2 c2",
                "s2 r2_1 s1_1 at1_1 w2 c1_1 r2_2 s1_2 at1_2 c2 c1_2",
                "s2 r2_1 s1_1 at1_1 w2 c1_1 r2_2 s1_2 c2 at1_2 c1_2",
                "s2 r2_1 s1_1 at1_1 w2 c1_1 r2_2 c2 s1_2 at1_2 c1_2",
                "s2 r2_1 s1_1 w2 at1_1 r2_2 c2 c1_1 s1_2 at1_2 c1_2",
                "s2 r2_1 s1_1 w2 r2_2 at1_1 c2 c1_1 s1_2 at1_2 c1_2",
                "s2 r2_1 s1_1 w2 r2_2 c2 at1_1 c1_1 s1_2 at1_2 c1_2",
                "s2 r2_1 w2 s1_1 at1_1 r2_2 c2 c1_1 s1_2 at1_2 c1_2",
                "s2 r2_1 w2 s1_1 r2_2 at1_1 c2 c1_1 s1_2 at1_2 c1_2",
                "s2 r2_1 w2 s1_1 r2_2 c2 at1_1 c1_1 s1_2 at1_2 c1_2",
                "s2 r2_1 w2 r2_2 s1_1 at1_1 c2 c1_1 s1_2 at1_2 c1_2",
                "s2 r2_1 w2 r2_2 s1_1 c2 at1_1 c1_1 s1_2 at1_2 c1_2",
                "s2 r2_1 w2 r2_2 c2 s1_1 at1_1 c1_1 s1_2 at1_2 c1_2"
        );

        ConcurrencyTestUtils.executePermutations(
                executions,
                operations,
                sessions,
                this::setupTables,
                this::dropTables );
    }

    /**
     * The test case is inspired by the following one of PostgreSQL:
     * https://github.com/postgres/postgres/blob/master/src/test/isolation/specs/alter-table-2.spec
     */

    @Test
    public void testConcurrentAlterAndInsert() {
        Session session1 = new Session(testHelper);
        Session session2 = new Session(testHelper);
        Set<Session> sessions = Set.of(session1, session2);

        Map<String, Runnable> operations = new HashMap<>();

        operations.put("s1a", session1::startTransaction);
        operations.put("s1b", () -> session1.executeStatement(
                "ALTER TABLE b ADD CONSTRAINT bfk FOREIGN KEY (a_id) REFERENCES a (i);",
                "sql"
        ));
        operations.put("s1c", session1::commitTransaction);

        operations.put("s2a", session2::startTransaction);
        operations.put("s2b", () -> session2.executeStatement(
                "SELECT * FROM a WHERE i = 1 LIMIT 1 FOR UPDATE;",
                "sql"
        ));
        operations.put("s2c", () -> session2.executeStatement(
                "SELECT * FROM b WHERE a_id = 3 LIMIT 1 FOR UPDATE;",
                "sql"
        ));
        operations.put("s2d", () -> session2.executeStatement(
                "INSERT INTO b VALUES (0);",
                "sql"
        ));
        operations.put("s2e", () -> session2.executeStatement(
                "INSERT INTO a VALUES (4);",
                "sql"
        ));
        operations.put("s2f", session2::commitTransaction);

        List<String> permutations = List.of(
                "s1a s1b s1c s2a s2b s2c s2d s2e s2f",
                "s1a s1b s2a s1c s2b s2c s2d s2e s2f",
                "s1a s1b s2a s2b s1c s2c s2d s2e s2f",
                "s1a s1b s2a s2b s2c s1c s2d s2e s2f",
                "s1a s1b s2a s2b s2c s2d s1c s2e s2f",
                "s1a s2a s1b s1c s2b s2c s2d s2e s2f",
                "s1a s2a s1b s2b s1c s2c s2d s2e s2f",
                "s1a s2a s1b s2b s2c s1c s2d s2e s2f",
                "s1a s2a s1b s2b s2c s2d s1c s2e s2f",
                "s1a s2a s2b s1b s1c s2c s2d s2e s2f",
                "s1a s2a s2b s1b s2c s1c s2d s2e s2f",
                "s1a s2a s2b s1b s2c s2d s1c s2e s2f",
                "s1a s2a s2b s2c s1b s1c s2d s2e s2f",
                "s1a s2a s2b s2c s1b s2d s1c s2e s2f",
                "s1a s2a s2b s2c s2d s1b s2e s2f s1c",
                "s1a s2a s2b s2c s2d s2e s1b s2f s1c",
                "s1a s2a s2b s2c s2d s2e s2f s1b s1c",
                "s2a s1a s1b s1c s2b s2c s2d s2e s2f",
                "s2a s1a s1b s2b s1c s2c s2d s2e s2f",
                "s2a s1a s1b s2b s2c s1c s2d s2e s2f",
                "s2a s1a s1b s2b s2c s2d s1c s2e s2f",
                "s2a s1a s2b s1b s1c s2c s2d s2e s2f",
                "s2a s1a s2b s1b s2c s1c s2d s2e s2f",
                "s2a s1a s2b s1b s2c s2d s1c s2e s2f",
                "s2a s1a s2b s2c s1b s1c s2d s2e s2f",
                "s2a s1a s2b s2c s1b s2d s1c s2e s2f",
                "s2a s1a s2b s2c s2d s1b s2e s2f s1c",
                "s2a s1a s2b s2c s2d s2e s1b s2f s1c",
                "s2a s1a s2b s2c s2d s2e s2f s1b s1c",
                "s2a s2b s1a s1b s1c s2c s2d s2e s2f",
                "s2a s2b s1a s1b s2c s1c s2d s2e s2f",
                "s2a s2b s1a s1b s2c s2d s1c s2e s2f",
                "s2a s2b s1a s2c s1b s1c s2d s2e s2f",
                "s2a s2b s1a s2c s1b s2d s1c s2e s2f",
                "s2a s2b s1a s2c s2d s1b s2e s2f s1c",
                "s2a s2b s1a s2c s2d s2e s1b s2f s1c",
                "s2a s2b s1a s2c s2d s2e s2f s1b s1c"
        );

        ConcurrencyTestUtils.executePermutations(
                permutations,
                operations,
                sessions,
                this::setupTables,
                this::dropTables
        );
    }

}
