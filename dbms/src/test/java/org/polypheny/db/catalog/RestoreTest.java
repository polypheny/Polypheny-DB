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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.entity.logical.LogicalTable;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
public class RestoreTest {


    private final String CONSTRAINT_CREATE = "CREATE TABLE IF NOT EXISTS constraint_test ("
            + "ctid INTEGER NOT NULL, "
            + "a INTEGER NOT NULL, "
            + "b INTEGER NOT NULL, "
            + "c INTEGER NOT NULL, "
            + "PRIMARY KEY (ctid), "
            + "CONSTRAINT u_a UNIQUE (a), "
            + "CONSTRAINT u_ab UNIQUE (a,b))";
    private final String CONSTRAINT2_CREATE = "CREATE TABLE IF NOT EXISTS constraint_test2 ("
            + "ctid INTEGER NOT NULL, "
            + "a INTEGER NOT NULL, "
            + "b INTEGER NOT NULL, "
            + "c INTEGER NOT NULL, "
            + "PRIMARY KEY (ctid), "
            + "FOREIGN KEY (a) REFERENCES public.constraint_test(a))";


    @BeforeAll
    public static void setUp() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @AfterAll
    public static void tearDown() throws SQLException {
        TestHelper.executeSQL( "DROP TABLE IF EXISTS constraint_test" );
        TestHelper.executeSQL( "DROP TABLE IF EXISTS constraint_test2" );
    }


    @Test
    public void foreignTableKeyTest() {
        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( CONSTRAINT_CREATE ),
                ( c, s ) -> s.executeUpdate( CONSTRAINT2_CREATE ),
                ( c, s ) -> c.commit()
        );
        LogicalTable table = Catalog.snapshot().rel().getTable( "public", "constraint_test2" ).orElseThrow();
        assertEquals( Catalog.snapshot().rel().getForeignKeys( table.id ).size(), 1 );
    }


}
