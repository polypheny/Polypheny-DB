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

package org.polypheny.db.crossmodel;

import static java.lang.String.format;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.CypherTestTemplate.Row;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;

public class LpgOnRelationalTest extends CrossModelTestTemplate {

    private static final String SCHEMA_NAME = "crossrelational";

    private static final String TABLE_NAME = "crossrelationaltable";

    private static final String FULL_TABLE_NAME = format( "%s.%s", SCHEMA_NAME, TABLE_NAME );


    private static final List<Object[]> DATA = ImmutableList.of(
            new Object[]{ 1, "Hans", 5 },
            new Object[]{ 2, "Alice", 7 },
            new Object[]{ 3, "Bob", 4 },
            new Object[]{ 4, "Saskia", 6 },
            new Object[]{ 5, "Rebecca", 3 },
            new Object[]{ 6, "Georg", 9 }
    );


    @BeforeAll
    public static void init() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        initStructure();
    }


    @AfterAll
    public static void tearDown() {
        destroyStructure();
    }


    private static void initStructure() {
        executeStatements( ( s, c ) -> {
            s.executeUpdate( format( "CREATE NAMESPACE %s", SCHEMA_NAME ) );
            s.executeUpdate( format( "CREATE TABLE %s( id INTEGER NOT NULL, name VARCHAR(39), foo INTEGER, PRIMARY KEY (id))", FULL_TABLE_NAME ) );

            for ( Object[] row : DATA ) {
                s.executeUpdate( format( "INSERT INTO %s VALUES (%s, '%s', %s)", FULL_TABLE_NAME, row[0], row[1], row[2] ) );
            }

            c.commit();
        } );
    }


    private static void destroyStructure() {
        executeStatements( ( s, c ) -> {
            s.executeUpdate( format( "DROP TABLE %s", FULL_TABLE_NAME ) );
            s.executeUpdate( format( "DROP SCHEMA %s", SCHEMA_NAME ) );

            c.commit();
        } );
    }


    private static TestNode rowToNodes( Object[] row ) {
        return TestNode.from(
                List.of( TABLE_NAME ),
                Pair.of( "foo", row[2] ),
                Pair.of( "name", row[1] ),
                Pair.of( "id", row[0] ) );
    }


    @Test
    public void simpleAllTest() {
        assert CypherTestTemplate.containsRows(
                CypherTestTemplate.execute( "MATCH (n) RETURN n", SCHEMA_NAME ),
                true,
                false,
                DATA.stream().map( r -> Row.of( rowToNodes( r ) ) ).toArray( Row[]::new ) );
    }


    @Test
    public void simpleMatchTest() {
        CypherTestTemplate.containsRows(
                CypherTestTemplate.execute( format( "MATCH (n:%s) RETURN n", TABLE_NAME ), SCHEMA_NAME ),
                true,
                false,
                DATA.stream().map( r -> Row.of( rowToNodes( r ) ) ).toArray( Row[]::new ) );
    }


    @Test
    public void emptyMatchTest() {
        CypherTestTemplate.containsRows(
                CypherTestTemplate.execute( format( "MATCH (n:%s) RETURN n", "random" ), SCHEMA_NAME ),
                true,
                false );
    }

}
