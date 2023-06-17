/*
 * Copyright 2019-2022 The Polypheny Project
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
import static org.polypheny.db.mql.MqlTestTemplate.execute;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;

@SuppressWarnings({ "SqlDialectInspection", "SqlNoDataSourceInspection" })
public class DocumentOnRelationalTest extends CrossModelTestTemplate {

    private static final String NAMESPACE_NAME = "crossRelational";

    private static final String TABLE_NAME = "crossRelationalTable";

    private static final String FULL_TABLE_NAME = format( "%s.%s", NAMESPACE_NAME, TABLE_NAME );

    private static final String[] ROW_IDS = new String[]{ "id", "name", "foo" };

    private static final List<Object[]> DATA = ImmutableList.of(
            new Object[]{ 1, "Hans", 5 },
            new Object[]{ 2, "Alice", 7 },
            new Object[]{ 3, "Bob", 4 },
            new Object[]{ 4, "Saskia", 6 },
            new Object[]{ 5, "Rebecca", 3 },
            new Object[]{ 6, "Georg", 9 }
    );


    private static List<String> asDocument( int... indexes ) {
        List<String> docs = new ArrayList<>();
        for ( Object[] row : DATA ) {
            List<String> doc = new ArrayList<>();
            for ( int index : indexes ) {
                doc.add( ROW_IDS[index] + ":" + row[index] );
            }
            docs.add( "{" + String.join( ",", doc ) + "}" );
        }

        return docs;
    }


    @BeforeClass
    public static void init() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        initStructure();
    }


    @AfterClass
    public static void tearDown() {
        destroyStructure();
    }


    private static void initStructure() {
        executeStatements( ( s, c ) -> {
            s.executeUpdate( format( "CREATE NAMESPACE %s", NAMESPACE_NAME ) );
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
            s.executeUpdate( format( "DROP SCHEMA %s", NAMESPACE_NAME ) );

            c.commit();
        } );
    }


    @Test
    public void simpleFindTest() {
        TestHelper.MongoConnection.checkDocResultSet(
                execute( String.format( "db.%s.find({})", TABLE_NAME ), NAMESPACE_NAME ),
                asDocument( 1, 2, 3 ),
                true,
                true );
    }


    @Test
    public void simpleProjectTest() {
        TestHelper.MongoConnection.checkDocResultSet(
                execute( String.format( "db.%s.find({},{id: 1})", TABLE_NAME ), NAMESPACE_NAME ),
                asDocument( 1 ),
                true,
                true );
    }


    @Test
    public void simpleFilterTest() {
        TestHelper.MongoConnection.checkDocResultSet(
                execute( String.format( "db.%s.find({id: 1},{})", TABLE_NAME ), NAMESPACE_NAME ),
                List.of( asDocument( 1, 2, 3 ).get( 0 ) ),
                true,
                true );
    }

}
