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

import com.google.common.collect.ImmutableList;
import java.sql.ResultSet;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.cypher.CypherTestTemplate;

public class RelationalOnLpgTest extends CrossModelTestTemplate {

    private static final String GRAPH_NAME = "crossGraph";

    private static final String DATA_LABEL = "label1";


    @BeforeClass
    public static void init() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        CypherTestTemplate.createGraph( GRAPH_NAME );
        CypherTestTemplate.execute( format( "CREATE (n:%s {key: 3})", DATA_LABEL ), GRAPH_NAME );
        CypherTestTemplate.execute( format( "CREATE (n:%s {key: 4})", DATA_LABEL + 1 ), GRAPH_NAME );
        CypherTestTemplate.execute( format( "CREATE (n:%s {key: 5})", DATA_LABEL.toUpperCase() ), GRAPH_NAME );
    }


    @AfterClass
    public static void tearDown() {
        CypherTestTemplate.deleteData( GRAPH_NAME );
    }


    @Test
    public void simpleSelectTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format( "SELECT * FROM \"%s\".\"%s\"", GRAPH_NAME, DATA_LABEL ) );
            // can not test use default comparator method as id is dynamic
            List<Object[]> data = TestHelper.convertResultSetToList( result );
            assert (data.size() == 1);
            assert (data.get( 0 ).length == 3);

            result = s.executeQuery( String.format( "SELECT * FROM \"%s\".\"%s\"", GRAPH_NAME, DATA_LABEL + 1 ) );
            data = TestHelper.convertResultSetToList( result );
            assert (data.size() == 1);
            assert (data.get( 0 ).length == 3);
        } );

    }


    @Test
    public void simpleSelectUpperCaseTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format( "SELECT * FROM \"%s\".\"%s\"", GRAPH_NAME, DATA_LABEL.toUpperCase() ) );
            // can not test use default comparator method as id is dynamic
            List<Object[]> data = TestHelper.convertResultSetToList( result );
            assert (data.size() == 1);
            assert (data.get( 0 ).length == 3);
            // assert key is in the row
            assert (data.get( 0 )[1].toString().contains( "5" ));
            // assert label is in the labels
            assert (data.get( 0 )[2].toString().contains( DATA_LABEL.toUpperCase() ));
            assert (!data.get( 0 )[2].toString().contains( DATA_LABEL ));
        } );

    }


    @Test
    public void simpleProjectTest() {
        executeStatements( ( s, c ) -> {
            ResultSet result = s.executeQuery( String.format( "SELECT properties, labels FROM \"%s\".\"%s\"", GRAPH_NAME, DATA_LABEL ) );
            TestHelper.checkResultSet( result,
                    ImmutableList.of( new Object[]{ "{key=3}", new Object[]{ DATA_LABEL } } ) );
        } );

    }

}
