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
import static org.polypheny.db.mql.MqlTestTemplate.execute;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.cypher.CypherTestTemplate;

public class DocumentOnLpgTest extends CrossModelTestTemplate {

    private static final String GRAPH_NAME = "crossGraph";

    private static final String DATA_LABEL = "label1";


    @BeforeAll
    public static void init() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        CypherTestTemplate.createGraph( GRAPH_NAME );
        CypherTestTemplate.execute( format( "CREATE (n:%s {key: 3})", DATA_LABEL ), GRAPH_NAME );
    }


    @AfterAll
    public static void tearDown() {
        CypherTestTemplate.deleteData( GRAPH_NAME );
    }


    @Test
    public void simpleFindTest() {
        TestHelper.MongoConnection.checkDocResultSet(
                execute( format( "db.%s.find({})", DATA_LABEL ), GRAPH_NAME ),
                ImmutableList.of( "{\"key\":\"3\"}" ), true, true );
    }


    @Test
    public void simpleFindUppercaseTest() {
        CypherTestTemplate.execute( format( "CREATE (n:%s {key: 5})", DATA_LABEL.toUpperCase() ), GRAPH_NAME );

        TestHelper.MongoConnection.checkDocResultSet(
                execute( format( "db.%s.find({})", DATA_LABEL.toUpperCase() ), GRAPH_NAME ),
                ImmutableList.of( "{\"key\":\"5\"}" ), true, true );

        CypherTestTemplate.execute( format( "MATCH (n:%s {key: 5}) DELETE n", DATA_LABEL.toUpperCase() ), GRAPH_NAME );
    }


    @Test
    public void simpleProjectTest() {
        TestHelper.MongoConnection.checkDocResultSet(
                execute( format( "db.%s.find({},{\"key\":1})", DATA_LABEL ), GRAPH_NAME ),
                ImmutableList.of( "{\"key\":\"3\"}" ), true, true );
    }


}
