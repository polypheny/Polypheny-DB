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

import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.CypherTestTemplate.Row;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.mql.MqlTestTemplate;
import org.polypheny.db.util.Pair;

@Ignore
public class LpgOnDocumentTest extends CrossModelTestTemplate {

    private static final String DATABASE_NAME = "crossDocumentSchema";

    private static final String COLLECTION_NAME = "crossCollection";


    @BeforeClass
    public static void init() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        MqlTestTemplate.initDatabase( DATABASE_NAME );
        MqlTestTemplate.createCollection( COLLECTION_NAME );
    }


    @AfterClass
    public static void tearDown() {
        MqlTestTemplate.dropDatabase( DATABASE_NAME );
    }


    @Test
    public void simpleMatchTest() {
        CypherTestTemplate.containsRows(
                CypherTestTemplate.execute( "MATCH (n) RETURN n", DATABASE_NAME ),
                true,
                false,
                Row.of(
                        TestNode.from( List.of( "Person" ), Pair.of( "name", "Max" ), Pair.of( "age", 25 ) ) ) );
    }

}
