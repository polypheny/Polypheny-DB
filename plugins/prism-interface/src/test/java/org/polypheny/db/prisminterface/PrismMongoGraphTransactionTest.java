/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.prisminterface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.polypheny.db.prisminterface.CypherTest.expect;
import static org.polypheny.db.prisminterface.CypherTest.integerValue;
import static org.polypheny.db.prisminterface.CypherTest.relationalFrame;
import static org.polypheny.db.prisminterface.CypherTest.singleRow;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.RollbackRequest;

@Tag("adapter")
@Timeout(30)
class PrismMongoGraphTransactionTest {

    private static final String DATA_GRAPH = "mongo_graph_owner_data";
    private static final String DDL_GRAPH = "mongo_graph_owner_ddl";


    @BeforeAll
    static void startMongoDb() {
        assumeTrue( "mongodb".equalsIgnoreCase( System.getProperty( "store.default" ) ) );
        TestHelper.getInstance();
    }


    @Test
    void graphDdlLeavesIndependentWritesUncommitted() throws IOException {
        try ( PrismConnection ddl = new PrismConnection( true, 4 ); PrismConnection writer = new PrismConnection( false, 4 ) ) {
            try {
                ddl.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + DATA_GRAPH, -1 );
                ddl.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE GRAPH NAMESPACE " + DDL_GRAPH, -1 );
                writer.execute( DATA_GRAPH, "cypher", "CREATE (:Transient)", -1 );

                ddl.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE " + DDL_GRAPH, -1 );
                expect( writer.send( Request.newBuilder().setRollbackRequest( RollbackRequest.getDefaultInstance() ) ), Response.TypeCase.ROLLBACK_RESPONSE );

                var result = ddl.execute( DATA_GRAPH, "cypher", "MATCH (n:Transient) RETURN count(n) AS nodes", -1 );
                assertEquals( 0, integerValue( singleRow( relationalFrame( result.getResult().getFrame(), true, List.of( "nodes" ) ) ).getValues( 0 ) ) );
            } finally {
                writer.send( Request.newBuilder().setRollbackRequest( RollbackRequest.getDefaultInstance() ) );
                ddl.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + DDL_GRAPH, -1 );
                ddl.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + DATA_GRAPH, -1 );
            }
        }
    }

}
