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

package org.polypheny.db.polyalg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.polyalg.parser.PolyAlgParser;
import org.polypheny.db.algebra.polyalg.parser.PolyAlgToAlgConverter;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgNode;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationPolyAlg;
import org.polypheny.db.information.InformationPolyAlg.PlanType;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.TranslatedQueryContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.TransactionManagerImpl;
import org.polypheny.db.type.entity.PolyValue;

public class PolyAlgParsingTest {

    private static final String ORIGIN = "PolyAlgParsingTest";


    @BeforeAll
    public static void start() throws SQLException {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestData();
    }


    @AfterAll
    public static void tearDown() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE polyalg_test" );
            }
        }
    }


    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE polyalg_test( id INTEGER NOT NULL, name VARCHAR(39), foo INTEGER, gender VARCHAR(39), PRIMARY KEY (id))" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (1, 'Hans', 5, 'Male')" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (2, 'Alice', 7, 'Female')" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (3, 'Bob', 4, 'Male')" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (4, 'Saskia', 6, 'Female')" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (5, 'Rebecca', 6, 'Female')" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (6, 'Georg', 9, 'Male')" );
                connection.commit();
            }
            connection.close();
        }
    }


    /**
     * Executes the query and checks whether both the result and the (logical and allocation) PolyAlgebra are still equal after one round-trip.
     * For this we do the following:
     * Query -> AlgNode1 -> Result1
     *
     * AlgNode1 -> PolyAlg1 -> AlgNode2 -> Result2
     *
     * AlgNode2 -> PolyAlg2
     *
     * Then we check whether PolyAlg1 equals PolyAlg2 and Result1 equals Result2.
     */
    private static void executeQueryRoundTrip( String query, QueryLanguage ql ) throws NodeParseException {
        TransactionManager transactionManager = TransactionManagerImpl.getInstance();
        Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultNamespaceId, true, ORIGIN );

        QueryContext qc = QueryContext.builder()
                .query( query )
                .language( ql )
                .isAnalysed( true )
                .usesCache( true )
                .origin( ORIGIN )
                .namespaceId( Catalog.defaultNamespaceId )
                .batch( -1 )
                .transactionManager( transactionManager )
                .transactions( List.of( transaction ) ).build();
        List<ExecutedContext> executedContexts = LanguageManager.getINSTANCE().anyQuery( qc );
        String result = getResultAsString( executedContexts, ql.dataModel() );

        String logical = null, allocation = null, physical = null;
        for ( Information info : transaction.getQueryAnalyzer().getInformationArray() ) {
            if ( info instanceof InformationPolyAlg polyInfo ) {
                switch ( PlanType.valueOf( polyInfo.planType ) ) {
                    case LOGICAL -> logical = polyInfo.getTextualPolyAlg();
                    case ALLOCATION -> allocation = polyInfo.getTextualPolyAlg();
                    case PHYSICAL -> physical = polyInfo.getTextualPolyAlg();
                }
            }
        }
        assertNotNull( logical );
        assertNotNull( allocation );
        assertNotNull( physical ); // Physical is not yet tested further since it is only partially implemented

        // Check that parsing and executing again returns the same result
        String resultFromLogical = executePolyAlg( logical, PlanType.LOGICAL, ql );
        assertEquals( result, resultFromLogical, "Result from query does not match result when executing the logical plan." );
        String resultFromAllocation = executePolyAlg( allocation, PlanType.ALLOCATION, ql );
        assertEquals( result, resultFromAllocation, "Result from query does not match result when executing the allocation plan." );

        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * Parses the given polyAlg into a plan of the specified type.
     * The plan is then executed and the result is returned as a string.
     * The plan is also serialized back to polyAlgebra to check whether it is equal to before.
     */
    private static String executePolyAlg( String polyAlg, PlanType planType, QueryLanguage ql ) throws NodeParseException {
        assert planType != PlanType.PHYSICAL : "Physical plan is not yet supported by this helper function";

        TransactionManager transactionManager = TransactionManagerImpl.getInstance();
        Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultNamespaceId, true, ORIGIN );
        Statement statement = transaction.createStatement();
        AlgRoot root = buildFromPolyAlg( polyAlg, planType, statement );
        assertEqualAfterRoundtrip( polyAlg, root.alg );

        QueryContext qc = QueryContext.builder()
                .query( polyAlg )
                .language( ql )
                .isAnalysed( true )
                .usesCache( true )
                .origin( ORIGIN )
                .batch( -1 )
                .transactionManager( transactionManager )
                .transactions( List.of( transaction ) )
                .statement( statement ).build();
        TranslatedQueryContext translated = TranslatedQueryContext.fromQuery( polyAlg, root, planType == PlanType.ALLOCATION, qc );
        List<ExecutedContext> executedContexts = LanguageManager.getINSTANCE().anyQuery( translated );
        String result = getResultAsString( executedContexts, ql.dataModel() );
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }
        return result;
    }


    private static String getResultAsString( List<ExecutedContext> executedContexts, DataModel dataModel ) {
        assertEquals( executedContexts.size(), 1 );
        ExecutedContext context = executedContexts.get( 0 );
        assertTrue( context.getException().isEmpty(), "Query resulted in an exception" );
        List<List<PolyValue>> rows = context.getIterator().getAllRowsAndClose();
        String tupleType = context.getIterator().getImplementation().tupleType.toString();
        StringBuilder sb = new StringBuilder( tupleType );

        for ( List<PolyValue> row : rows ) {
            sb.append( "\n" );
            for ( PolyValue v : row ) {
                String json = v.toJson();
                if ( json.contains( "\"id\"" ) ) {
                    try {
                        ObjectMapper objectMapper = new ObjectMapper();
                        JsonNode jsonNode = objectMapper.readTree( json );
                        ((ObjectNode) jsonNode).remove( "id" );
                        json = objectMapper.writeValueAsString( jsonNode );
                    } catch ( JsonProcessingException ignored ) {
                    }
                }
                sb.append( json ).append( "," );
            }
        }
        return sb.toString();
    }


    private static AlgRoot buildFromPolyAlg( String polyAlg, PlanType planType, Statement statement ) throws NodeParseException {
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
        AlgCluster cluster = AlgCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder, null, snapshot );
        PolyAlgToAlgConverter converter = new PolyAlgToAlgConverter( planType, snapshot, cluster );
        return converter.convert( (PolyAlgNode) PolyAlgParser.create( polyAlg ).parseQuery() );
    }


    /**
     * Get AlgNode tree for the given (logical) PolyAlgebra.
     */
    private static AlgRoot buildFromPolyAlg( String polyAlg ) throws NodeParseException {
        AlgCluster cluster = AlgCluster.create( new VolcanoPlanner(), new RexBuilder( AlgDataTypeFactory.DEFAULT ), null, null );
        PolyAlgToAlgConverter converter = new PolyAlgToAlgConverter( PlanType.LOGICAL, Catalog.snapshot(), cluster );
        return converter.convert( (PolyAlgNode) PolyAlgParser.create( polyAlg ).parseQuery() );
    }


    private static String toPolyAlg( AlgNode node ) {
        StringBuilder sb = new StringBuilder();
        node.buildPolyAlgebra( sb, "" );
        return sb.toString();
    }


    private static void assertEqualAfterRoundtrip( String polyAlgBefore, AlgNode node ) {
        // Remove any whitespaces before comparing
        String polyAlgAfter = toPolyAlg( node );
        assertEquals( polyAlgBefore.replaceAll( "\\s", "" ), polyAlgAfter.replaceAll( "\\s", "" ) );
    }


    @Test
    public void projectPolyAlgTest() throws NodeParseException {
        String polyAlg = """
                REL_PROJECT[id, name, foo](
                 REL_FILTER[>(foo, 5)](
                  REL_SCAN[public.polyalg_test]))
                """;
        AlgNode node = buildFromPolyAlg( polyAlg ).alg;
        assertEqualAfterRoundtrip( polyAlg, node );
    }


    @Test
    public void aggregatePolyAlgTest() throws NodeParseException {
        String polyAlg = """
                REL_AGGREGATE[group=name, aggregates=COUNT(DISTINCT foo) AS EXPR$1](
                 REL_PROJECT[foo, name](
                  REL_SCAN[public.polyalg_test]))
                """;
        AlgNode node = buildFromPolyAlg( polyAlg ).alg;
        assertEqualAfterRoundtrip( polyAlg, node );
    }


    @Test
    public void opAliasPolyAlgTest() throws NodeParseException {
        String polyAlg = """
                 P[foo, name](
                  REL_SCAN[public.polyalg_test])
                """;
        AlgNode node = buildFromPolyAlg( polyAlg ).alg;
        String polyAlgAfter = toPolyAlg( node );
        assertEquals( polyAlg.replace( "P[", "REL_PROJECT[" ).replaceAll( "\\s", "" ),
                polyAlgAfter.replaceAll( "\\s", "" ) );
    }


    @Test
    public void paramAliasPolyAlgTest() throws NodeParseException {
        String polyAlg = """
                 REL_SORT[fetch=2](
                  REL_SCAN[public.polyalg_test])
                """;
        AlgNode node = buildFromPolyAlg( polyAlg ).alg;
        String polyAlgAfter = toPolyAlg( node );
        assertEquals( polyAlg.replace( "fetch=", "limit=" ).replaceAll( "\\s", "" ),
                polyAlgAfter.replaceAll( "\\s", "" ) );
    }


    @Test
    public void sortPolyAlgTest() throws NodeParseException {
        String polyAlg = """
                REL_SORT[sort=name DESC LAST, limit=2, offset=1](
                  REL_SCAN[public.polyalg_test])
                """;
        AlgNode node = buildFromPolyAlg( polyAlg ).alg;
        assertEqualAfterRoundtrip( polyAlg, node );
    }


    @Test
    public void joinPolyAlgTest() throws NodeParseException {
        String polyAlg = """
                REL_PROJECT[id, name, foo, id0, name0, foo0](
                 REL_JOIN[=(id, id0), type=LEFT](
                  REL_SCAN[public.polyalg_test],
                  REL_PROJECT[id AS id0, name AS name0, foo AS foo0](
                   REL_SCAN[public.polyalg_test])))
                """;
        AlgNode node = buildFromPolyAlg( polyAlg ).alg;
        assertEqualAfterRoundtrip( polyAlg, node );
    }


    @Test
    public void unionPolyAlgTest() throws NodeParseException {
        String polyAlg = """
                REL_UNION[all=true](
                 REL_PROJECT[id](
                  REL_SCAN[public.polyalg_test]),
                 REL_PROJECT[foo](
                  REL_SCAN[public.polyalg_test]))
                """;
        AlgNode node = buildFromPolyAlg( polyAlg ).alg;
        assertEqualAfterRoundtrip( polyAlg, node );
    }


    @Test
    public void sqlProjectFilterTest() throws NodeParseException {
        executeQueryRoundTrip( "SELECT id, name AS first_name FROM polyalg_test WHERE foo <= 6", QueryLanguage.from( "sql" ) );
    }


    @Test
    public void sqlDistinctAggregateTest() throws NodeParseException {
        executeQueryRoundTrip( "SELECT gender, COUNT(distinct foo) FROM polyalg_test GROUP BY gender", QueryLanguage.from( "sql" ) );
    }


    @Test
    public void sqlFilterAggregateTest() throws NodeParseException {
        executeQueryRoundTrip( "SELECT gender, COUNT(foo) FILTER(WHERE foo < 5) as filtered FROM polyalg_test GROUP BY gender", QueryLanguage.from( "sql" ) );
    }


    @Test
    public void sqlAggregateWithNullNameTest() throws NodeParseException {
        executeQueryRoundTrip( "SELECT id, gender FROM polyalg_test as e WHERE EXISTS ( SELECT 1 FROM polyalg_test WHERE gender = 'Male' AND id = e.id );", QueryLanguage.from( "sql" ) );
    }


    @Test
    public void sqlUnionTest() throws NodeParseException {
        executeQueryRoundTrip( "(SELECT id FROM polyalg_test) UNION (SELECT foo FROM polyalg_test)", QueryLanguage.from( "sql" ) );
    }


    @Test
    public void sqlCastTest() throws NodeParseException {
        executeQueryRoundTrip( "SELECT id, CAST(foo as VARCHAR(3)), 14.2 FROM polyalg_test", QueryLanguage.from( "sql" ) );
    }


    @Test
    public void sqlSortTest() throws NodeParseException {
        executeQueryRoundTrip( "SELECT * FROM polyalg_test ORDER BY foo desc", QueryLanguage.from( "sql" ) );
    }


    @Test
    public void sqlJoinWithRenameTest() throws NodeParseException {
        executeQueryRoundTrip( "SELECT * FROM polyalg_test t1 JOIN polyalg_test t2 ON t1.id=t2.foo", QueryLanguage.from( "sql" ) );
    }


    @Test
    public void sqlInsertTest() throws NodeParseException {
        executeQueryRoundTrip( "INSERT INTO polyalg_test VALUES (7, 'Mike', 12, 'Male')", QueryLanguage.from( "sql" ) );
    }

    @Test
    public void sqlAliasWithSpaceFilterTest() throws NodeParseException {
        executeQueryRoundTrip( "SELECT *, 'foo value' FROM (SELECT foo AS \"foo value\" FROM polyalg_test) WHERE \"foo value\" < 10", QueryLanguage.from( "sql" ) );
    }


    @Test
    public void cypherCrossModelTest() throws NodeParseException {
        executeQueryRoundTrip( "MATCH (n) where n.foo < 6 RETURN n ORDER BY n.name LIMIT 3", QueryLanguage.from( "cypher" ) );
        executeQueryRoundTrip( "MATCH (n:polyalg_test {gender: 'Female'}) where n.foo < 6 RETURN n ORDER BY n.name LIMIT 3", QueryLanguage.from( "cypher" ) );
    }


    @Test
    public void cypherExtractFromPathTest() throws NodeParseException {
        executeQueryRoundTrip( "MATCH (n)-[r]->(m) RETURN r", QueryLanguage.from( "cypher" ) );
    }

    @Test
    public void mongoCrossModelTest() throws NodeParseException {
        executeQueryRoundTrip( "db.polyalg_test.find({'gender': 'Female'})", QueryLanguage.from( "mql" ) );
    }

}
