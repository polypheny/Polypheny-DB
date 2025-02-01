/*
 * Copyright 2019-2025 The Polypheny Project
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

import static java.lang.String.format;
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
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.polyalg.PolyAlgRegistry;
import org.polypheny.db.algebra.polyalg.parser.PolyAlgParser;
import org.polypheny.db.algebra.polyalg.parser.PolyAlgToAlgConverter;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgNode;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationPolyAlg;
import org.polypheny.db.information.InformationPolyAlg.PlanType;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.mql.MqlTestTemplate;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.TranslatedQueryContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.routing.UiRoutingPageUtil;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.TransactionManagerImpl;
import org.polypheny.db.type.entity.PolyValue;


@Slf4j
@SuppressWarnings("SqlNoDataSourceInspection")
public class PolyAlgParsingTest {

    private static final String ORIGIN = "PolyAlgParsingTest";
    private static final String GRAPH_NAME = "polyAlgGraph";
    private static final String DOC_NAME = MqlTestTemplate.namespace;
    private static final String DOC_COLL = "polyalgdocs";


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
            connection.commit();
            connection.close();
        }
        CypherTestTemplate.deleteData( GRAPH_NAME );
        MqlTestTemplate.dropCollection( DOC_COLL );
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

        CypherTestTemplate.createGraph( GRAPH_NAME );
        CypherTestTemplate.execute( "CREATE (p:Person {name: 'Ann', age: 45, depno: 13})", GRAPH_NAME );
        CypherTestTemplate.execute( "CREATE (p:Person {name: 'Bob', age: 31, depno: 13})", GRAPH_NAME );
        CypherTestTemplate.execute( "CREATE (p:Person {name: 'Hans', age: 55, depno: 7})", GRAPH_NAME );
        CypherTestTemplate.execute( "CREATE (p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira', age:3, type:'dog'})", GRAPH_NAME );

        MqlTestTemplate.initDatabase( DOC_NAME );
        MqlTestTemplate.createCollection( DOC_COLL, DOC_NAME );
        List<String> docs = List.of(
                "{\"item\": \"journal\", \"qty\": 25, \"tags\": [\"blank\", \"red\"], \"dim_cm\": [14, 21]}",
                "{\"item\": \"notebook\", \"qty\": 50, \"tags\": [\"red\", \"blank\"], \"dim_cm\": [14, 21]}",
                "{ \"item\": \"paper\", \"qty\": 100, \"tags\": [\"red\", \"blank\", \"plain\"], \"dim_cm\": [14, 21], }",
                "{\"item\": \"planner\", \"qty\": 75, \"tags\": [\"blank\", \"red\"], \"dim_cm\": [22.85, 30]}",
                "{\"item\": \"postcard\", \"qty\": 45, \"tags\": [\"blue\"], \"dim_cm\": [10, 15.25]}"
        );
        MqlTestTemplate.insertMany( docs, DOC_COLL );
    }


    private static void testCypherRoundTrip( String query ) throws NodeParseException {
        testQueryRoundTrip( query, QueryLanguage.from( "cypher" ), GRAPH_NAME );
    }


    private static void testSqlRoundTrip( String query ) throws NodeParseException {
        testQueryRoundTrip( query, QueryLanguage.from( "sql" ), null );
    }


    private static void testMqlRoundTrip( String query ) throws NodeParseException {
        testQueryRoundTrip( query, QueryLanguage.from( "mql" ), DOC_NAME );
    }


    /**
     * Executes the query and checks whether both the result and the (logical and allocation) PolyAlgebra are still equal after one round-trip.
     * For this we do the following:
     * Query -> AlgNode1 -> Result1
     * <p>
     * AlgNode1 -> PolyAlg1 -> AlgNode2 -> Result2
     * <p>
     * AlgNode2 -> PolyAlg2
     * <p>
     * Then we check whether PolyAlg1 equals PolyAlg2 and Result1 equals Result2.
     */
    private static void testQueryRoundTrip( String query, QueryLanguage ql, String namespace ) throws NodeParseException {
        long ns = namespace == null ? Catalog.defaultNamespaceId : Catalog.snapshot().getNamespace( namespace ).orElseThrow().id;
        TransactionManager transactionManager = TransactionManagerImpl.getInstance();
        Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, ns, true, ORIGIN );

        try {

            QueryContext qc = QueryContext.builder()
                    .query( query )
                    .language( ql )
                    .isAnalysed( true )
                    .usesCache( true )
                    .origin( ORIGIN )
                    .namespaceId( ns )
                    .batch( -1 )
                    .transactionManager( transactionManager )
                    .transactions( List.of( transaction ) ).build();
            List<ExecutedContext> executedContexts = LanguageManager.getINSTANCE().anyQuery( qc );
            String result = getResultAsString( executedContexts, ql.dataModel() );

            String logical = null, allocation = null, physical = null;

            int tries = 5;
            try {
                // plans are serialized in a separate thread, which might take some time
                while ( UiRoutingPageUtil.runningTasks() > 0 && tries-- > 0 ) {
                    Thread.sleep( 2000 );
                }
                if ( tries == 0 ) {
                    throw new RuntimeException( "Took too long to set all plans" );
                }

                for ( Information info : transaction.getQueryAnalyzer().getInformationArray() ) {
                    if ( info instanceof InformationPolyAlg polyInfo && polyInfo.getTextualPolyAlg() != null ) {
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

            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
            } finally {
                transaction.commit(); // execute PolyAlg creates new transaction, as long as only DQLs are tested like this
            }
            if ( transactionManager.getNumberOfActiveTransactions() > 0 ) {
                throw new RuntimeException();
            }

            // Check that parsing and executing again returns the same result
            String resultFromLogical = executePolyAlg( logical, PlanType.LOGICAL, ql );
            assertEquals( result, resultFromLogical, "Result from query does not match result when executing the logical plan." );
            String resultFromAllocation = executePolyAlg( allocation, PlanType.ALLOCATION, ql );
            assertEquals( result, resultFromAllocation, "Result from query does not match result when executing the allocation plan." );
        } catch ( Exception e ) {
            transaction.rollback( "Error during testing round trip: " + e.getMessage() );
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
        try {
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

        } catch ( Exception e ) {
            transaction.rollback( "Error during execution of polyAlg: " + e.getMessage() );
            throw new RuntimeException( e );
        }
    }


    private static String getResultAsString( List<ExecutedContext> executedContexts, DataModel dataModel ) {
        assertEquals( 1, executedContexts.size() );
        ExecutedContext context = executedContexts.get( 0 );

        assertTrue( context.getException().isEmpty(), "Query resulted in an exception" );
        try {

            @NotNull ResultIterator iter = context.getIterator();
            String tupleType = context.getImplementation().tupleType.toString();
            List<List<PolyValue>> rows = iter.getAllRowsAndClose();

            StringBuilder sb = new StringBuilder( tupleType );

            for ( List<PolyValue> row : rows ) {
                sb.append( "\n" );
                for ( PolyValue v : row ) {
                    String json = v == null ? "NULL" : v.toJson();
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
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
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
        String REL_PROJECT = PolyAlgRegistry.getDeclaration( LogicalRelProject.class ).opName;
        String REL_FILTER = PolyAlgRegistry.getDeclaration( LogicalRelFilter.class ).opName;
        String REL_SCAN = PolyAlgRegistry.getDeclaration( LogicalRelScan.class ).opName;

        String polyAlg = format( """
                %s[id, name, foo](
                 %s[>(foo, 5)](
                  %s[public.polyalg_test]))
                """, REL_PROJECT, REL_FILTER, REL_SCAN );
        AlgNode node = buildFromPolyAlg( polyAlg ).alg;
        assertEqualAfterRoundtrip( polyAlg, node );
    }


    @Test
    public void opAliasPolyAlgTest() throws NodeParseException {
        String REL_PROJECT = PolyAlgRegistry.getDeclaration( LogicalRelProject.class ).opName;
        String ALIAS = PolyAlgRegistry.getDeclaration( LogicalRelProject.class ).opAliases.iterator().next();
        String REL_SCAN = PolyAlgRegistry.getDeclaration( LogicalRelScan.class ).opName;

        String polyAlg = format( """
                 %s[foo, name](
                  %s[public.polyalg_test])
                """, ALIAS, REL_SCAN );
        AlgNode node = buildFromPolyAlg( polyAlg ).alg;
        String polyAlgAfter = toPolyAlg( node );
        assertEquals( polyAlg.replace( ALIAS + "[", REL_PROJECT + "[" ).replaceAll( "\\s", "" ),
                polyAlgAfter.replaceAll( "\\s", "" ) );
    }


    @Test
    public void paramAliasPolyAlgTest() throws NodeParseException {
        String REL_SORT = PolyAlgRegistry.getDeclaration( LogicalRelSort.class ).opName;
        String REL_SCAN = PolyAlgRegistry.getDeclaration( LogicalRelScan.class ).opName;
        String LIMIT_ALIAS = PolyAlgRegistry.getDeclaration( LogicalRelSort.class ).getParam( "limit" ).getAliases().iterator().next();
        String polyAlg = format( """
                 %s[%s=2](
                  %s[public.polyalg_test])
                """, REL_SORT, LIMIT_ALIAS, REL_SCAN );
        AlgNode node = buildFromPolyAlg( polyAlg ).alg;
        String polyAlgAfter = toPolyAlg( node );
        assertEquals( polyAlg.replace( LIMIT_ALIAS + "=", "limit=" ).replaceAll( "\\s", "" ),
                polyAlgAfter.replaceAll( "\\s", "" ) );
    }


    @Test
    public void sqlProjectFilterTest() throws NodeParseException {
        testSqlRoundTrip( "SELECT id, name AS first_name FROM polyalg_test WHERE foo <= 6" );
    }


    @Test
    public void sqlDistinctAggregateTest() throws NodeParseException {
        testSqlRoundTrip( "SELECT gender, COUNT(distinct foo) FROM polyalg_test GROUP BY gender" );
    }


    @Test
    public void sqlFilterAggregateTest() throws NodeParseException {
        testSqlRoundTrip( "SELECT gender, COUNT(foo) FILTER(WHERE foo < 5) as filtered FROM polyalg_test GROUP BY gender" );
    }


    @Test
    public void sqlAggregateWithNullNameTest() throws NodeParseException {
        testSqlRoundTrip( "SELECT id, gender FROM polyalg_test as e WHERE EXISTS ( SELECT 1 FROM polyalg_test WHERE gender = 'Male' AND id = e.id );" );
    }


    @Test
    public void sqlUnionTest() throws NodeParseException {
        testSqlRoundTrip( "(SELECT id FROM polyalg_test) UNION (SELECT foo FROM polyalg_test)" );
    }


    @Test
    public void sqlCastTest() throws NodeParseException {
        testSqlRoundTrip( "SELECT id, CAST(foo as VARCHAR(3)), 14.2 FROM polyalg_test" );
    }


    @Test
    public void sqlSortTest() throws NodeParseException {
        testSqlRoundTrip( "SELECT * FROM polyalg_test ORDER BY foo desc" );
    }


    @Test
    public void sqlJoinWithRenameTest() throws NodeParseException {
        testSqlRoundTrip( "SELECT * FROM polyalg_test t1 JOIN polyalg_test t2 ON t1.id=t2.foo" );
    }


    @Test
    public void sqlInsertTest() throws NodeParseException {
        testSqlRoundTrip( "INSERT INTO polyalg_test VALUES (7, 'Mike', 12, 'Male')" );
    }


    @Test
    public void sqlAliasWithSpaceFilterTest() throws NodeParseException {
        testSqlRoundTrip( "SELECT *, 'foo value' FROM (SELECT foo AS \"foo value\" FROM polyalg_test) WHERE \"foo value\" < 10" );
    }


    @Test
    public void cypherRelCrossModelTest() throws NodeParseException {
        testQueryRoundTrip( "MATCH (n) where n.foo < 6 RETURN n ORDER BY n.name LIMIT 3", QueryLanguage.from( "cypher" ), null );
        testQueryRoundTrip( "MATCH (n:polyalg_test {gender: 'Female'}) where n.foo < 6 RETURN n ORDER BY n.name LIMIT 3", QueryLanguage.from( "cypher" ), null );
    }


    @Test
    public void cypherExtractFromPathTest() throws NodeParseException {
        testQueryRoundTrip( "MATCH (n)-[r]->(m) RETURN r", QueryLanguage.from( "cypher" ), null );
    }


    @Test
    public void mongoRelCrossModelTest() throws NodeParseException {
        testQueryRoundTrip( "db.polyalg_test.find({'gender': 'Female'})", QueryLanguage.from( "mql" ), null );
    }


    @Test
    public void cypherMatchNodeTest() throws NodeParseException {
        testCypherRoundTrip( "MATCH (n:Person) RETURN n ORDER BY n.name" );
    }


    @Test
    public void cypherMatchPathTest() throws NodeParseException {
        testCypherRoundTrip( "MATCH (n:Person)-[rel:OWNER_OF]->(a:Animal) RETURN n" );
    }


    @Test
    public void cypherCreateTest() throws NodeParseException {
        testCypherRoundTrip( "CREATE (c:Car {color: 'red'}), (p:Person {name: 'Martin'}), (p)-[:OWNS_CAR]->(c)" );
    }


    @Test
    public void sqlLpgCrossModelTest() throws NodeParseException {
        testQueryRoundTrip( "SELECT * FROM " + GRAPH_NAME + ".Person", QueryLanguage.from( "sql" ), GRAPH_NAME );
    }


    @Test
    public void mongoLpgCrossModelTest() throws NodeParseException {
        testQueryRoundTrip( "db.Person.find({})", QueryLanguage.from( "mql" ), GRAPH_NAME );
    }


    @Test
    public void mongoFindTest() throws NodeParseException {
        testMqlRoundTrip( "db." + DOC_COLL + ".find({item: 'journal'})" );
    }


    @Test
    public void mongoArrayFindTest() throws NodeParseException {
        testMqlRoundTrip( "db." + DOC_COLL + ".find( { tags: [\"red\", \"blank\"] } )" );
    }


    @Test
    public void mongoElementRefTest() throws NodeParseException {
        testMqlRoundTrip( "db." + DOC_COLL + ".find({\"dim_cm\": {\"$elemMatch\": {\"$gt\": 22}}})" );
    }


    @Test
    public void mongoInsertTest() throws NodeParseException {
        testMqlRoundTrip( "db." + DOC_COLL + ".insertOne({item: \"canvas\"})" );
    }


    @Test
    public void sqlDocCrossModelTest() throws NodeParseException {
        testQueryRoundTrip( "SELECT * FROM " + DOC_NAME + "." + DOC_COLL, QueryLanguage.from( "sql" ), DOC_NAME );
    }


    @Test
    public void cypherDocCrossModelTest() throws NodeParseException {
        testQueryRoundTrip( "MATCH (n:" + DOC_COLL + ") RETURN n", QueryLanguage.from( "cypher" ), DOC_NAME );
    }

}
