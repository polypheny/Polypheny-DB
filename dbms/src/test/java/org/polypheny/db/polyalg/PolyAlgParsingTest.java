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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.polyalg.parser.PolyAlgParser;
import org.polypheny.db.algebra.polyalg.parser.PolyAlgToAlgConverter;
import org.polypheny.db.algebra.polyalg.parser.nodes.PolyAlgNode;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rex.RexBuilder;

public class PolyAlgParsingTest {


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
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE polyalg_test" );
            }
        }
    }


    private static void addTestData() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE polyalg_test( id INTEGER NOT NULL, name VARCHAR(39), foo INTEGER, PRIMARY KEY (id))" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (1, 'Hans', 5)" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (2, 'Alice', 7)" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (3, 'Bob', 4)" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (4, 'Saskia', 6)" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (5, 'Rebecca', 3)" );
                statement.executeUpdate( "INSERT INTO polyalg_test VALUES (6, 'Georg', 9)" );
                connection.commit();
            }
        }
    }


    private static AlgRoot fromPolyAlg( String polyAlg ) throws NodeParseException {
        AlgDataTypeFactory factory = AlgDataTypeFactory.DEFAULT;
        AlgCluster cluster = AlgCluster.create( new VolcanoPlanner(), new RexBuilder( factory ), null, null );
        Snapshot snapshot = Catalog.snapshot();

        PolyAlgToAlgConverter converter = new PolyAlgToAlgConverter( snapshot, cluster );

        PolyAlgParser parser = PolyAlgParser.create( polyAlg );
        PolyAlgNode node = (PolyAlgNode) parser.parseQuery();
        return converter.convert( node );
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
                PROJECT[id, name, foo](
                 FILTER[>(foo, 5)](
                  SCAN[public.polyalg_test]))
                """;
        AlgNode node = fromPolyAlg( polyAlg ).alg;
        assertTrue( node instanceof LogicalRelProject );
        assertTrue( node.getInput( 0 ) instanceof LogicalRelFilter );
        assertEqualAfterRoundtrip( polyAlg, node );

    }


    @Test
    public void aggregatePolyAlgTest() throws NodeParseException {
        String polyAlg = """
                AGG[group=name, aggs=COUNT(DISTINCT foo) AS EXPR$1](
                 PROJECT[foo, name](
                  SCAN[public.polyalg_test]))
                """;
        AlgNode node = fromPolyAlg( polyAlg ).alg;
        assertTrue( node instanceof LogicalRelAggregate );
        assertEqualAfterRoundtrip( polyAlg, node );
    }


    @Test
    public void sortPolyAlgTest() throws NodeParseException {
        String polyAlg = """
                SORT[sort=name DESC LAST, limit=2, offset=1](
                  SCAN[public.polyalg_test])
                """;
        AlgNode node = fromPolyAlg( polyAlg ).alg;
        assertTrue( node instanceof LogicalRelSort );
        assertEqualAfterRoundtrip( polyAlg, node );
    }


    @Test
    public void joinPolyAlgTest() throws NodeParseException {
        String polyAlg = """
                PROJECT[id, name, foo, id0, name0, foo0](
                 JOIN[=(id, id0), type=LEFT](
                  SCAN[public.polyalg_test],
                  PROJECT[id AS id0, name AS name0, foo AS foo0](
                   SCAN[public.polyalg_test])))
                """;
        AlgNode node = fromPolyAlg( polyAlg ).alg;
        assertTrue( node.getInput( 0 ) instanceof LogicalRelJoin );
        assertEqualAfterRoundtrip( polyAlg, node );
    }


    @Test
    public void unionPolyAlgTest() throws NodeParseException {
        String polyAlg = """
                UNION[all=true](
                 PROJECT[id](
                  SCAN[public.polyalg_test]),
                 PROJECT[foo](
                  SCAN[public.polyalg_test]))
                """;
        AlgNode node = fromPolyAlg( polyAlg ).alg;
        assertTrue( node instanceof LogicalRelUnion );
        assertEqualAfterRoundtrip( polyAlg, node );
    }

}
