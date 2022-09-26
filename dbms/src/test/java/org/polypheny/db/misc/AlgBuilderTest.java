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

package org.polypheny.db.misc;


import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgDistributions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.prepare.ContextImpl;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.sql.language.SqlMatchRecognize;
import org.polypheny.db.test.Matchers;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.FrameworkConfig;
import org.polypheny.db.tools.Frameworks;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Holder;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Unit test for {@link AlgBuilder}.
 */
@SuppressWarnings({ "SqlNoDataSourceInspection", "SqlDialectInspection" })
@Slf4j
public class AlgBuilderTest {

    private static Transaction transaction;


    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        addTestSchema();
        transaction = TestHelper.getInstance().getTransaction();
    }


    @AfterClass
    public static void tearDown() throws SQLException {
        try {
            dropTestSchema();
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }
    }


    private static void addTestSchema() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE department( deptno INTEGER NOT NULL, name VARCHAR(20) NOT NULL, loc VARCHAR(50) NULL, PRIMARY KEY (deptno))" );
                statement.executeUpdate( "CREATE TABLE employee( empid BIGINT NOT NULL, ename VARCHAR(20), job VARCHAR(10), mgr INTEGER, hiredate DATE, salary DECIMAL(7,2), commission DECIMAL(7,2), deptno INTEGER NOT NULL, PRIMARY KEY (empid)) " );
                connection.commit();
            }
        }
    }


    private static void dropTestSchema() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE department" );
                statement.executeUpdate( "DROP TABLE employee" );
                connection.commit();
            }
        }
    }


    private AlgBuilder createAlgBuilder() {
        final SchemaPlus rootSchema = transaction.getSchema().plus();
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( Parser.ParserConfig.DEFAULT )
                .defaultSchema( rootSchema.getSubSchema( transaction.getDefaultSchema().name ) )
                .traitDefs( (List<AlgTraitDef>) null )
                .programs( Programs.heuristicJoinOrder( Programs.RULE_SET, true, 2 ) )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( rootSchema ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl();
                            }
                        },
                        "",
                        0,
                        0,
                        null ) ).build();
        return AlgBuilder.create( config );
    }


    @Test
    public void testScan() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM employee
        final AlgNode root =
                createAlgBuilder()
                        .scan( "employee" )
                        .build();
        assertThat( root, Matchers.hasTree( "LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n" ) );
    }


    @Test
    public void testScanQualifiedTable() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM "public"."employee"
        final AlgNode root =
                createAlgBuilder()
                        .scan( "public", "employee" )
                        .build();
        assertThat( root, Matchers.hasTree( "LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n" ) );
    }


    @Test
    public void testScanInvalidTable() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM zzz
        try {
            final AlgNode root =
                    createAlgBuilder()
                            .scan( "ZZZ" ) // this relation does not exist
                            .build();
            fail( "expected error, got " + root );
        } catch ( Exception e ) {
            assertThat( e.getMessage(), is( "Table 'ZZZ' not found" ) );
        }
    }


    @Test
    public void testScanInvalidSchema() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM "zzz"."employee"
        try {
            final AlgNode root =
                    createAlgBuilder()
                            .scan( "ZZZ", "employee" ) // the table exists, but the schema does not
                            .build();
            fail( "expected error, got " + root );
        } catch ( Exception e ) {
            assertThat( e.getMessage(), is( "Table 'ZZZ.employee' not found" ) );
        }
    }


    @Test
    public void testScanInvalidQualifiedTable() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM "public"."zzz"
        try {
            final AlgNode root =
                    createAlgBuilder()
                            .scan( "public", "ZZZ" ) // the schema is valid, but the table does not exist
                            .build();
            fail( "expected error, got " + root );
        } catch ( Exception e ) {
            assertThat( e.getMessage(), is( "Table 'public.ZZZ' not found" ) );
        }
    }


    @Test
    @Ignore
    public void testScanValidTableWrongCase() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM "employee"
        final boolean oldCaseSensitiveValue = RuntimeConfig.RELATIONAL_CASE_SENSITIVE.getBoolean();
        try {
            RuntimeConfig.RELATIONAL_CASE_SENSITIVE.setBoolean( true );
            final AlgNode root =
                    createAlgBuilder()
                            .scan( "EMPLOYEE" ) // the table is named 'employee', not 'EMPLOYEE'
                            .build();
            fail( "Expected error (table names are case-sensitive), but got " + root );
        } catch ( Exception e ) {
            assertThat( e.getMessage(), is( "Table 'EMPLOYEE' not found" ) );
        } finally {
            RuntimeConfig.RELATIONAL_CASE_SENSITIVE.setBoolean( oldCaseSensitiveValue );
        }
    }


    @Test
    public void testScanFilterTrue() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE TRUE
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root = builder.scan( "employee" )
                .filter( builder.literal( true ) )
                .build();
        assertThat( root, Matchers.hasTree( "LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n" ) );
    }


    @Test
    public void testScanFilterTriviallyFalse() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE 1 = 2
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .filter( builder.equals( builder.literal( 1 ), builder.literal( 2 ) ) )
                        .build();
        assertThat( root, Matchers.hasTree( "LogicalValues(model=[RELATIONAL], tuples=[[]])\n" ) );
    }


    @Test
    public void testScanFilterEquals() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE deptno = 20
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root = builder.scan( "employee" )
                .filter( builder.equals( builder.field( "deptno" ), builder.literal( 20 ) ) )
                .build();
        final String expected = "LogicalFilter(model=[RELATIONAL], condition=[=($7, 20)])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testScanFilterOr() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE (deptno = 20 OR commission IS NULL) AND mgr IS NOT NULL
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .filter(
                                builder.call(
                                        OperatorRegistry.get( OperatorName.OR ), builder.call( OperatorRegistry.get( OperatorName.EQUALS ), builder.field( "deptno" ), builder.literal( 20 ) ),
                                        builder.isNull( builder.field( 6 ) ) ),
                                builder.isNotNull( builder.field( 3 ) ) )
                        .build();
        final String expected = "LogicalFilter(model=[RELATIONAL], condition=[AND(OR(=($7, 20), IS NULL($6)), IS NOT NULL($3))])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testScanFilterOr2() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE deptno = 20 OR deptno = 20
        // simplifies to
        //   SELECT *
        //   FROM emp
        //   WHERE deptno = 20
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .filter(
                                builder.call(
                                        OperatorRegistry.get( OperatorName.OR ),
                                        builder.call(
                                                OperatorRegistry.get( OperatorName.GREATER_THAN ),
                                                builder.field( "deptno" ),
                                                builder.literal( 20 ) ),
                                        builder.call(
                                                OperatorRegistry.get( OperatorName.GREATER_THAN ),
                                                builder.field( "deptno" ),
                                                builder.literal( 20 ) ) ) )
                        .build();
        final String expected = "LogicalFilter(model=[RELATIONAL], condition=[>($7, 20)])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testScanFilterAndFalse() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE deptno = 20 AND FALSE
        // simplifies to
        //   VALUES
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .filter(
                                builder.call(
                                        OperatorRegistry.get( OperatorName.GREATER_THAN ),
                                        builder.field( "deptno" ),
                                        builder.literal( 20 ) ),
                                builder.literal( false ) )
                        .build();
        final String expected = "LogicalValues(model=[RELATIONAL], tuples=[[]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testScanFilterAndTrue() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE deptno = 20 AND TRUE
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .filter(
                                builder.call(
                                        OperatorRegistry.get( OperatorName.GREATER_THAN ),
                                        builder.field( "deptno" ),
                                        builder.literal( 20 ) ),
                                builder.literal( true ) )
                        .build();
        final String expected = "LogicalFilter(model=[RELATIONAL], condition=[>($7, 20)])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Test case for "AlgBuilder incorrectly simplifies a filter with duplicate conjunction to empty".
     */
    @Test
    public void testScanFilterDuplicateAnd() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE deptno > 20 AND deptno > 20 AND deptno > 20
        final AlgBuilder builder = createAlgBuilder();
        builder.scan( "employee" );
        final RexNode condition = builder.call(
                OperatorRegistry.get( OperatorName.GREATER_THAN ),
                builder.field( "deptno" ),
                builder.literal( 20 ) );
        final RexNode condition2 = builder.call(
                OperatorRegistry.get( OperatorName.LESS_THAN ),
                builder.field( "deptno" ),
                builder.literal( 30 ) );
        final AlgNode root = builder.filter( condition, condition, condition )
                .build();
        final String expected = "LogicalFilter(model=[RELATIONAL], condition=[>($7, 20)])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );

        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE deptno > 20 AND deptno < 30 AND deptno > 20
        final AlgNode root2 = builder.scan( "employee" )
                .filter( condition, condition2, condition, condition )
                .build();
        final String expected2 = ""
                + "LogicalFilter(model=[RELATIONAL], condition=[AND(>($7, 20), <($7, 30))])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root2, Matchers.hasTree( expected2 ) );
    }


    @Test
    public void testBadFieldName() {
        final AlgBuilder builder = createAlgBuilder();
        try {
            RexInputRef ref = builder.scan( "employee" ).field( "foo" );
            fail( "expected error, got " + ref );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "field [foo] not found; input fields are: [empid, ename, job, mgr, hiredate, salary, commission, deptno]" ) );
        }
    }


    @Test
    public void testBadFieldOrdinal() {
        final AlgBuilder builder = createAlgBuilder();
        try {
            RexInputRef ref = builder.scan( "department" ).field( 20 );
            fail( "expected error, got " + ref );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "field ordinal [20] out of range; input fields are: [deptno, name, loc]" ) );
        }
    }


    @Test
    public void testBadType() {
        final AlgBuilder builder = createAlgBuilder();
        try {
            builder.scan( "employee" );
            RexNode call = builder.call( OperatorRegistry.get( OperatorName.PLUS ), builder.field( 1 ), builder.field( 3 ) );
            fail( "expected error, got " + call );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "Cannot infer return type for +; operand types: [VARCHAR(20), INTEGER]" ) );
        }
    }


    @Test
    public void testProject() {
        // Equivalent SQL:
        //   SELECT deptno, CAST(commission AS SMALLINT) AS commission, 20 AS $f2,
        //     commission AS commission3, commission AS c
        //   FROM emp
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .project(
                                builder.field( "deptno" ),
                                builder.cast( builder.field( 6 ), PolyType.SMALLINT ),
                                builder.literal( 20 ),
                                builder.field( 6 ),
                                builder.alias( builder.field( 6 ), "C" ) )
                        .build();
        // Note: CAST(commission) gets the commission alias because it occurs first
        // Note: AS(commission, C) becomes just $6
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], deptno=[$7], commission=[CAST($6):SMALLINT NOT NULL], $f2=[20], commission0=[$6], C=[$6])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Tests each method that creates a scalar expression.
     */
    @Test
    @Ignore
    public void testProject2() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .project(
                                builder.field( "deptno" ),
                                builder.cast( builder.field( 6 ), PolyType.INTEGER ),
                                builder.or(
                                        builder.equals( builder.field( "deptno" ), builder.literal( 20 ) ),
                                        builder.and(
                                                builder.literal( null ),
                                                builder.equals( builder.field( "deptno" ), builder.literal( 10 ) ),
                                                builder.and( builder.isNull( builder.field( 6 ) ), builder.not( builder.isNotNull( builder.field( 7 ) ) ) ) ),
                                        builder.equals(
                                                builder.field( "deptno" ),
                                                builder.literal( 20 ) ),
                                        builder.equals(
                                                builder.field( "deptno" ),
                                                builder.literal( 30 ) ) ),
                                builder.alias( builder.isNull( builder.field( 2 ) ), "n2" ),
                                builder.alias( builder.isNotNull( builder.field( 3 ) ), "nn2" ),
                                builder.literal( 20 ),
                                builder.field( 6 ),
                                builder.alias( builder.field( 6 ), "C" ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], deptno=[$7], commission=[CAST($6):INTEGER NOT NULL],"
                + " $f2=[OR(=($7, 20), AND(null:NULL, =($7, 10), IS NULL($6),"
                + " IS NULL($7)), =($7, 30))], n2=[IS NULL($2)],"
                + " nn2=[IS NOT NULL($3)], $f5=[20], commission0=[$6], C=[$6])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testProjectIdentity() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "department" )
                        .project( builder.fields( Mappings.bijection( Arrays.asList( 0, 1, 2 ) ) ) )
                        .build();
        final String expected = "LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Test case for "AlgBuilder does not translate identity projects even if they rename fields".
     */
    @Test
    public void testProjectIdentityWithFieldsRename() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "department" )
                        .project(
                                builder.alias( builder.field( 0 ), "a" ),
                                builder.alias( builder.field( 1 ), "b" ),
                                builder.alias( builder.field( 2 ), "c" ) )
                        .as( "t1" )
                        .project( builder.field( "a" ), builder.field( "t1", "c" ) )
                        .build();
        final String expected = "LogicalProject(model=[RELATIONAL], a=[$0], c=[$2])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Variation on {@link #testProjectIdentityWithFieldsRename}: don't use a table alias, and make sure the field names propagate through a filter.
     */
    @Test
    public void testProjectIdentityWithFieldsRenameFilter() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "department" )
                        .project(
                                builder.alias( builder.field( 0 ), "a" ),
                                builder.alias( builder.field( 1 ), "b" ),
                                builder.alias( builder.field( 2 ), "c" ) )
                        .filter(
                                builder.call( OperatorRegistry.get( OperatorName.EQUALS ), builder.field( "a" ), builder.literal( 20 ) ) )
                        .aggregate(
                                builder.groupKey( 0, 1, 2 ),
                                builder.aggregateCall( OperatorRegistry.getAgg( OperatorName.SUM ), builder.field( 0 ) ) )
                        .project(
                                builder.field( "c" ),
                                builder.field( "a" ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], c=[$2], a=[$0])\n"
                + "  LogicalAggregate(model=[RELATIONAL], group=[{0, 1, 2}], agg#0=[SUM($0)])\n"
                + "    LogicalFilter(model=[RELATIONAL], condition=[=($0, 20)])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testProjectLeadingEdge() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .project( builder.fields( Mappings.bijection( Arrays.asList( 0, 1, 2 ) ) ) )
                        .build();
        final String expected = "LogicalProject(model=[RELATIONAL], empid=[$0], ename=[$1], job=[$2])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    private void project1( int value, PolyType polyType, String message, String expected ) {
        final AlgBuilder builder = createAlgBuilder();
        RexBuilder rex = builder.getRexBuilder();
        AlgNode actual =
                builder.values( new String[]{ "x" }, 42 )
                        .empty()
                        .project( rex.makeLiteral( value, rex.getTypeFactory().createPolyType( polyType ), false ) )
                        .build();
        assertThat( message, actual, Matchers.hasTree( expected ) );
    }


    @Test
    public void testProject1asInt() {
        project1( 1, PolyType.INTEGER,
                "project(1 as INT) might omit type of 1 in the output plan as it is convention to omit INTEGER for integer literals",
                "LogicalProject(model=[RELATIONAL], $f0=[1])\n"
                        + "  LogicalValues(model=[RELATIONAL], tuples=[[]])\n" );
    }


    @Test
    public void testProject1asBigInt() {
        project1( 1, PolyType.BIGINT, "project(1 as BIGINT) should contain type of 1 in the output plan since the convention is to omit type of INTEGER",
                "LogicalProject(model=[RELATIONAL], $f0=[1:BIGINT])\n"
                        + "  LogicalValues(model=[RELATIONAL], tuples=[[]])\n" );
    }


    @Test
    public void testRename() {
        final AlgBuilder builder = createAlgBuilder();

        // No rename necessary (null name is ignored)
        AlgNode root =
                builder.scan( "department" )
                        .rename( Arrays.asList( "deptno", null ) )
                        .build();
        final String expected = "LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );

        // No rename necessary (prefix matches)
        root =
                builder.scan( "department" )
                        .rename( ImmutableList.of( "deptno" ) )
                        .build();
        assertThat( root, Matchers.hasTree( expected ) );

        // Add project to rename fields
        root =
                builder.scan( "department" )
                        .rename( Arrays.asList( "NAME", null, "deptno" ) )
                        .build();
        final String expected2 = ""
                + "LogicalProject(model=[RELATIONAL], NAME=[$0], name=[$1], deptno=[$2])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected2 ) );

        // If our requested list has non-unique names, we might get the same field names we started with. Don't add a useless project.
        root =
                builder.scan( "department" )
                        .rename( Arrays.asList( "deptno", null, "deptno" ) )
                        .build();
        final String expected3 = ""
                + "LogicalProject(model=[RELATIONAL], deptno=[$0], name=[$1], deptno0=[$2])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected3 ) );
        root =
                builder.scan( "department" )
                        .rename( Arrays.asList( "deptno", null, "deptno" ) )
                        .rename( Arrays.asList( "deptno", null, "deptno" ) )
                        .build();
        // No extra Project
        assertThat( root, Matchers.hasTree( expected3 ) );

        // Name list too long
        try {
            root =
                    builder.scan( "department" )
                            .rename( ImmutableList.of( "NAME", "deptno", "Y", "Z" ) )
                            .build();
            fail( "expected error, got " + root );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "More names than fields" ) );
        }
    }


    @Test
    public void testRenameValues() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.values( new String[]{ "a", "b" }, true, 1, false, -50 )
                        .build();
        final String expected = "LogicalValues(model=[RELATIONAL], tuples=[[{ true, 1 }, { false, -50 }]])\n";
        assertThat( root, Matchers.hasTree( expected ) );

        // When you rename Values, you get a Values with a new row type, no Project
        root =
                builder.push( root )
                        .rename( ImmutableList.of( "x", "y z" ) )
                        .build();
        assertThat( root, Matchers.hasTree( expected ) );
        assertThat( root.getRowType().getFieldNames().toString(), is( "[x, y z]" ) );
    }


    @Test
    public void testPermute() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .permute( Mappings.bijection( Arrays.asList( 1, 2, 0 ) ) )
                        .build();
        final String expected = "LogicalProject(model=[RELATIONAL], job=[$2], empid=[$0], ename=[$1])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testConvert() {
        final AlgBuilder builder = createAlgBuilder();
        AlgDataType rowType =
                builder.getTypeFactory().builder()
                        .add( "a", null, PolyType.BIGINT )
                        .add( "b", null, PolyType.VARCHAR, 10 )
                        .add( "c", null, PolyType.VARCHAR, 10 )
                        .build();
        AlgNode root =
                builder.scan( "department" )
                        .convert( rowType, false )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], deptno=[CAST($0):BIGINT NOT NULL], name=[CAST($1):VARCHAR(10) NOT NULL], loc=[CAST($2):VARCHAR(10) NOT NULL])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testConvertRename() {
        final AlgBuilder builder = createAlgBuilder();
        AlgDataType rowType =
                builder.getTypeFactory().builder()
                        .add( "a", null, PolyType.BIGINT )
                        .add( "b", null, PolyType.VARCHAR, 10 )
                        .add( "c", null, PolyType.VARCHAR, 10 )
                        .build();
        AlgNode root =
                builder.scan( "department" )
                        .convert( rowType, true )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], a=[CAST($0):BIGINT NOT NULL], b=[CAST($1):VARCHAR(10) NOT NULL], c=[CAST($2):VARCHAR(10) NOT NULL])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAggregate() {
        // Equivalent SQL:
        //   SELECT COUNT(DISTINCT deptno) AS c
        //   FROM emp
        //   GROUP BY ()
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .aggregate( builder.groupKey(), builder.count( true, "C", builder.field( "deptno" ) ) )
                        .build();
        final String expected = ""
                + "LogicalAggregate(model=[RELATIONAL], group=[{}], C=[COUNT(DISTINCT $7)])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAggregate2() {
        // Equivalent SQL:
        //   SELECT COUNT(*) AS c, SUM(mgr + 1) AS s
        //   FROM emp
        //   GROUP BY ename, hiredate + mgr
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .aggregate(
                                builder.groupKey(
                                        builder.field( 1 ),
                                        builder.call(
                                                OperatorRegistry.get( OperatorName.PLUS ),
                                                builder.field( 4 ),
                                                builder.field( 3 ) ),
                                        builder.field( 1 ) ),
                                builder.countStar( "C" ),
                                builder.sum(
                                        builder.call( OperatorRegistry.get( OperatorName.PLUS ), builder.field( 3 ),
                                                builder.literal( 1 ) ) ).as( "S" ) )
                        .build();
        final String expected = ""
                + "LogicalAggregate(model=[RELATIONAL], group=[{1, 8}], C=[COUNT()], S=[SUM($9)])\n"
                + "  LogicalProject(model=[RELATIONAL], empid=[$0], ename=[$1], job=[$2], mgr=[$3], hiredate=[$4], salary=[$5], commission=[$6], deptno=[$7], $f8=[+($4, $3)], $f9=[+($3, 1)])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Test case for "AlgBuilder wrongly skips creation of Aggregate that prunes columns if input is unique".
     */
    @Test
    public void testAggregate3() {
        // Equivalent SQL:
        //   SELECT DISTINCT deptno FROM (
        //     SELECT deptno, COUNT(*)
        //     FROM emp
        //     GROUP BY deptno)
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .aggregate( builder.groupKey( builder.field( 1 ) ), builder.count().as( "C" ) )
                        .aggregate( builder.groupKey( builder.field( 0 ) ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], ename=[$0])\n"
                + "  LogicalAggregate(model=[RELATIONAL], group=[{1}], C=[COUNT()])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * As {@link #testAggregate3()} but with Filter.
     */
    @Test
    public void testAggregate4() {
        // Equivalent SQL:
        //   SELECT DISTINCT deptno FROM (
        //     SELECT deptno, COUNT(*)
        //     FROM emp
        //     GROUP BY deptno
        //     HAVING COUNT(*) > 3)
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .aggregate(
                                builder.groupKey( builder.field( 1 ) ),
                                builder.count().as( "C" ) )
                        .filter(
                                builder.call( OperatorRegistry.get( OperatorName.GREATER_THAN ), builder.field( 1 ), builder.literal( 3 ) ) )
                        .aggregate(
                                builder.groupKey( builder.field( 0 ) ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], ename=[$0])\n"
                + "  LogicalFilter(model=[RELATIONAL], condition=[>($1, 3)])\n"
                + "    LogicalAggregate(model=[RELATIONAL], group=[{1}], C=[COUNT()])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAggregateFilter() {
        // Equivalent SQL:
        //   SELECT deptno, COUNT(*) FILTER (WHERE empid > 100) AS c
        //   FROM emp
        //   GROUP BY ROLLUP(deptno)
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .aggregate(
                                builder.groupKey( ImmutableBitSet.of( 7 ), ImmutableList.of( ImmutableBitSet.of( 7 ), ImmutableBitSet.of() ) ),
                                builder.count()
                                        .filter(
                                                builder.call(
                                                        OperatorRegistry.get( OperatorName.GREATER_THAN ),
                                                        builder.field( "empid" ),
                                                        builder.literal( 100 ) ) )
                                        .as( "C" ) )
                        .build();
        final String expected = ""
                + "LogicalAggregate(model=[RELATIONAL], group=[{7}], groups=[[{7}, {}]], C=[COUNT() FILTER $8])\n"
                + "  LogicalProject(model=[RELATIONAL], empid=[$0], ename=[$1], job=[$2], mgr=[$3], hiredate=[$4], salary=[$5], commission=[$6], deptno=[$7], $f8=[>($0, 100)])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAggregateFilterFails() {
        // Equivalent SQL:
        //   SELECT deptno, SUM(salary) FILTER (WHERE commission) AS c
        //   FROM emp
        //   GROUP BY deptno
        try {
            final AlgBuilder builder = createAlgBuilder();
            AlgNode root =
                    builder.scan( "employee" )
                            .aggregate(
                                    builder.groupKey( builder.field( "deptno" ) ),
                                    builder.sum( builder.field( "salary" ) )
                                            .filter( builder.field( "commission" ) )
                                            .as( "C" ) )
                            .build();
            fail( "expected error, got " + root );
        } catch ( PolyphenyDbException e ) {
            assertThat(
                    e.getMessage(),
                    is( "FILTER expression must be of type BOOLEAN" ) );
        }
    }


    @Test
    public void testAggregateFilterNullable() {
        // Equivalent SQL:
        //   SELECT deptno, SUM(salary) FILTER (WHERE commission < 100) AS c
        //   FROM emp
        //   GROUP BY deptno
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .aggregate(
                                builder.groupKey( builder.field( "deptno" ) ),
                                builder.sum( builder.field( "salary" ) )
                                        .filter( builder.call( OperatorRegistry.get( OperatorName.LESS_THAN ), builder.field( "commission" ), builder.literal( 100 ) ) )
                                        .as( "C" ) )
                        .build();
        final String expected = ""
                + "LogicalAggregate(model=[RELATIONAL], group=[{7}], C=[SUM($5) FILTER $8])\n"
                + "  LogicalProject(model=[RELATIONAL], empid=[$0], ename=[$1], job=[$2], mgr=[$3], hiredate=[$4], salary=[$5], commission=[$6], deptno=[$7], $f8=[IS TRUE(<($6, 100))])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Test case for "AlgBuilder gives NPE if groupKey contains alias".
     *
     * Now, the alias does not cause a new expression to be added to the input, but causes the referenced fields to be renamed.
     */
    @Test
    public void testAggregateProjectWithAliases() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .project( builder.field( "deptno" ) )
                        .aggregate( builder.groupKey( builder.alias( builder.field( "deptno" ), "departmentNo" ) ) )
                        .build();
        final String expected = ""
                + "LogicalAggregate(model=[RELATIONAL], group=[{0}])\n"
                + "  LogicalProject(model=[RELATIONAL], departmentNo=[$7])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAggregateProjectWithExpression() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .project( builder.field( "deptno" ) )
                        .aggregate(
                                builder.groupKey(
                                        builder.alias(
                                                builder.call( OperatorRegistry.get( OperatorName.PLUS ), builder.field( "deptno" ), builder.literal( 3 ) ),
                                                "d3" ) ) )
                        .build();
        final String expected = ""
                + "LogicalAggregate(model=[RELATIONAL], group=[{1}])\n"
                + "  LogicalProject(model=[RELATIONAL], deptno=[$7], d3=[+($7, 3)])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAggregateGroupingKeyOutOfRangeFails() {
        final AlgBuilder builder = createAlgBuilder();
        try {
            AlgNode root =
                    builder.scan( "employee" )
                            .aggregate( builder.groupKey( ImmutableBitSet.of( 17 ) ) )
                            .build();
            fail( "expected error, got " + root );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "out of bounds: {17}" ) );
        }
    }


    @Test
    public void testAggregateGroupingSetNotSubsetFails() {
        final AlgBuilder builder = createAlgBuilder();
        try {
            AlgNode root =
                    builder.scan( "employee" )
                            .aggregate( builder.groupKey( ImmutableBitSet.of( 7 ), ImmutableList.of( ImmutableBitSet.of( 4 ), ImmutableBitSet.of() ) ) )
                            .build();
            fail( "expected error, got " + root );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "group set element [$4] must be a subset of group key" ) );
        }
    }


    @Test
    public void testAggregateGroupingSetDuplicateIgnored() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .aggregate(
                                builder.groupKey(
                                        ImmutableBitSet.of( 7, 6 ),
                                        ImmutableList.of( ImmutableBitSet.of( 7 ), ImmutableBitSet.of( 6 ), ImmutableBitSet.of( 7 ) ) ) )
                        .build();
        final String expected = ""
                + "LogicalAggregate(model=[RELATIONAL], group=[{6, 7}], groups=[[{6}, {7}]])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAggregateGrouping() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .aggregate(
                                builder.groupKey( 6, 7 ),
                                builder.aggregateCall( OperatorRegistry.getAgg( OperatorName.GROUPING ), builder.field( "deptno" ) ).as( "g" ) )
                        .build();
        final String expected = ""
                + "LogicalAggregate(model=[RELATIONAL], group=[{6, 7}], g=[GROUPING($7)])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAggregateGroupingWithDistinctFails() {
        final AlgBuilder builder = createAlgBuilder();
        try {
            AlgNode root =
                    builder.scan( "employee" )
                            .aggregate(
                                    builder.groupKey( 6, 7 ),
                                    builder.aggregateCall( OperatorRegistry.getAgg( OperatorName.GROUPING ), builder.field( "deptno" ) )
                                            .distinct( true )
                                            .as( "g" ) )
                            .build();
            fail( "expected error, got " + root );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "DISTINCT not allowed" ) );
        }
    }


    @Test
    public void testAggregateGroupingWithFilterFails() {
        final AlgBuilder builder = createAlgBuilder();
        try {
            AlgNode root =
                    builder.scan( "employee" )
                            .aggregate(
                                    builder.groupKey( 6, 7 ),
                                    builder.aggregateCall(
                                                    OperatorRegistry.getAgg( OperatorName.GROUPING ),
                                                    builder.field( "deptno" ) )
                                            .filter( builder.literal( true ) )
                                            .as( "g" ) )
                            .build();
            fail( "expected error, got " + root );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "FILTER not allowed" ) );
        }
    }


    @Test
    public void testDistinct() {
        // Equivalent SQL:
        //   SELECT DISTINCT deptno
        //   FROM emp
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .project( builder.field( "deptno" ) )
                        .distinct()
                        .build();
        final String expected = "LogicalAggregate(model=[RELATIONAL], group=[{0}])\n"
                + "  LogicalProject(model=[RELATIONAL], deptno=[$7])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    @Ignore
    public void testDistinctAlready() {
        // department is already distinct
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "department" )
                        .distinct()
                        .build();
        final String expected = "LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testDistinctEmpty() {
        // Is a relation with zero columns distinct? What about if we know there are zero rows? It is a matter of definition: there are no duplicate rows, but applying "select ... group by ()" to it would change the result.
        // In theory, we could omit the distinct if we know there is precisely one row, but we don't currently.
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .filter( builder.call( OperatorRegistry.get( OperatorName.IS_NULL ), builder.field( "commission" ) ) )
                        .project()
                        .distinct()
                        .build();
        final String expected = "LogicalAggregate(model=[RELATIONAL], group=[{}])\n"
                + "  LogicalProject(model=[RELATIONAL])\n"
                + "    LogicalFilter(model=[RELATIONAL], condition=[IS NULL($6)])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testUnion() {
        // Equivalent SQL:
        //   SELECT deptno FROM emp
        //   UNION ALL
        //   SELECT deptno FROM dept
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "department" )
                        .project( builder.field( "deptno" ) )
                        .scan( "employee" )
                        .filter( builder.call( OperatorRegistry.get( OperatorName.EQUALS ), builder.field( "deptno" ), builder.literal( 20 ) ) )
                        .project( builder.field( "empid" ) )
                        .union( true )
                        .build();
        final String expected = ""
                + "LogicalUnion(model=[RELATIONAL], all=[true])\n"
                + "  LogicalProject(model=[RELATIONAL], deptno=[$0])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, department]])\n"
                + "  LogicalProject(model=[RELATIONAL], empid=[$0])\n"
                + "    LogicalFilter(model=[RELATIONAL], condition=[=($7, 20)])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Test case for SetOps with incompatible args
     */
    @Test
    public void testBadUnionArgsErrorMessage() {
        // Equivalent SQL:
        //   SELECT empid, SALARY FROM emp
        //   UNION ALL
        //   SELECT deptno FROM dept
        final AlgBuilder builder = createAlgBuilder();
        try {
            final AlgNode root =
                    builder.scan( "department" )
                            .project( builder.field( "deptno" ) )
                            .scan( "employee" )
                            .project( builder.field( "empid" ), builder.field( "salary" ) )
                            .union( true )
                            .build();
            fail( "Expected error, got " + root );
        } catch ( IllegalArgumentException e ) {
            final String expected = "Cannot compute compatible row type for arguments to set op: RecordType(INTEGER deptno), RecordType(BIGINT empid, DECIMAL(7, 2) salary)";
            assertThat( e.getMessage(), is( expected ) );
        }
    }


    @Test
    public void testUnion3() {
        // Equivalent SQL:
        //   SELECT deptno FROM dept
        //   UNION ALL
        //   SELECT empid FROM emp
        //   UNION ALL
        //   SELECT deptno FROM emp
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "department" )
                        .project( builder.field( "deptno" ) )
                        .scan( "employee" )
                        .project( builder.field( "empid" ) )
                        .scan( "employee" )
                        .project( builder.field( "deptno" ) )
                        .union( true, 3 )
                        .build();
        final String expected = ""
                + "LogicalUnion(model=[RELATIONAL], all=[true])\n"
                + "  LogicalProject(model=[RELATIONAL], deptno=[$0])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, department]])\n"
                + "  LogicalProject(model=[RELATIONAL], empid=[$0])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "  LogicalProject(model=[RELATIONAL], deptno=[$7])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testUnion1() {
        // Equivalent SQL:
        //   SELECT deptno FROM dept
        //   UNION ALL
        //   SELECT empid FROM emp
        //   UNION ALL
        //   SELECT deptno FROM emp
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "department" )
                        .project( builder.field( "deptno" ) )
                        .scan( "employee" )
                        .project( builder.field( "empid" ) )
                        .scan( "employee" )
                        .project( builder.field( "deptno" ) )
                        .union( true, 1 )
                        .build();
        final String expected = "LogicalProject(model=[RELATIONAL], deptno=[$7])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testIntersect() {
        // Equivalent SQL:
        //   SELECT empid FROM emp
        //   WHERE deptno = 20
        //   INTERSECT
        //   SELECT deptno FROM dept
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "department" )
                        .project( builder.field( "deptno" ) )
                        .scan( "employee" )
                        .filter(
                                builder.call(
                                        OperatorRegistry.get( OperatorName.EQUALS ),
                                        builder.field( "deptno" ),
                                        builder.literal( 20 ) ) )
                        .project( builder.field( "empid" ) )
                        .intersect( false )
                        .build();
        final String expected = ""
                + "LogicalIntersect(model=[RELATIONAL], all=[false])\n"
                + "  LogicalProject(model=[RELATIONAL], deptno=[$0])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, department]])\n"
                + "  LogicalProject(model=[RELATIONAL], empid=[$0])\n"
                + "    LogicalFilter(model=[RELATIONAL], condition=[=($7, 20)])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testIntersect3() {
        // Equivalent SQL:
        //   SELECT deptno FROM dept
        //   INTERSECT ALL
        //   SELECT empid FROM emp
        //   INTERSECT ALL
        //   SELECT deptno FROM emp
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "department" )
                        .project( builder.field( "deptno" ) )
                        .scan( "employee" )
                        .project( builder.field( "empid" ) )
                        .scan( "employee" )
                        .project( builder.field( "deptno" ) )
                        .intersect( true, 3 )
                        .build();
        final String expected = ""
                + "LogicalIntersect(model=[RELATIONAL], all=[true])\n"
                + "  LogicalProject(model=[RELATIONAL], deptno=[$0])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, department]])\n"
                + "  LogicalProject(model=[RELATIONAL], empid=[$0])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "  LogicalProject(model=[RELATIONAL], deptno=[$7])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testExcept() {
        // Equivalent SQL:
        //   SELECT empid FROM emp
        //   WHERE deptno = 20
        //   MINUS
        //   SELECT deptno FROM dept
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "department" )
                        .project( builder.field( "deptno" ) )
                        .scan( "employee" )
                        .filter(
                                builder.call(
                                        OperatorRegistry.get( OperatorName.EQUALS ),
                                        builder.field( "deptno" ),
                                        builder.literal( 20 ) ) )
                        .project( builder.field( "empid" ) )
                        .minus( false )
                        .build();
        final String expected = ""
                + "LogicalMinus(model=[RELATIONAL], all=[false])\n"
                + "  LogicalProject(model=[RELATIONAL], deptno=[$0])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, department]])\n"
                + "  LogicalProject(model=[RELATIONAL], empid=[$0])\n"
                + "    LogicalFilter(model=[RELATIONAL], condition=[=($7, 20)])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testJoin() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM (SELECT * FROM employee WHERE commission IS NULL)
        //   JOIN dept ON emp.deptno = dept.deptno
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .filter(
                                builder.call(
                                        OperatorRegistry.get( OperatorName.IS_NULL ),
                                        builder.field( "commission" ) ) )
                        .scan( "department" )
                        .join(
                                JoinAlgType.INNER,
                                builder.call(
                                        OperatorRegistry.get( OperatorName.EQUALS ),
                                        builder.field( 2, 0, "deptno" ),
                                        builder.field( 2, 1, "deptno" ) ) )
                        .build();
        final String expected = ""
                + "LogicalJoin(model=[RELATIONAL], condition=[=($7, $8)], joinType=[inner])\n"
                + "  LogicalFilter(model=[RELATIONAL], condition=[IS NULL($6)])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Same as {@link #testJoin} using USING.
     */
    @Test
    public void testJoinUsing() {
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root2 =
                builder.scan( "employee" )
                        .filter( builder.call( OperatorRegistry.get( OperatorName.IS_NULL ), builder.field( "commission" ) ) )
                        .scan( "department" )
                        .join( JoinAlgType.INNER, "deptno" )
                        .build();
        final String expected = ""
                + "LogicalJoin(model=[RELATIONAL], condition=[=($7, $8)], joinType=[inner])\n"
                + "  LogicalFilter(model=[RELATIONAL], condition=[IS NULL($6)])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root2, Matchers.hasTree( expected ) );
    }


    @Test
    public void testJoin2() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   LEFT JOIN dept ON emp.deptno = dept.deptno
        //     AND emp.empid = 123
        //     AND dept.deptno IS NOT NULL
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .scan( "department" )
                        .join(
                                JoinAlgType.LEFT,
                                builder.call(
                                        OperatorRegistry.get( OperatorName.EQUALS ),
                                        builder.field( 2, 0, "deptno" ),
                                        builder.field( 2, 1, "deptno" ) ),
                                builder.call(
                                        OperatorRegistry.get( OperatorName.EQUALS ),
                                        builder.field( 2, 0, "empid" ),
                                        builder.literal( 123 ) ),
                                builder.call(
                                        OperatorRegistry.get( OperatorName.IS_NOT_NULL ),
                                        builder.field( 2, 1, "deptno" ) ) )
                        .build();
        // Note that "dept.deptno IS NOT NULL" has been simplified away.
        final String expected = ""
                + "LogicalJoin(model=[RELATIONAL], condition=[AND(=($7, $8), =($0, 123))], joinType=[left])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testJoinCartesian() {
        // Equivalent SQL:
        //   SELECT * employee CROSS JOIN dept
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .scan( "department" )
                        .join( JoinAlgType.INNER )
                        .build();
        final String expected =
                "LogicalJoin(model=[RELATIONAL], condition=[true], joinType=[inner])\n"
                        + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                        + "  LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testCorrelationFails() {
        final AlgBuilder builder = createAlgBuilder();
        final Holder<RexCorrelVariable> v = Holder.of( null );
        try {
            builder.scan( "employee" )
                    .variable( v )
                    .filter( builder.equals( builder.field( 0 ), v.get() ) )
                    .scan( "department" )
                    .join( JoinAlgType.INNER, builder.literal( true ),
                            ImmutableSet.of( v.get().id ) );
            fail( "expected error" );
        } catch ( IllegalArgumentException e ) {
            assertThat(
                    e.getMessage(),
                    containsString( "variable $cor0 must not be used by left input to correlation" ) );
        }
    }


    @Test
    public void testCorrelationWithCondition() {
        final AlgBuilder builder = createAlgBuilder();
        final Holder<RexCorrelVariable> v = Holder.of( null );
        AlgNode root = builder.scan( "employee" )
                .variable( v )
                .scan( "department" )
                .filter( builder.equals( builder.field( 0 ), builder.field( v.get(), "deptno" ) ) )
                .join(
                        JoinAlgType.LEFT,
                        builder.equals( builder.field( 2, 0, "salary" ), builder.literal( 1000 ) ),
                        ImmutableSet.of( v.get().id ) )
                .build();
        // Note that the join filter gets pushed to the right-hand input of LogicalCorrelate
        final String expected = ""
                + "LogicalCorrelate(model=[RELATIONAL], correlation=[$cor0], joinType=[left], requiredColumns=[{7}])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "  LogicalFilter(model=[RELATIONAL], condition=[=($cor0.salary, 1000)])\n"
                + "    LogicalFilter(model=[RELATIONAL], condition=[=($0, $cor0.deptno)])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAlias() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM employee AS e, dept
        //   WHERE e.deptno = department.deptno
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "e" )
                        .scan( "department" )
                        .join( JoinAlgType.LEFT )
                        .filter( builder.equals( builder.field( "e", "deptno" ), builder.field( "department", "deptno" ) ) )
                        .project( builder.field( "e", "ename" ), builder.field( "department", "name" ) )
                        .build();
        final String expected = "LogicalProject(model=[RELATIONAL], ename=[$1], name=[$9])\n"
                + "  LogicalFilter(model=[RELATIONAL], condition=[=($7, $8)])\n"
                + "    LogicalJoin(model=[RELATIONAL], condition=[true], joinType=[left])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
        final AlgDataTypeField field = root.getRowType().getFieldList().get( 1 );
        assertThat( field.getName(), is( "name" ) );
        assertThat( field.getType().isNullable(), is( true ) );
    }


    @Test
    public void testAlias2() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM employee AS e, employee as m, dept
        //   WHERE e.deptno = dept.deptno
        //   AND m.empid = e.mgr
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "e" )
                        .scan( "employee" )
                        .as( "m" )
                        .scan( "department" )
                        .join( JoinAlgType.INNER )
                        .join( JoinAlgType.INNER )
                        .filter(
                                builder.equals( builder.field( "e", "deptno" ), builder.field( "department", "deptno" ) ),
                                builder.equals( builder.field( "m", "empid" ), builder.field( "e", "mgr" ) ) )
                        .build();
        final String expected = ""
                + "LogicalFilter(model=[RELATIONAL], condition=[AND(=($7, $16), =($8, $3))])\n"
                + "  LogicalJoin(model=[RELATIONAL], condition=[true], joinType=[inner])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "    LogicalJoin(model=[RELATIONAL], condition=[true], joinType=[inner])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAliasSort() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "e" )
                        .sort( 0 )
                        .project( builder.field( "e", "empid" ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], empid=[$0])\n"
                + "  LogicalSort(model=[RELATIONAL], sort0=[$0], dir0=[ASC])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAliasLimit() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "e" )
                        .sort( 1 )
                        .sortLimit( 10, 20 ) // aliases were lost here if preceded by sort()
                        .project( builder.field( "e", "empid" ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], empid=[$0])\n"
                + "  LogicalSort(model=[RELATIONAL], sort0=[$1], dir0=[ASC], offset=[10], fetch=[20])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Test case for "AlgBuilder's project() doesn't preserve alias".
     */
    @Test
    public void testAliasProject() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "employee_alias" )
                        .project( builder.field( "deptno" ), builder.literal( 20 ) )
                        .project( builder.field( "employee_alias", "deptno" ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], deptno=[$7])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Tests that table aliases are propagated even when there is a project on top of a project. (Aliases tend to get lost when projects are merged).
     */
    @Test
    public void testAliasProjectProject() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "employee_alias" )
                        .project(
                                builder.field( "deptno" ),
                                builder.literal( 20 ) )
                        .project(
                                builder.field( 1 ),
                                builder.literal( 10 ),
                                builder.field( 0 ) )
                        .project(
                                builder.alias(
                                        builder.field( 1 ), "sum" ),
                                builder.field( "employee_alias", "deptno" ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], sum=[10], deptno=[$7])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Tests that table aliases are propagated and are available to a filter, even when there is a project on top of a project. (Aliases tend to get lost when projects are merged).
     */
    @Test
    public void testAliasFilter() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "employee_alias" )
                        .project(
                                builder.field( "deptno" ),
                                builder.literal( 20 ) )
                        .project(
                                builder.field( 1 ), // literal 20
                                builder.literal( 10 ),
                                builder.field( 0 ) ) // deptno
                        .filter(
                                builder.call(
                                        OperatorRegistry.get( OperatorName.GREATER_THAN ),
                                        builder.field( 1 ),
                                        builder.field( "employee_alias", "deptno" ) ) )
                        .build();
        final String expected = ""
                + "LogicalFilter(model=[RELATIONAL], condition=[>($1, $2)])\n"
                + "  LogicalProject(model=[RELATIONAL], $f1=[20], $f12=[10], deptno=[$7])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testAliasAggregate() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "employee_alias" )
                        .project(
                                builder.field( "deptno" ),
                                builder.literal( 20 ) )
                        .aggregate(
                                builder.groupKey( builder.field( "employee_alias", "deptno" ) ),
                                builder.sum( builder.field( 1 ) ) )
                        .project(
                                builder.alias( builder.field( 1 ), "sum" ),
                                builder.field( "employee_alias", "deptno" ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], sum=[$1], deptno=[$0])\n"
                + "  LogicalAggregate(model=[RELATIONAL], group=[{0}], agg#0=[SUM($1)])\n"
                + "    LogicalProject(model=[RELATIONAL], deptno=[$7], $f1=[20])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Tests that a projection retains field names after a join.
     */
    @Test
    public void testProjectJoin() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "e" )
                        .scan( "department" )
                        .join( JoinAlgType.INNER )
                        .project(
                                builder.field( "department", "deptno" ),
                                builder.field( 0 ),
                                builder.field( "e", "mgr" ) )
                        // essentially a no-op, was previously throwing exception due to project() using join-renamed fields
                        .project(
                                builder.field( "department", "deptno" ),
                                builder.field( 1 ),
                                builder.field( "e", "mgr" ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], deptno=[$8], empid=[$0], mgr=[$3])\n"
                + "  LogicalJoin(model=[RELATIONAL], condition=[true], joinType=[inner])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Tests that a projection after a projection.
     */
    @Test
    public void testProjectProject() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "e" )
                        .projectPlus(
                                builder.alias(
                                        builder.call(
                                                OperatorRegistry.get( OperatorName.PLUS ),
                                                builder.field( 0 ),
                                                builder.field( 3 ) ),
                                        "x" ) )
                        .project(
                                builder.field( "e", "deptno" ),
                                builder.field( 0 ),
                                builder.field( "e", "mgr" ),
                                Util.last( builder.fields() ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], deptno=[$7], empid=[$0], mgr=[$3], x=[+($0, $3)])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testMultiLevelAlias() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "e" )
                        .scan( "employee" )
                        .as( "m" )
                        .scan( "department" )
                        .join( JoinAlgType.INNER )
                        .join( JoinAlgType.INNER )
                        .project(
                                builder.field( "department", "deptno" ),
                                builder.field( 16 ),
                                builder.field( "m", "empid" ),
                                builder.field( "e", "mgr" ) )
                        .as( "all" )
                        .filter(
                                builder.call(
                                        OperatorRegistry.get( OperatorName.GREATER_THAN ),
                                        builder.field( "department", "deptno" ),
                                        builder.literal( 100 ) ) )
                        .project(
                                builder.field( "department", "deptno" ),
                                builder.field( "all", "empid" ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], deptno=[$0], empid=[$2])\n"
                + "  LogicalFilter(model=[RELATIONAL], condition=[>($0, 100)])\n"
                + "    LogicalProject(model=[RELATIONAL], deptno=[$16], deptno0=[$16], empid=[$8], mgr=[$3])\n"
                + "      LogicalJoin(model=[RELATIONAL], condition=[true], joinType=[inner])\n"
                + "        LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "        LogicalJoin(model=[RELATIONAL], condition=[true], joinType=[inner])\n"
                + "          LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "          LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testUnionAlias() {
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .as( "e1" )
                        .project(
                                builder.field( "empid" ),
                                builder.call(
                                        OperatorRegistry.get( OperatorName.CONCAT ),
                                        builder.field( "ename" ),
                                        builder.literal( "-1" ) ) )
                        .scan( "employee" )
                        .as( "e2" )
                        .project(
                                builder.field( "empid" ),
                                builder.call(
                                        OperatorRegistry.get( OperatorName.CONCAT ),
                                        builder.field( "ename" ),
                                        builder.literal( "-2" ) ) )
                        .union( false ) // aliases lost here
                        .project( builder.fields( Lists.newArrayList( 1, 0 ) ) )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], $f1=[$1], empid=[$0])\n"
                + "  LogicalUnion(model=[RELATIONAL], all=[false])\n"
                + "    LogicalProject(model=[RELATIONAL], empid=[$0], $f1=[||($1, '-1')])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "    LogicalProject(model=[RELATIONAL], empid=[$0], $f1=[||($1, '-2')])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Test case for "Add{@link AlgBuilder}  field() method to reference aliased relations not on top of stack", accessing tables aliased that are not accessible in the top AlgNode.
     */
    @Test
    public void testAliasPastTop() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   LEFT JOIN dept ON emp.deptno = dept.deptno
        //     AND emp.empid = 123
        //     AND dept.deptno IS NOT NULL
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" )
                        .scan( "department" )
                        .join(
                                JoinAlgType.LEFT,
                                builder.call(
                                        OperatorRegistry.get( OperatorName.EQUALS ),
                                        builder.field( 2, "employee", "deptno" ),
                                        builder.field( 2, "department", "deptno" ) ),
                                builder.call(
                                        OperatorRegistry.get( OperatorName.EQUALS ),
                                        builder.field( 2, "employee", "empid" ),
                                        builder.literal( 123 ) ) )
                        .build();
        final String expected = ""
                + "LogicalJoin(model=[RELATIONAL], condition=[AND(=($7, $8), =($0, 123))], joinType=[left])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * As {@link #testAliasPastTop()}.
     */
    @Test
    public void testAliasPastTop2() {
        // Equivalent SQL:
        //   SELECT t1.empid, t2.empid, t3.deptno
        //   FROM employee t1
        //   INNER JOIN employee t2 ON t1.empid = t2.empid
        //   INNER JOIN dept t3 ON t1.deptno = t3.deptno
        //     AND t2.job != t3.loc
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "employee" ).as( "t1" )
                        .scan( "employee" ).as( "t2" )
                        .join(
                                JoinAlgType.INNER,
                                builder.equals(
                                        builder.field( 2, "t1", "empid" ),
                                        builder.field( 2, "t2", "empid" ) ) )
                        .scan( "department" ).as( "t3" )
                        .join(
                                JoinAlgType.INNER,
                                builder.equals(
                                        builder.field( 2, "t1", "deptno" ),
                                        builder.field( 2, "t3", "deptno" ) ),
                                builder.not(
                                        builder.equals(
                                                builder.field( 2, "t2", "job" ),
                                                builder.field( 2, "t3", "loc" ) ) ) )
                        .build();
        // Cols:
        // 0-7   employee as t1
        // 8-15  employee as t2
        // 16-18 department as t3
        final String expected = ""
                + "LogicalJoin(model=[RELATIONAL], condition=[AND(=($7, $16), <>($10, $18))], joinType=[inner])\n"
                + "  LogicalJoin(model=[RELATIONAL], condition=[=($0, $8)], joinType=[inner])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testEmpty() {
        // Equivalent SQL:
        //   SELECT deptno, true FROM dept LIMIT 0
        // optimized to
        //   VALUES
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.scan( "department" )
                        .project(
                                builder.field( 0 ),
                                builder.literal( false ) )
                        .empty()
                        .build();
        final String expected = "LogicalValues(model=[RELATIONAL], tuples=[[]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
        final String expectedType = "RecordType(INTEGER NOT NULL deptno, BOOLEAN NOT NULL $f1) NOT NULL";
        assertThat( root.getRowType().getFullTypeString(), is( expectedType ) );
    }


    @Test
    public void testValues() {
        // Equivalent SQL:
        //   VALUES (true, 1), (false, -50) AS t(a, b)
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root = builder.values( new String[]{ "a", "b" }, true, 1, false, -50 ).build();
        final String expected = "LogicalValues(model=[RELATIONAL], tuples=[[{ true, 1 }, { false, -50 }]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
        final String expectedType = "RecordType(BOOLEAN NOT NULL a, INTEGER NOT NULL b) NOT NULL";
        assertThat( root.getRowType().getFullTypeString(), is( expectedType ) );
    }


    /**
     * Tests creating Values with some field names and some values null.
     */
    @Test
    public void testValuesNullable() {
        // Equivalent SQL:
        //   VALUES (null, 1, 'abc'), (false, null, 'longer string')
        final AlgBuilder builder = createAlgBuilder();
        AlgNode root =
                builder.values( new String[]{ "a", null, "c" }, null, 1, "abc", false, null, "longer string" ).build();
        final String expected = "LogicalValues(model=[RELATIONAL], tuples=[[{ null, 1, 'abc' }, { false, null, 'longer string' }]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
        final String expectedType = "RecordType(BOOLEAN a, INTEGER expr$1, CHAR(13) NOT NULL c) NOT NULL";
        assertThat( root.getRowType().getFullTypeString(), is( expectedType ) );
    }


    @Test
    public void testValuesBadNullFieldNames() {
        try {
            final AlgBuilder builder = createAlgBuilder();
            AlgBuilder root = builder.values( (String[]) null, "a", "b" );
            fail( "expected error, got " + root );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "Value count must be a positive multiple of field count" ) );
        }
    }


    @Test
    public void testValuesBadNoFields() {
        try {
            final AlgBuilder builder = createAlgBuilder();
            AlgBuilder root = builder.values( new String[0], 1, 2, 3 );
            fail( "expected error, got " + root );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "Value count must be a positive multiple of field count" ) );
        }
    }


    @Test
    public void testValuesBadNoValues() {
        try {
            final AlgBuilder builder = createAlgBuilder();
            AlgBuilder root = builder.values( new String[]{ "a", "b" } );
            fail( "expected error, got " + root );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "Value count must be a positive multiple of field count" ) );
        }
    }


    @Test
    public void testValuesBadOddMultiple() {
        try {
            final AlgBuilder builder = createAlgBuilder();
            AlgBuilder root = builder.values( new String[]{ "a", "b" }, 1, 2, 3, 4, 5 );
            fail( "expected error, got " + root );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "Value count must be a positive multiple of field count" ) );
        }
    }


    @Test
    public void testValuesBadAllNull() {
        try {
            final AlgBuilder builder = createAlgBuilder();
            AlgBuilder root = builder.values( new String[]{ "a", "b" }, null, null, 1, null );
            fail( "expected error, got " + root );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), is( "All values of field 'b' are null; cannot deduce type" ) );
        }
    }


    @Test
    public void testValuesAllNull() {
        final AlgBuilder builder = createAlgBuilder();
        AlgDataType rowType =
                builder.getTypeFactory().builder()
                        .add( "a", null, PolyType.BIGINT )
                        .add( "a", null, PolyType.VARCHAR, 10 )
                        .build();
        AlgNode root = builder.values( rowType, null, null, 1, null ).build();
        final String expected = "LogicalValues(model=[RELATIONAL], tuples=[[{ null, null }, { 1, null }]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
        final String expectedType = "RecordType(BIGINT NOT NULL a, VARCHAR(10) NOT NULL a) NOT NULL";
        assertThat( root.getRowType().getFullTypeString(), is( expectedType ) );
    }


    @Test
    public void testSort() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   ORDER BY 3. 1 DESC
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root =
                builder.scan( "employee" )
                        .sort( builder.field( 2 ), builder.desc( builder.field( 0 ) ) )
                        .build();
        final String expected = "LogicalSort(model=[RELATIONAL], sort0=[$2], sort1=[$0], dir0=[ASC], dir1=[DESC])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );

        // same result using ordinals
        final AlgNode root2 = builder.scan( "employee" ).sort( 2, -1 ).build();
        assertThat( root2, Matchers.hasTree( expected ) );
    }


    /**
     * Test case for "OFFSET 0 causes AssertionError".
     */
    @Test
    public void testTrivialSort() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   OFFSET 0
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root =
                builder.scan( "employee" )
                        .sortLimit( 0, -1, ImmutableList.of() )
                        .build();
        final String expected = "LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testSortDuplicate() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   ORDER BY empid DESC, deptno, empid ASC, hiredate
        //
        // The sort key "empid ASC" is unnecessary and is ignored.
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root =
                builder.scan( "employee" )
                        .sort(
                                builder.desc( builder.field( "empid" ) ),
                                builder.field( "deptno" ),
                                builder.field( "empid" ),
                                builder.field( "hiredate" ) )
                        .build();
        final String expected = "LogicalSort(model=[RELATIONAL], sort0=[$0], sort1=[$7], sort2=[$4], dir0=[DESC], dir1=[ASC], dir2=[ASC])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testSortByExpression() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   ORDER BY ename ASC NULLS LAST, hiredate + mgr DESC NULLS FIRST
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root =
                builder.scan( "employee" )
                        .sort(
                                builder.nullsLast( builder.desc( builder.field( 1 ) ) ),
                                builder.nullsFirst(
                                        builder.call(
                                                OperatorRegistry.get( OperatorName.PLUS ),
                                                builder.field( 4 ),
                                                builder.field( 3 ) ) ) )
                        .build();
        final String expected =
                "LogicalProject(model=[RELATIONAL], empid=[$0], ename=[$1], job=[$2], mgr=[$3], hiredate=[$4], salary=[$5], commission=[$6], deptno=[$7])\n"
                        + "  LogicalSort(model=[RELATIONAL], sort0=[$1], sort1=[$8], dir0=[DESC-nulls-last], dir1=[ASC-nulls-first])\n"
                        + "    LogicalProject(model=[RELATIONAL], empid=[$0], ename=[$1], job=[$2], mgr=[$3], hiredate=[$4], salary=[$5], commission=[$6], deptno=[$7], $f8=[+($4, $3)])\n"
                        + "      LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testLimit() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   OFFSET 2 FETCH 10
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root =
                builder.scan( "employee" )
                        .limit( 2, 10 )
                        .build();
        final String expected =
                "LogicalSort(model=[RELATIONAL], offset=[2], fetch=[10])\n"
                        + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testSortLimit() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   ORDER BY deptno DESC FETCH 10
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root =
                builder.scan( "employee" )
                        .sortLimit( -1, 10, builder.desc( builder.field( "deptno" ) ) )
                        .build();
        final String expected =
                "LogicalSort(model=[RELATIONAL], sort0=[$7], dir0=[DESC], fetch=[10])\n"
                        + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testSortLimit0() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   ORDER BY deptno DESC FETCH 0
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root =
                builder.scan( "employee" )
                        .sortLimit( -1, 0, builder.desc( builder.field( "deptno" ) ) )
                        .build();
        final String expected = "LogicalValues(model=[RELATIONAL], tuples=[[]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Test case for "AlgBuilder sort-combining optimization treats aliases incorrectly".
     */
    @Test
    public void testSortOverProjectSort() {
        final AlgBuilder builder = createAlgBuilder();
        builder.scan( "employee" )
                .sort( 0 )
                .project( builder.field( 1 ) )
                // was throwing exception here when attempting to apply to inner sort node
                .limit( 0, 1 )
                .build();
        AlgNode root = builder.scan( "employee" )
                .sort( 0 )
                .project( Lists.newArrayList( builder.field( 1 ) ), Lists.newArrayList( "F1" ) )
                .limit( 0, 1 )
                // make sure we can still access the field by alias
                .project( builder.field( "F1" ) )
                .build();
        String expected = "LogicalProject(model=[RELATIONAL], F1=[$1])\n"
                + "  LogicalSort(model=[RELATIONAL], sort0=[$0], dir0=[ASC], fetch=[1])\n"
                + "    LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    /**
     * Tests that a sort on a field followed by a limit gives the same effect as calling sortLimit.
     *
     * In general a relational operator cannot rely on the order of its input, but it is reasonable to merge sort and limit if they were created by consecutive builder operations. And clients such as Piglet rely on it.
     */
    @Test
    public void testSortThenLimit() {
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root =
                builder.scan( "employee" )
                        .sort( builder.desc( builder.field( "deptno" ) ) )
                        .limit( -1, 10 )
                        .build();
        final String expected = ""
                + "LogicalSort(model=[RELATIONAL], sort0=[$7], dir0=[DESC], fetch=[10])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );

        final AlgNode root2 =
                builder.scan( "employee" )
                        .sortLimit( -1, 10, builder.desc( builder.field( "deptno" ) ) )
                        .build();
        assertThat( root2, Matchers.hasTree( expected ) );
    }


    /**
     * Tests that a sort on an expression followed by a limit gives the same effect as calling sortLimit.
     */
    @Test
    public void testSortExpThenLimit() {
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root =
                builder.scan( "department" )
                        .sort(
                                builder.desc(
                                        builder.call(
                                                OperatorRegistry.get( OperatorName.PLUS ),
                                                builder.field( "deptno" ),
                                                builder.literal( 1 ) ) ) )
                        .limit( 3, 10 )
                        .build();
        final String expected = ""
                + "LogicalProject(model=[RELATIONAL], deptno=[$0], name=[$1], loc=[$2])\n"
                + "  LogicalSort(model=[RELATIONAL], sort0=[$3], dir0=[DESC], offset=[3], fetch=[10])\n"
                + "    LogicalProject(model=[RELATIONAL], deptno=[$0], name=[$1], loc=[$2], $f3=[+($0, 1)])\n"
                + "      LogicalScan(model=[RELATIONAL], table=[[public, department]])\n";
        assertThat( root, Matchers.hasTree( expected ) );

        final AlgNode root2 =
                builder.scan( "department" )
                        .sortLimit( 3, 10,
                                builder.desc(
                                        builder.call(
                                                OperatorRegistry.get( OperatorName.PLUS ),
                                                builder.field( "deptno" ),
                                                builder.literal( 1 ) ) ) )
                        .build();
        assertThat( root2, Matchers.hasTree( expected ) );
    }


    /**
     * Test case for "AlgBuilder.call throws NullPointerException if argument types are invalid".
     */
    @Test
    public void testTypeInferenceValidation() {
        final AlgBuilder builder = createAlgBuilder();
        // test for a) call(operator, Iterable<RexNode>)
        final RexNode arg0 = builder.literal( 0 );
        final RexNode arg1 = builder.literal( "xyz" );
        try {
            builder.call( OperatorRegistry.get( OperatorName.PLUS ), Lists.newArrayList( arg0, arg1 ) );
            fail( "Invalid combination of parameter types" );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), containsString( "Cannot infer return type" ) );
        }

        // test for b) call(operator, RexNode...)
        try {
            builder.call( OperatorRegistry.get( OperatorName.PLUS ), arg0, arg1 );
            fail( "Invalid combination of parameter types" );
        } catch ( IllegalArgumentException e ) {
            assertThat( e.getMessage(), containsString( "Cannot infer return type" ) );
        }
    }


    @Test
    public void testMatchRecognize() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   MATCH_RECOGNIZE (
        //     PARTITION BY deptno
        //     ORDER BY empid asc
        //     MEASURES
        //       STRT.mgr as start_nw,
        //       LAST(DOWN.mgr) as bottom_nw,
        //     PATTERN (STRT DOWN+ UP+) WITHIN INTERVAL '5' SECOND
        //     DEFINE
        //       DOWN as DOWN.mgr < PREV(DOWN.mgr),
        //       UP as UP.mgr > PREV(UP.mgr)
        //   )
        final AlgBuilder builder = createAlgBuilder().scan( "employee" );
        final AlgDataTypeFactory typeFactory = builder.getTypeFactory();
        final AlgDataType intType = typeFactory.createPolyType( PolyType.INTEGER );

        RexNode pattern = builder.patternConcat(
                builder.literal( "STRT" ),
                builder.patternQuantify(
                        builder.literal( "DOWN" ),
                        builder.literal( 1 ),
                        builder.literal( -1 ),
                        builder.literal( false ) ),
                builder.patternQuantify(
                        builder.literal( "UP" ),
                        builder.literal( 1 ),
                        builder.literal( -1 ),
                        builder.literal( false ) ) );

        ImmutableMap.Builder<String, RexNode> pdBuilder = new ImmutableMap.Builder<>();
        RexNode downDefinition = builder.call(
                OperatorRegistry.get( OperatorName.LESS_THAN ),
                builder.call(
                        OperatorRegistry.get( OperatorName.PREV ),
                        builder.patternField( "DOWN", intType, 3 ),
                        builder.literal( 0 ) ),
                builder.call(
                        OperatorRegistry.get( OperatorName.PREV ),
                        builder.patternField( "DOWN", intType, 3 ),
                        builder.literal( 1 ) ) );
        pdBuilder.put( "DOWN", downDefinition );
        RexNode upDefinition = builder.call(
                OperatorRegistry.get( OperatorName.GREATER_THAN ),
                builder.call(
                        OperatorRegistry.get( OperatorName.PREV ),
                        builder.patternField( "UP", intType, 3 ),
                        builder.literal( 0 ) ),
                builder.call(
                        OperatorRegistry.get( OperatorName.PREV ),
                        builder.patternField( "UP", intType, 3 ),
                        builder.literal( 1 ) ) );
        pdBuilder.put( "UP", upDefinition );

        ImmutableList.Builder<RexNode> measuresBuilder = new ImmutableList.Builder<>();
        measuresBuilder.add(
                builder.alias( builder.patternField( "STRT", intType, 3 ), "start_nw" ) );
        measuresBuilder.add(
                builder.alias(
                        builder.call(
                                OperatorRegistry.get( OperatorName.LAST ),
                                builder.patternField( "DOWN", intType, 3 ),
                                builder.literal( 0 ) ),
                        "bottom_nw" ) );

        RexNode after = builder.getRexBuilder().makeFlag(
                SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW );

        ImmutableList.Builder<RexNode> partitionKeysBuilder = new ImmutableList.Builder<>();
        partitionKeysBuilder.add( builder.field( "deptno" ) );

        ImmutableList.Builder<RexNode> orderKeysBuilder = new ImmutableList.Builder<>();
        orderKeysBuilder.add( builder.field( "empid" ) );

        RexNode interval = builder.literal( "INTERVAL '5' SECOND" );

        final ImmutableMap<String, TreeSet<String>> subsets = ImmutableMap.of();
        final AlgNode root = builder
                .match( pattern, false, false, pdBuilder.build(), measuresBuilder.build(), after, subsets, false, partitionKeysBuilder.build(), orderKeysBuilder.build(), interval )
                .build();
        final String expected = "LogicalMatch(model=[RELATIONAL], partition=[[$7]], order=[[0]], "
                + "outputFields=[[$7, 'start_nw', 'bottom_nw']], allRows=[false], "
                + "after=[FLAG(SKIP TO NEXT ROW)], pattern=[(('STRT', "
                + "PATTERN_QUANTIFIER('DOWN', 1, -1, false)), "
                + "PATTERN_QUANTIFIER('UP', 1, -1, false))], "
                + "isStrictStarts=[false], isStrictEnds=[false], "
                + "interval=['INTERVAL ''5'' SECOND'], subsets=[[]], "
                + "patternDefinitions=[[<(PREV(DOWN.$3, 0), PREV(DOWN.$3, 1)), "
                + ">(PREV(UP.$3, 0), PREV(UP.$3, 1))]], "
                + "inputFields=[[empid, ename, job, mgr, hiredate, salary, commission, deptno]])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testFilterCastAny() {
        final AlgBuilder builder = createAlgBuilder();
        final AlgDataType anyType = builder.getTypeFactory().createPolyType( PolyType.ANY );
        final AlgNode root =
                builder.scan( "employee" )
                        .filter(
                                builder.cast(
                                        builder.getRexBuilder().makeInputRef( anyType, 0 ),
                                        PolyType.BOOLEAN ) )
                        .build();
        final String expected = ""
                + "LogicalFilter(model=[RELATIONAL], condition=[CAST($0):BOOLEAN NOT NULL])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testFilterCastNull() {
        final AlgBuilder builder = createAlgBuilder();
        final AlgDataTypeFactory typeFactory = builder.getTypeFactory();
        final AlgNode root =
                builder.scan( "employee" )
                        .filter(
                                builder.getRexBuilder().makeCast(
                                        typeFactory.createTypeWithNullability(
                                                typeFactory.createPolyType( PolyType.BOOLEAN ),
                                                true ),
                                        builder.equals(
                                                builder.field( "deptno" ),
                                                builder.literal( 10 ) ) ) )
                        .build();
        final String expected = ""
                + "LogicalFilter(model=[RELATIONAL], condition=[=($7, 10)])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testRelBuilderToString() {
        final AlgBuilder builder = createAlgBuilder();
        builder.scan( "employee" );

        // One entry on the stack, a single-node tree
        final String expected1 = "LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( Util.toLinux( builder.toString() ), is( expected1 ) );

        // One entry on the stack, a two-node tree
        builder.filter( builder.equals( builder.field( 2 ), builder.literal( 3 ) ) );
        final String expected2 = "LogicalFilter(model=[RELATIONAL], condition=[=($2, 3)])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( Util.toLinux( builder.toString() ), is( expected2 ) );

        // Two entries on the stack
        builder.scan( "department" );
        final String expected3 = "LogicalScan(model=[RELATIONAL], table=[[public, department]])\n"
                + "LogicalFilter(model=[RELATIONAL], condition=[=($2, 3)])\n"
                + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( Util.toLinux( builder.toString() ), is( expected3 ) );
    }


    private void checkExpandTable( AlgBuilder builder, Matcher<AlgNode> matcher ) {
        final AlgNode root =
                builder.scan( "JDBC_public", "employee" )
                        .filter( builder.call( OperatorRegistry.get( OperatorName.GREATER_THAN ), builder.field( 2 ), builder.literal( 10 ) ) )
                        .build();
        assertThat( root, matcher );
    }


    @Test
    public void testExchange() {
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root = builder.scan( "employee" )
                .exchange( AlgDistributions.hash( Lists.newArrayList( 0 ) ) )
                .build();
        final String expected =
                "LogicalExchange(model=[RELATIONAL], distribution=[hash[0]])\n"
                        + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }


    @Test
    public void testSortExchange() {
        final AlgBuilder builder = createAlgBuilder();
        final AlgNode root =
                builder.scan( "public", "employee" )
                        .sortExchange(
                                AlgDistributions.hash( Lists.newArrayList( 0 ) ),
                                AlgCollations.of( 0 ) )
                        .build();
        final String expected =
                "LogicalSortExchange(model=[RELATIONAL], distribution=[hash[0]], collation=[[0]])\n"
                        + "  LogicalScan(model=[RELATIONAL], table=[[public, employee]])\n";
        assertThat( root, Matchers.hasTree( expected ) );
    }

}
