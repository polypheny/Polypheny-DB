/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.tools;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.junit.Test;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableTableScan;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.java.ReflectiveSchema;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.jdbc.ContextImpl;
import org.polypheny.db.jdbc.JavaTypeFactoryImpl;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.RelOptAbstractTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptSchema;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.RelTraitDef;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.plan.volcano.AbstractConverter;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.rel.RelDistributionTraitDef;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rel.type.RelDataTypeSystemImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.HrSchema;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.Path;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.ProjectableFilterableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.Statistics;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.dialect.AnsiSqlDialect;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.parser.SqlParseException;
import org.polypheny.db.sql.parser.SqlParser;
import org.polypheny.db.sql.parser.SqlParser.SqlParserConfig;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;


/**
 * Unit tests for methods in {@link Frameworks}.
 */
public class FrameworksTest {

    @Test
    public void testOptimize() {
        RelNode x =
                Frameworks.withPlanner( ( cluster, relOptSchema, rootSchema ) -> {
                    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
                    final Table table = new AbstractTable() {
                        @Override
                        public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
                            final RelDataType stringType = typeFactory.createJavaType( String.class );
                            final RelDataType integerType = typeFactory.createJavaType( Integer.class );
                            return typeFactory.builder()
                                    .add( "s", null, stringType )
                                    .add( "i", null, integerType )
                                    .build();
                        }
                    };

                    // "SELECT * FROM myTable"
                    final RelOptAbstractTable relOptTable = new RelOptAbstractTable( relOptSchema, "myTable", table.getRowType( typeFactory ) ) {
                    };
                    final EnumerableTableScan tableRel = EnumerableTableScan.create( cluster, relOptTable );

                    // "WHERE i > 1"
                    final RexBuilder rexBuilder = cluster.getRexBuilder();
                    final RexNode condition =
                            rexBuilder.makeCall(
                                    SqlStdOperatorTable.GREATER_THAN,
                                    rexBuilder.makeFieldAccess( rexBuilder.makeRangeReference( tableRel ), "i", true ),
                                    rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
                    final LogicalFilter filter = LogicalFilter.create( tableRel, condition );

                    // Specify that the result should be in Enumerable convention.
                    final RelNode rootRel = filter;
                    final RelOptPlanner planner = cluster.getPlanner();
                    RelTraitSet desiredTraits = cluster.traitSet().replace( EnumerableConvention.INSTANCE );
                    final RelNode rootRel2 = planner.changeTraits( rootRel, desiredTraits );
                    planner.setRoot( rootRel2 );

                    // Now, plan.
                    return planner.findBestExp();
                } );
        String s = RelOptUtil.dumpPlan( "", x, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES );
        assertThat(
                Util.toLinux( s ),
                equalTo( "EnumerableFilter(condition=[>($1, 1)])\n  EnumerableTableScan(table=[[myTable]])\n" ) );
    }


    /**
     * Unit test to test create root schema which has no "metadata" schema.
     */
    @Test
    public void testCreateRootSchemaWithNoMetadataSchema() {
        SchemaPlus rootSchema = Frameworks.createRootSchema( false );
        assertThat( rootSchema.getSubSchemaNames().size(), equalTo( 0 ) );
    }


    /**
     * Tests that validation (specifically, inferring the result of adding two DECIMAL(19, 0) values together) happens differently with a type system that allows a larger maximum precision for decimals.
     *
     * Test case for "Add RelDataTypeSystem plugin, allowing different max precision of a DECIMAL".
     *
     * Also tests the plugin system, by specifying implementations of a plugin interface with public and private constructors.
     */
    @Test
    public void testTypeSystem() {
        checkTypeSystem( 19, Frameworks.newConfigBuilder()
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( Frameworks.createRootSchema( false ) ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl();
                            }
                        },
                        "",
                        0,
                        0,
                        null ) )
                .build() );
        checkTypeSystem( 25, Frameworks.newConfigBuilder().typeSystem( HiveLikeTypeSystem.INSTANCE )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( Frameworks.createRootSchema( false ) ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl( HiveLikeTypeSystem.INSTANCE );
                            }
                        },
                        "",
                        0,
                        0,
                        null ) )
                .build() );
        checkTypeSystem( 31, Frameworks.newConfigBuilder().typeSystem( new HiveLikeTypeSystem2() )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( Frameworks.createRootSchema( false ) ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl( new HiveLikeTypeSystem2() );
                            }
                        },
                        "",
                        0,
                        0,
                        null ) )
                .build() );
    }


    private void checkTypeSystem( final int expected, FrameworkConfig config ) {
        Frameworks.withPrepare(
                new Frameworks.PrepareAction<Void>( config ) {
                    @Override
                    public Void apply( RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus rootSchema ) {
                        final RelDataType type = cluster.getTypeFactory().createSqlType( PolyType.DECIMAL, 30, 2 );
                        final RexLiteral literal = cluster.getRexBuilder().makeExactLiteral( BigDecimal.ONE, type );
                        final RexNode call = cluster.getRexBuilder().makeCall( SqlStdOperatorTable.PLUS, literal, literal );
                        assertEquals( expected, call.getType().getPrecision() );
                        return null;
                    }
                } );
    }


    /**
     * Tests that the validator expands identifiers by default.
     *
     * Test case for "Validator in Frameworks should expand identifiers".
     */
    @Test
    public void testFrameworksValidatorWithIdentifierExpansion() throws Exception {
        final SchemaPlus schema = Frameworks
                .createRootSchema( true )
                .add( "hr", new ReflectiveSchema( new HrSchema() ) );

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema( schema )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( schema ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl();
                            }
                        },
                        "",
                        0,
                        0,
                        null ) )
                .build();
        final Planner planner = Frameworks.getPlanner( config );
        SqlNode parse = planner.parse( "select * from \"emps\" " );
        SqlNode val = planner.validate( parse );

        String valStr = val.toSqlString( AnsiSqlDialect.DEFAULT, false ).getSql();

        String expandedStr = "SELECT `emps`.`empid`, `emps`.`deptno`, `emps`.`name`, `emps`.`salary`, `emps`.`commission`\n" + "FROM `hr`.`emps` AS `emps`";
        assertThat( Util.toLinux( valStr ), equalTo( expandedStr ) );
    }


    /**
     * Test for {@link Path}.
     */
    @Test
    public void testSchemaPath() {
        final SchemaPlus schema = Frameworks
                .createRootSchema( true )
                .add( "hr", new ReflectiveSchema( new HrSchema() ) );

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema( schema )
                .build();
        final Path path = Schemas.path( config.getDefaultSchema() );
        assertThat( path.size(), is( 2 ) );
        assertThat( path.get( 0 ).left, is( "" ) );
        assertThat( path.get( 1 ).left, is( "hr" ) );
        assertThat( path.names().size(), is( 1 ) );
        assertThat( path.names().get( 0 ), is( "hr" ) );
        assertThat( path.schemas().size(), is( 2 ) );

        final Path parent = path.parent();
        assertThat( parent.size(), is( 1 ) );
        assertThat( parent.names().size(), is( 0 ) );

        final Path grandparent = parent.parent();
        assertThat( grandparent.size(), is( 0 ) );

        try {
            Object o = grandparent.parent();
            fail( "expected exception, got " + o );
        } catch ( IllegalArgumentException e ) {
            // ok
        }
    }


    /**
     * Test case for "AssertionError when pushing project to ProjectableFilterableTable" using UPDATE via {@link Frameworks}.
     */
    @Test
    public void testUpdate() throws Exception {
        Table table = new TableImpl();
        final SchemaPlus rootSchema = Frameworks.createRootSchema( true );
        SchemaPlus schema = rootSchema.add( "x", new AbstractSchema() );
        schema.add( "MYTABLE", table );
        List<RelTraitDef> traitDefs = new ArrayList<>();
        traitDefs.add( ConventionTraitDef.INSTANCE );
        traitDefs.add( RelDistributionTraitDef.INSTANCE );
        SqlParserConfig parserConfig =
                SqlParser.configBuilder( SqlParserConfig.DEFAULT )
                        .setCaseSensitive( false )
                        .build();

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( parserConfig )
                .defaultSchema( schema )
                .traitDefs( traitDefs )
                // define the rules you want to apply
                .ruleSets( RuleSets.ofList( AbstractConverter.ExpandConversionRule.INSTANCE ) )
                .programs( Programs.ofRules( Programs.RULE_SET ) )
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
                        null ) )
                .build();
        executeQuery( config, " UPDATE MYTABLE set id=7 where id=1", RuntimeConfig.DEBUG.getBoolean() );
    }


    private void executeQuery( FrameworkConfig config, @SuppressWarnings("SameParameterValue") String query, boolean debug ) throws RelConversionException, SqlParseException, ValidationException {
        Planner planner = Frameworks.getPlanner( config );
        if ( debug ) {
            System.out.println( "Query:" + query );
        }
        SqlNode n = planner.parse( query );
        n = planner.validate( n );
        RelNode root = planner.rel( n ).project();
        if ( debug ) {
            System.out.println( RelOptUtil.dumpPlan( "-- Logical Plan", root, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        RelOptCluster cluster = root.getCluster();
        final RelOptPlanner optPlanner = cluster.getPlanner();

        RelTraitSet desiredTraits = cluster.traitSet().replace( EnumerableConvention.INSTANCE );
        final RelNode newRoot = optPlanner.changeTraits( root, desiredTraits );
        if ( debug ) {
            System.out.println( RelOptUtil.dumpPlan( "-- Mid Plan", newRoot, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        optPlanner.setRoot( newRoot );
        RelNode bestExp = optPlanner.findBestExp();
        if ( debug ) {
            System.out.println( RelOptUtil.dumpPlan( "-- Best Plan", bestExp, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
    }


    /**
     * Modifiable, filterable table.
     */
    private static class TableImpl extends AbstractTable implements ModifiableTable, ProjectableFilterableTable {

        TableImpl() {
        }


        @Override
        public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
            return typeFactory.builder()
                    .add( "id", null, typeFactory.createSqlType( PolyType.INTEGER ) )
                    .add( "name", null, typeFactory.createSqlType( PolyType.INTEGER ) )
                    .build();
        }


        @Override
        public Statistic getStatistic() {
            return Statistics.of( 15D, ImmutableList.of( ImmutableBitSet.of( 0 ) ), ImmutableList.of() );
        }


        @Override
        public Enumerable<Object[]> scan( DataContext root, List<RexNode> filters, int[] projects ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public Collection getModifiableCollection() {
            throw new UnsupportedOperationException();
        }


        @Override
        public TableModify toModificationRel( RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
            return LogicalTableModify.create( table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
        }


        @Override
        public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public Type getElementType() {
            return Object.class;
        }


        @Override
        public Expression getExpression( SchemaPlus schema, String tableName, Class clazz ) {
            throw new UnsupportedOperationException();
        }
    }


    /**
     * Dummy type system, similar to Hive's, accessed via an INSTANCE member.
     */
    public static class HiveLikeTypeSystem extends RelDataTypeSystemImpl {

        public static final RelDataTypeSystem INSTANCE = new HiveLikeTypeSystem();


        private HiveLikeTypeSystem() {
        }


        @Override
        public int getMaxNumericPrecision() {
            assert super.getMaxNumericPrecision() == 19;
            return 25;
        }
    }


    /**
     * Dummy type system, similar to Hive's, accessed via a public default constructor.
     */
    public static class HiveLikeTypeSystem2 extends RelDataTypeSystemImpl {

        public HiveLikeTypeSystem2() {
        }


        @Override
        public int getMaxNumericPrecision() {
            assert super.getMaxNumericPrecision() == 19;
            return 38;
        }
    }
}

