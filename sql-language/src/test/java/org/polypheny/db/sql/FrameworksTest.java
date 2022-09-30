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

package org.polypheny.db.sql;


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
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableScan;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.java.ReflectiveSchema;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgDataTypeSystemImpl;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgOptAbstractTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.AbstractConverter;
import org.polypheny.db.prepare.ContextImpl;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.prepare.Prepare;
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
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.dialect.AnsiSqlDialect;
import org.polypheny.db.tools.AlgConversionException;
import org.polypheny.db.tools.FrameworkConfig;
import org.polypheny.db.tools.Frameworks;
import org.polypheny.db.tools.Planner;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.tools.RuleSets;
import org.polypheny.db.tools.ValidationException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;


/**
 * Unit tests for methods in {@link Frameworks}.
 */
public class FrameworksTest extends SqlLanguagelDependant {

    @Test
    public void testOptimize() {
        AlgNode x =
                Frameworks.withPlanner( ( cluster, algOptSchema, rootSchema ) -> {
                    final AlgDataTypeFactory typeFactory = cluster.getTypeFactory();
                    final Table table = new AbstractTable() {
                        @Override
                        public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
                            final AlgDataType stringType = typeFactory.createJavaType( String.class );
                            final AlgDataType integerType = typeFactory.createJavaType( Integer.class );
                            return typeFactory.builder()
                                    .add( "s", null, stringType )
                                    .add( "i", null, integerType )
                                    .build();
                        }
                    };

                    // "SELECT * FROM myTable"
                    final AlgOptAbstractTable algOptTable = new AlgOptAbstractTable( algOptSchema, "myTable", table.getRowType( typeFactory ) ) {
                    };
                    final EnumerableScan tableRel = EnumerableScan.create( cluster, algOptTable );

                    // "WHERE i > 1"
                    final RexBuilder rexBuilder = cluster.getRexBuilder();
                    final RexNode condition =
                            rexBuilder.makeCall(
                                    OperatorRegistry.get( OperatorName.GREATER_THAN ),
                                    rexBuilder.makeFieldAccess( rexBuilder.makeRangeReference( tableRel ), "i", true ),
                                    rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
                    final LogicalFilter filter = LogicalFilter.create( tableRel, condition );

                    // Specify that the result should be in Enumerable convention.
                    final AlgNode rootRel = filter;
                    final AlgOptPlanner planner = cluster.getPlanner();
                    AlgTraitSet desiredTraits = cluster.traitSet().replace( EnumerableConvention.INSTANCE );
                    final AlgNode rootRel2 = planner.changeTraits( rootRel, desiredTraits );
                    planner.setRoot( rootRel2 );

                    // Now, plan.
                    return planner.findBestExp();
                } );
        String s = AlgOptUtil.dumpPlan( "", x, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES );
        assertThat(
                Util.toLinux( s ),
                equalTo( "EnumerableFilter(model=[RELATIONAL], condition=[>($1, 1)])\n  EnumerableScan(model=[RELATIONAL], table=[[myTable]])\n" ) );
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
                    public Void apply( AlgOptCluster cluster, AlgOptSchema algOptSchema, SchemaPlus rootSchema ) {
                        final AlgDataType type = cluster.getTypeFactory().createPolyType( PolyType.DECIMAL, 30, 2 );
                        final RexLiteral literal = cluster.getRexBuilder().makeExactLiteral( BigDecimal.ONE, type );
                        final RexNode call = cluster.getRexBuilder().makeCall( OperatorRegistry.get( OperatorName.PLUS ), literal, literal );
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
                .add( "hr", new ReflectiveSchema( new HrSchema() ), NamespaceType.RELATIONAL );

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
        Node parse = planner.parse( "select * from \"emps\" " );
        Node val = planner.validate( parse );

        String valStr = ((SqlNode) val).toSqlString( AnsiSqlDialect.DEFAULT, false ).getSql();

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
                .add( "hr", new ReflectiveSchema( new HrSchema() ), NamespaceType.RELATIONAL );

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
    @Ignore // test is no longer needed? as the streamer prevents this error and uses different end implementation
    public void testUpdate() throws Exception {
        Table table = new TableImpl();
        final SchemaPlus rootSchema = Frameworks.createRootSchema( true );
        SchemaPlus schema = rootSchema.add( "x", new AbstractSchema(), NamespaceType.RELATIONAL );
        schema.add( "MYTABLE", table );
        List<AlgTraitDef> traitDefs = new ArrayList<>();
        traitDefs.add( ConventionTraitDef.INSTANCE );
        traitDefs.add( AlgDistributionTraitDef.INSTANCE );
        ParserConfig parserConfig =
                Parser.configBuilder( ParserConfig.DEFAULT )
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


    private void executeQuery( FrameworkConfig config, @SuppressWarnings("SameParameterValue") String query, boolean debug ) throws AlgConversionException, NodeParseException, ValidationException {
        Planner planner = Frameworks.getPlanner( config );
        if ( debug ) {
            System.out.println( "Query:" + query );
        }
        Node n = planner.parse( query );
        n = planner.validate( n );
        AlgNode root = planner.alg( n ).project();
        if ( debug ) {
            System.out.println( AlgOptUtil.dumpPlan( "-- Logical Plan", root, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        AlgOptCluster cluster = root.getCluster();
        final AlgOptPlanner optPlanner = cluster.getPlanner();

        AlgTraitSet desiredTraits = cluster.traitSet().replace( EnumerableConvention.INSTANCE );
        final AlgNode newRoot = optPlanner.changeTraits( root, desiredTraits );
        if ( debug ) {
            System.out.println( AlgOptUtil.dumpPlan( "-- Mid Plan", newRoot, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        optPlanner.setRoot( newRoot );
        AlgNode bestExp = optPlanner.findBestExp();
        if ( debug ) {
            System.out.println( AlgOptUtil.dumpPlan( "-- Best Plan", bestExp, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
        }
    }


    /**
     * Modifiable, filterable table.
     */
    private static class TableImpl extends AbstractTable implements ModifiableTable, ProjectableFilterableTable {

        TableImpl() {
        }


        @Override
        public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
            return typeFactory.builder()
                    .add( "id", null, typeFactory.createPolyType( PolyType.INTEGER ) )
                    .add( "name", null, typeFactory.createPolyType( PolyType.INTEGER ) )
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
        public Modify toModificationAlg( AlgOptCluster cluster, AlgOptTable table, Prepare.CatalogReader catalogReader, AlgNode child, Modify.Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
            return LogicalModify.create( table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
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
    public static class HiveLikeTypeSystem extends AlgDataTypeSystemImpl {

        public static final AlgDataTypeSystem INSTANCE = new HiveLikeTypeSystem();


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
    public static class HiveLikeTypeSystem2 extends AlgDataTypeSystemImpl {

        public HiveLikeTypeSystem2() {
        }


        @Override
        public int getMaxNumericPrecision() {
            assert super.getMaxNumericPrecision() == 19;
            return 38;
        }

    }

}

