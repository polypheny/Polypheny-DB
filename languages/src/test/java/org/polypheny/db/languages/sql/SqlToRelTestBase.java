/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages.sql;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.catalog.MockCatalogReader;
import org.polypheny.db.catalog.MockCatalogReaderDynamic;
import org.polypheny.db.catalog.MockCatalogReaderSimple;
import org.polypheny.db.config.PolyphenyDbConnectionConfig;
import org.polypheny.db.core.Conformance;
import org.polypheny.db.core.ConformanceEnum;
import org.polypheny.db.core.Monotonicity;
import org.polypheny.db.core.OperatorTable;
import org.polypheny.db.core.RelFieldTrimmer;
import org.polypheny.db.core.ValidatorCatalogReader;
import org.polypheny.db.core.ValidatorTable;
import org.polypheny.db.languages.MockSqlOperatorTable;
import org.polypheny.db.languages.NodeToRelConverter;
import org.polypheny.db.languages.NodeToRelConverter.Config;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.core.DiffRepository;
import org.polypheny.db.languages.core.MockConfigBuilder;
import org.polypheny.db.languages.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.languages.sql.validate.SqlValidatorImpl;
import org.polypheny.db.languages.sql2rel.SqlToRelConverter;
import org.polypheny.db.languages.sql2rel.StandardConvertletTable;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptSchema;
import org.polypheny.db.plan.RelOptSchemaWithSampling;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelDistribution;
import org.polypheny.db.rel.RelDistributions;
import org.polypheny.db.rel.RelFieldCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelReferentialConstraint;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.test.MockRelOptPlanner;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * SqlToRelTestBase is an abstract base for tests which involve conversion from SQL to relational algebra.
 *
 * SQL statements to be translated can use the schema defined in {@link MockCatalogReader}; note that this is slightly different from Farrago's SALES schema. If you get a parser or validator
 * error from your test SQL, look down in the stack until you see "Caused by", which will usually tell you the real error.
 */
public abstract class SqlToRelTestBase {

    protected static final String NL = System.getProperty( "line.separator" );

    protected final Tester tester = createTester();


    public SqlToRelTestBase() {
        super();
    }


    public Tester createTester() {
        return new TesterImpl( getDiffRepos(), false, false, true, false, null, null, Config.DEFAULT, ConformanceEnum.DEFAULT, Contexts.empty() );
    }


    protected Tester createTester( Conformance conformance ) {
        return new TesterImpl( getDiffRepos(), false, false, true, false, null, null, Config.DEFAULT, conformance, Contexts.empty() );
    }


    protected Tester getTesterWithDynamicTable() {
        return tester.withCatalogReaderFactory( MockCatalogReaderDynamic::new );
    }


    /**
     * Returns the default diff repository for this test, or null if there is no repository.
     *
     * The default implementation returns null.
     *
     * Sub-classes that want to use a diff repository can override. Sub-sub-classes can override again, inheriting test cases and overriding selected test results.
     *
     * And individual test cases can override by providing a different tester object.
     *
     * @return Diff repository
     */
    protected DiffRepository getDiffRepos() {
        return null;
    }


    /**
     * Checks that every node of a relational expression is valid.
     *
     * @param rel Relational expression
     */
    public static void assertValid( RelNode rel ) {
        SqlToRelConverterTest.RelValidityChecker checker = new SqlToRelConverterTest.RelValidityChecker();
        checker.go( rel );
        assertEquals( 0, checker.invalidCount );
    }

    //~ Inner Interfaces -------------------------------------------------------


    /**
     * Helper class which contains default implementations of methods used for running sql-to-rel conversion tests.
     */
    public interface Tester {

        /**
         * Converts a SQL string to a {@link RelNode} tree.
         *
         * @param sql SQL statement
         * @return Relational expression, never null
         */
        RelRoot convertSqlToRel( String sql );

        SqlNode parseQuery( String sql ) throws Exception;

        /**
         * Factory method to create a {@link SqlValidator}.
         */
        SqlValidator createValidator( ValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory );

        /**
         * Factory method for a
         * {@link Prepare.CatalogReader}.
         */
        Prepare.CatalogReader createCatalogReader( RelDataTypeFactory typeFactory );

        RelOptPlanner createPlanner();

        /**
         * Returns the {@link OperatorTable} to use.
         */
        OperatorTable getOperatorTable();

        /**
         * Returns the SQL dialect to test.
         */
        Conformance getConformance();

        /**
         * Checks that a SQL statement converts to a given plan.
         *
         * @param sql SQL query
         * @param plan Expected plan
         */
        void assertConvertsTo( String sql, String plan );

        /**
         * Checks that a SQL statement converts to a given plan, optionally trimming columns that are not needed.
         *
         * @param sql SQL query
         * @param plan Expected plan
         * @param trim Whether to trim columns that are not needed
         */
        void assertConvertsTo( String sql, String plan, boolean trim );

        /**
         * Returns the diff repository.
         *
         * @return Diff repository
         */
        DiffRepository getDiffRepos();

        /**
         * Returns the validator.
         *
         * @return Validator
         */
        SqlValidator getValidator();

        /**
         * Returns a tester that optionally decorrelates queries.
         */
        Tester withDecorrelation( boolean enable );

        /**
         * Returns a tester that optionally decorrelates queries after planner rules have fired.
         */
        Tester withLateDecorrelation( boolean enable );

        /**
         * Returns a tester that optionally expands sub-queries. If {@code expand} is false, the plan contains a {@link org.polypheny.db.rex.RexSubQuery} for each sub-query.
         *
         * @see Prepare#THREAD_EXPAND
         */
        Tester withExpand( boolean expand );

        /**
         * Returns a tester that optionally uses a {@code SqlToRelConverter.Config}.
         */
        Tester withConfig( Config config );

        /**
         * Returns a tester with a {@link Conformance}.
         */
        Tester withConformance( Conformance conformance );

        Tester withCatalogReaderFactory( SqlTestFactory.MockCatalogReaderFactory factory );

        /**
         * Returns a tester that optionally trims unused fields.
         */
        Tester withTrim( boolean enable );

        Tester withClusterFactory( Function<RelOptCluster, RelOptCluster> function );

        boolean isLateDecorrelate();

        /**
         * Returns a tester that uses a given context.
         */
        Tester withContext( Context context );

    }


    /**
     * Mock implementation of {@link RelOptSchema}.
     */
    protected static class MockRelOptSchema implements RelOptSchemaWithSampling {

        private final ValidatorCatalogReader catalogReader;
        private final RelDataTypeFactory typeFactory;


        public MockRelOptSchema( ValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory ) {
            this.catalogReader = catalogReader;
            this.typeFactory = typeFactory;
        }


        @Override
        public RelOptTable getTableForMember( List<String> names ) {
            final ValidatorTable table = catalogReader.getTable( names );
            final RelDataType rowType = table.getRowType();
            final List<RelCollation> collationList = deduceMonotonicity( table );
            if ( names.size() < 3 ) {
                String[] newNames2 = { "CATALOG", "SALES", "" };
                List<String> newNames = new ArrayList<>();
                int i = 0;
                while ( newNames.size() < newNames2.length ) {
                    newNames.add( i, newNames2[i] );
                    ++i;
                }
                names = newNames;
            }
            return createColumnSet( table, names, rowType, collationList );
        }


        private List<RelCollation> deduceMonotonicity( ValidatorTable table ) {
            final RelDataType rowType = table.getRowType();
            final List<RelCollation> collationList = new ArrayList<>();

            // Deduce which fields the table is sorted on.
            int i = -1;
            for ( RelDataTypeField field : rowType.getFieldList() ) {
                ++i;
                final Monotonicity monotonicity = table.getMonotonicity( field.getName() );
                if ( monotonicity != Monotonicity.NOT_MONOTONIC ) {
                    final RelFieldCollation.Direction direction =
                            monotonicity.isDecreasing()
                                    ? RelFieldCollation.Direction.DESCENDING
                                    : RelFieldCollation.Direction.ASCENDING;
                    collationList.add( RelCollations.of( new RelFieldCollation( i, direction ) ) );
                }
            }
            return collationList;
        }


        @Override
        public RelOptTable getTableForMember( List<String> names, final String datasetName, boolean[] usedDataset ) {
            final RelOptTable table = getTableForMember( names );

            // If they're asking for a sample, just for test purposes, assume there's a table called "<table>:<sample>".
            RelOptTable datasetTable =
                    new DelegatingRelOptTable( table ) {
                        @Override
                        public List<String> getQualifiedName() {
                            final List<String> list = new ArrayList<>( super.getQualifiedName() );
                            list.set(
                                    list.size() - 1,
                                    list.get( list.size() - 1 ) + ":" + datasetName );
                            return ImmutableList.copyOf( list );
                        }
                    };
            if ( usedDataset != null ) {
                assert usedDataset.length == 1;
                usedDataset[0] = true;
            }
            return datasetTable;
        }


        protected MockColumnSet createColumnSet( ValidatorTable table, List<String> names, final RelDataType rowType, final List<RelCollation> collationList ) {
            return new MockColumnSet( names, rowType, collationList );
        }


        @Override
        public RelDataTypeFactory getTypeFactory() {
            return typeFactory;
        }


        @Override
        public void registerRules( RelOptPlanner planner ) throws Exception {
        }


        /**
         * Mock column set.
         */
        protected class MockColumnSet implements RelOptTable {

            private final List<String> names;
            private final RelDataType rowType;
            private final List<RelCollation> collationList;


            protected MockColumnSet( List<String> names, RelDataType rowType, final List<RelCollation> collationList ) {
                this.names = ImmutableList.copyOf( names );
                this.rowType = rowType;
                this.collationList = collationList;
            }


            @Override
            public <T> T unwrap( Class<T> clazz ) {
                if ( clazz.isInstance( this ) ) {
                    return clazz.cast( this );
                }
                return null;
            }


            @Override
            public List<String> getQualifiedName() {
                return names;
            }


            @Override
            public double getRowCount() {
                // use something other than 0 to give costing tests some room, and make emps bigger than depts for join asymmetry
                if ( Iterables.getLast( names ).equals( "EMP" ) ) {
                    return 1000;
                } else {
                    return 100;
                }
            }


            @Override
            public RelDataType getRowType() {
                return rowType;
            }


            @Override
            public RelOptSchema getRelOptSchema() {
                return MockRelOptSchema.this;
            }


            @Override
            public RelNode toRel( ToRelContext context ) {
                return LogicalTableScan.create( context.getCluster(), this );
            }


            @Override
            public List<RelCollation> getCollationList() {
                return collationList;
            }


            @Override
            public RelDistribution getDistribution() {
                return RelDistributions.BROADCAST_DISTRIBUTED;
            }


            @Override
            public boolean isKey( ImmutableBitSet columns ) {
                return false;
            }


            @Override
            public List<RelReferentialConstraint> getReferentialConstraints() {
                return ImmutableList.of();
            }


            @Override
            public List<ColumnStrategy> getColumnStrategies() {
                throw new UnsupportedOperationException();
            }


            @Override
            public Expression getExpression( Class clazz ) {
                return null;
            }


            @Override
            public RelOptTable extend( List<RelDataTypeField> extendedFields ) {
                final RelDataType extendedRowType = getRelOptSchema().getTypeFactory().builder()
                        .addAll( rowType.getFieldList() )
                        .addAll( extendedFields )
                        .build();
                return new MockColumnSet( names, extendedRowType, collationList );
            }

        }

    }


    /**
     * Table that delegates to a given table.
     */
    private static class DelegatingRelOptTable implements RelOptTable {

        private final RelOptTable parent;


        DelegatingRelOptTable( RelOptTable parent ) {
            this.parent = parent;
        }


        @Override
        public <T> T unwrap( Class<T> clazz ) {
            if ( clazz.isInstance( this ) ) {
                return clazz.cast( this );
            }
            return parent.unwrap( clazz );
        }


        @Override
        public Expression getExpression( Class clazz ) {
            return parent.getExpression( clazz );
        }


        @Override
        public RelOptTable extend( List<RelDataTypeField> extendedFields ) {
            return parent.extend( extendedFields );
        }


        @Override
        public List<String> getQualifiedName() {
            return parent.getQualifiedName();
        }


        @Override
        public double getRowCount() {
            return parent.getRowCount();
        }


        @Override
        public RelDataType getRowType() {
            return parent.getRowType();
        }


        @Override
        public RelOptSchema getRelOptSchema() {
            return parent.getRelOptSchema();
        }


        @Override
        public RelNode toRel( ToRelContext context ) {
            return LogicalTableScan.create( context.getCluster(), this );
        }


        @Override
        public List<RelCollation> getCollationList() {
            return parent.getCollationList();
        }


        @Override
        public RelDistribution getDistribution() {
            return parent.getDistribution();
        }


        @Override
        public boolean isKey( ImmutableBitSet columns ) {
            return parent.isKey( columns );
        }


        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return parent.getReferentialConstraints();
        }


        @Override
        public List<ColumnStrategy> getColumnStrategies() {
            return parent.getColumnStrategies();
        }

    }


    /**
     * Default implementation of {@link Tester}, using mock classes {@link MockRelOptSchema} and {@link MockRelOptPlanner}.
     */
    public static class TesterImpl implements Tester {

        private RelOptPlanner planner;
        private OperatorTable opTab;
        private final DiffRepository diffRepos;
        private final boolean enableDecorrelate;
        private final boolean enableLateDecorrelate;
        private final boolean enableTrim;
        private final boolean enableExpand;
        private final Conformance conformance;
        private final SqlTestFactory.MockCatalogReaderFactory catalogReaderFactory;
        private final Function<RelOptCluster, RelOptCluster> clusterFactory;
        private RelDataTypeFactory typeFactory;
        public final Config config;
        private final Context context;


        /**
         * Creates a TesterImpl.
         *
         * @param diffRepos Diff repository
         * @param enableDecorrelate Whether to decorrelate
         * @param enableTrim Whether to trim unused fields
         * @param enableExpand Whether to expand sub-queries
         * @param catalogReaderFactory Function to create catalog reader, or null
         * @param clusterFactory Called after a cluster has been created
         */
        protected TesterImpl(
                DiffRepository diffRepos,
                boolean enableDecorrelate,
                boolean enableTrim,
                boolean enableExpand,
                boolean enableLateDecorrelate,
                SqlTestFactory.MockCatalogReaderFactory catalogReaderFactory,
                Function<RelOptCluster,
                        RelOptCluster> clusterFactory ) {
            this(
                    diffRepos,
                    enableDecorrelate,
                    enableTrim,
                    enableExpand,
                    enableLateDecorrelate,
                    catalogReaderFactory,
                    clusterFactory,
                    Config.DEFAULT,
                    ConformanceEnum.DEFAULT,
                    Contexts.empty() );
        }


        protected TesterImpl(
                DiffRepository diffRepos,
                boolean enableDecorrelate,
                boolean enableTrim,
                boolean enableExpand,
                boolean enableLateDecorrelate,
                SqlTestFactory.MockCatalogReaderFactory catalogReaderFactory,
                Function<RelOptCluster, RelOptCluster> clusterFactory,
                Config config,
                Conformance conformance,
                Context context ) {
            this.diffRepos = diffRepos;
            this.enableDecorrelate = enableDecorrelate;
            this.enableTrim = enableTrim;
            this.enableExpand = enableExpand;
            this.enableLateDecorrelate = enableLateDecorrelate;
            this.catalogReaderFactory = catalogReaderFactory;
            this.clusterFactory = clusterFactory;
            this.config = config;
            this.conformance = conformance;
            this.context = context;
        }


        @Override
        public RelRoot convertSqlToRel( String sql ) {
            Objects.requireNonNull( sql );
            final SqlNode sqlQuery;
            final Config localConfig;
            try {
                sqlQuery = parseQuery( sql );
            } catch ( RuntimeException | Error e ) {
                throw e;
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }
            final RelDataTypeFactory typeFactory = getTypeFactory();
            final Prepare.CatalogReader catalogReader = createCatalogReader( typeFactory );
            final SqlValidator validator = createValidator( catalogReader, typeFactory );
            final PolyphenyDbConnectionConfig polyphenyDbConfig = context.unwrap( PolyphenyDbConnectionConfig.class );
            if ( polyphenyDbConfig != null ) {
                validator.setDefaultNullCollation( polyphenyDbConfig.defaultNullCollation() );
            }
            if ( config == Config.DEFAULT ) {
                localConfig = NodeToRelConverter.configBuilder().withTrimUnusedFields( true ).withExpand( enableExpand ).build();
            } else {
                localConfig = config;
            }

            final SqlToRelConverter converter = createSqlToRelConverter( validator, catalogReader, typeFactory, localConfig );

            final SqlNode validatedQuery = validator.validateSql( sqlQuery );
            RelRoot root = converter.convertQuery( validatedQuery, false, true );
            assert root != null;
            if ( enableDecorrelate || enableTrim ) {
                root = root.withRel( converter.flattenTypes( root.rel, true ) );
            }
            if ( enableDecorrelate ) {
                root = root.withRel( converter.decorrelate( sqlQuery, root.rel ) );
            }
            if ( enableTrim ) {
                root = root.withRel( converter.trimUnusedFields( true, root.rel ) );
            }
            return root;
        }


        protected SqlToRelConverter createSqlToRelConverter( final SqlValidator validator, final Prepare.CatalogReader catalogReader, final RelDataTypeFactory typeFactory, final Config config ) {
            final RexBuilder rexBuilder = new RexBuilder( typeFactory );
            RelOptCluster cluster = RelOptCluster.create( getPlanner(), rexBuilder );
            if ( clusterFactory != null ) {
                cluster = clusterFactory.apply( cluster );
            }
            return new SqlToRelConverter( null, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, config );
        }


        protected final RelDataTypeFactory getTypeFactory() {
            if ( typeFactory == null ) {
                typeFactory = createTypeFactory();
            }
            return typeFactory;
        }


        protected RelDataTypeFactory createTypeFactory() {
            return new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        }


        protected final RelOptPlanner getPlanner() {
            if ( planner == null ) {
                planner = createPlanner();
            }
            return planner;
        }


        @Override
        public SqlNode parseQuery( String sql ) throws Exception {
            final ParserConfig sqlParserConfig = MockConfigBuilder.mockParserConfig().setConformance( getConformance() ).build();
            Parser parser = MockConfigBuilder.createMockParser( sql, sqlParserConfig );
            return (SqlNode) parser.parseQuery();
        }


        @Override
        public Conformance getConformance() {
            return conformance;
        }


        @Override
        public SqlValidator createValidator( ValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory ) {
            return new FarragoTestValidator( getOperatorTable(), catalogReader, typeFactory, getConformance() );
        }


        @Override
        public final OperatorTable getOperatorTable() {
            if ( opTab == null ) {
                opTab = createOperatorTable();
            }
            return opTab;
        }


        /**
         * Creates an operator table.
         *
         * @return New operator table
         */
        protected OperatorTable createOperatorTable() {
            final MockSqlOperatorTable opTab = new MockSqlOperatorTable( SqlStdOperatorTable.instance() );
            MockSqlOperatorTable.addRamp( opTab );
            return opTab;
        }


        @Override
        public Prepare.CatalogReader createCatalogReader( RelDataTypeFactory typeFactory ) {
            MockCatalogReader catalogReader;
            if ( this.catalogReaderFactory != null ) {
                catalogReader = catalogReaderFactory.create( typeFactory, true );
            } else {
                catalogReader = new MockCatalogReaderSimple( typeFactory, true );
            }
            return catalogReader.init();
        }


        @Override
        public RelOptPlanner createPlanner() {
            return new MockRelOptPlanner( context );
        }


        @Override
        public void assertConvertsTo( String sql, String plan ) {
            assertConvertsTo( sql, plan, false );
        }


        @Override
        public void assertConvertsTo( String sql, String plan, boolean trim ) {
            String sql2 = getDiffRepos().expand( "sql", sql );
            RelNode rel = convertSqlToRel( sql2 ).project();

            assertNotNull( rel );
            assertValid( rel );

            if ( trim ) {
                final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create( rel.getCluster(), null );
                final RelFieldTrimmer trimmer = createFieldTrimmer( relBuilder );
                rel = trimmer.trim( rel );
                assertNotNull( rel );
                assertValid( rel );
            }

            // NOTE jvs 28-Mar-2006:  insert leading newline so that plans come out nicely stacked instead of first line immediately after CDATA start
            String actual = NL + RelOptUtil.toString( rel );
            diffRepos.assertEquals( "plan", plan, actual );
        }


        /**
         * Creates a RelFieldTrimmer.
         *
         * @param relBuilder Builder
         * @return Field trimmer
         */
        public RelFieldTrimmer createFieldTrimmer( RelBuilder relBuilder ) {
            return new RelFieldTrimmer( getValidator(), relBuilder );
        }


        @Override
        public DiffRepository getDiffRepos() {
            return diffRepos;
        }


        @Override
        public SqlValidator getValidator() {
            final RelDataTypeFactory typeFactory = getTypeFactory();
            final ValidatorCatalogReader catalogReader = createCatalogReader( typeFactory );
            return createValidator( catalogReader, typeFactory );
        }


        @Override
        public TesterImpl withDecorrelation( boolean enableDecorrelate ) {
            return this.enableDecorrelate == enableDecorrelate
                    ? this
                    : new TesterImpl( diffRepos, enableDecorrelate, enableTrim, enableExpand, enableLateDecorrelate, catalogReaderFactory, clusterFactory, config, conformance, context );
        }


        @Override
        public Tester withLateDecorrelation( boolean enableLateDecorrelate ) {
            return this.enableLateDecorrelate == enableLateDecorrelate
                    ? this
                    : new TesterImpl( diffRepos, enableDecorrelate, enableTrim, enableExpand, enableLateDecorrelate, catalogReaderFactory, clusterFactory, config, conformance, context );
        }


        @Override
        public TesterImpl withConfig( Config config ) {
            return this.config == config
                    ? this
                    : new TesterImpl( diffRepos, enableDecorrelate, enableTrim, enableExpand, enableLateDecorrelate, catalogReaderFactory, clusterFactory, config, conformance, context );
        }


        @Override
        public Tester withTrim( boolean enableTrim ) {
            return this.enableTrim == enableTrim
                    ? this
                    : new TesterImpl( diffRepos, enableDecorrelate, enableTrim, enableExpand, enableLateDecorrelate, catalogReaderFactory, clusterFactory, config, conformance, context );
        }


        @Override
        public Tester withExpand( boolean enableExpand ) {
            return this.enableExpand == enableExpand
                    ? this
                    : new TesterImpl( diffRepos, enableDecorrelate, enableTrim, enableExpand, enableLateDecorrelate, catalogReaderFactory, clusterFactory, config, conformance, context );
        }


        @Override
        public Tester withConformance( Conformance conformance ) {
            return new TesterImpl( diffRepos, enableDecorrelate, false, enableExpand, enableLateDecorrelate, catalogReaderFactory, clusterFactory, config, conformance, context );
        }


        @Override
        public Tester withCatalogReaderFactory( SqlTestFactory.MockCatalogReaderFactory factory ) {
            return new TesterImpl( diffRepos, enableDecorrelate, false, enableExpand, enableLateDecorrelate, factory, clusterFactory, config, conformance, context );
        }


        @Override
        public Tester withClusterFactory( Function<RelOptCluster, RelOptCluster> clusterFactory ) {
            return new TesterImpl( diffRepos, enableDecorrelate, false, enableExpand, enableLateDecorrelate, catalogReaderFactory, clusterFactory, config, conformance, context );
        }


        @Override
        public Tester withContext( Context context ) {
            return new TesterImpl( diffRepos, enableDecorrelate, false, enableExpand, enableLateDecorrelate, catalogReaderFactory, clusterFactory, config, conformance, context );
        }


        @Override
        public boolean isLateDecorrelate() {
            return enableLateDecorrelate;
        }

    }


    /**
     * Validator for testing.
     */
    private static class FarragoTestValidator extends SqlValidatorImpl {

        FarragoTestValidator( OperatorTable opTab, ValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, Conformance conformance ) {
            super( opTab, catalogReader, typeFactory, conformance );
        }


        // override SqlValidator
        @Override
        public boolean shouldExpandIdentifiers() {
            return true;
        }

    }

}

