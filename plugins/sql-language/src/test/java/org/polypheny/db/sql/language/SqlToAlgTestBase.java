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

package org.polypheny.db.sql.language;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.util.List;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgFieldTrimmer;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.NodeToAlgConverter.Config;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.DiffRepository;
import org.polypheny.db.sql.MockSqlOperatorTable;
import org.polypheny.db.sql.SqlLanguageDependent;
import org.polypheny.db.sql.language.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Pair;


/**
 * SqlToAlgTestBase is an abstract base for tests which involve conversion from SQL to relational algebra.
 * <p>
 * SQL statements to be translated can use the schema defined in note that this is slightly different from Farrago's SALES schema. If you get a parser or validator
 * error from your test SQL, look down in the stack until you see "Caused by", which will usually tell you the real error.
 */
public abstract class SqlToAlgTestBase extends SqlLanguageDependent {

    protected static final String NL = System.lineSeparator();

    protected final Tester tester = createTester();


    public SqlToAlgTestBase() {
        super();
    }


    public Tester createTester() {
        return new TesterImpl( getDiffRepos(), false, false, true, false, null, Config.DEFAULT, ConformanceEnum.DEFAULT, Contexts.empty() );
    }


    /**
     * Returns the default diff repository for this test, or null if there is no repository.
     * <p>
     * The default implementation returns null.
     * <p>
     * Sub-classes that want to use a diff repository can override. Sub-sub-classes can override again, inheriting test cases and overriding selected test results.
     * <p>
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
     * @param alg Relational expression
     */
    public static void assertValid( AlgNode alg ) {
        SqlToAlgConverterTest.RelValidityChecker checker = new SqlToAlgConverterTest.RelValidityChecker();
        checker.go( alg );
        assertEquals( 0, checker.invalidCount );
    }

    //~ Inner Interfaces -------------------------------------------------------


    /**
     * Helper class which contains default implementations of methods used for running sql-to-rel conversion tests.
     */
    public interface Tester {

        /**
         * Converts a SQL string to a {@link AlgNode} tree.
         *
         * @param sql SQL statement
         * @return Relational expression, never null
         */
        AlgRoot convertSqlToAlg( String sql, @Nullable Snapshot snapshot );

        default AlgRoot convertSqlToAlg( String sql ) {
            return convertSqlToAlg( sql, Catalog.snapshot() );
        }

        /**
         * Returns the SQL dialect to test.
         */
        Conformance getConformance();

        /**
         * Checks that a SQL statement converts to a given plan, optionally trimming columns that are not needed.
         *
         * @param sql SQL query
         * @param plan Expected plan
         * @param trim Whether to trim columns that are not needed
         */
        void assertConvertsTo( String sql, String plan, boolean trim );

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
         * Returns a tester that optionally expands sub-queries. If {@code expand} is false, the plan contains a {@link org.polypheny.db.rex.RexSubQuery} for each sub-query.
         *
         * @see Prepare#THREAD_EXPAND
         */
        Tester withExpand( boolean expand );

        /**
         * Returns a tester that optionally uses a {@code SqlToAlgConverter.Config}.
         */
        Tester withConfig( Config config );

        /**
         * Returns a tester with a {@link Conformance}.
         */
        Tester withConformance( Conformance conformance );

        boolean isLateDecorrelate();

    }


    /**
     * Default implementation of {@link Tester}.
     */
    public static class TesterImpl implements Tester {

        private OperatorTable opTab;
        private final DiffRepository diffRepos;
        private final boolean enableDecorrelate;
        private final boolean enableLateDecorrelate;
        private final boolean enableTrim;
        private final boolean enableExpand;
        private final Conformance conformance;
        private final Function<AlgCluster, AlgCluster> clusterFactory;
        private AlgDataTypeFactory typeFactory;
        public final Config config;
        private final Context context;


        /**
         * Creates a TesterImpl.
         *
         * @param diffRepos Diff repository
         * @param enableDecorrelate Whether to decorrelate
         * @param enableTrim Whether to trim unused fields
         * @param enableExpand Whether to expand sub-queries
         * @param clusterFactory Called after a cluster has been created
         */
        protected TesterImpl(
                DiffRepository diffRepos,
                boolean enableDecorrelate,
                boolean enableTrim,
                boolean enableExpand,
                boolean enableLateDecorrelate,
                Function<AlgCluster,
                        AlgCluster> clusterFactory ) {
            this(
                    diffRepos,
                    enableDecorrelate,
                    enableTrim,
                    enableExpand,
                    enableLateDecorrelate,
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
                Function<AlgCluster, AlgCluster> clusterFactory,
                Config config,
                Conformance conformance,
                Context context ) {
            this.diffRepos = diffRepos;
            this.enableDecorrelate = enableDecorrelate;
            this.enableTrim = enableTrim;
            this.enableExpand = enableExpand;
            this.enableLateDecorrelate = enableLateDecorrelate;
            this.clusterFactory = clusterFactory;
            this.config = config;
            this.conformance = conformance;
            this.context = context;


        }


        @SneakyThrows
        @Override
        public AlgRoot convertSqlToAlg( String sql, @Nullable Snapshot snapshot ) {
            if ( snapshot != null ) {
                // ok for testing
                Field field = PolyCatalog.class.getDeclaredField( "snapshot" );
                field.setAccessible( true );
                field.set( Catalog.getInstance(), snapshot );
            }

            QueryLanguage language = QueryLanguage.from( "sql" );

            Processor processor = language.processorSupplier().get();

            List<? extends Node> nodes = processor.parse( sql );

            TransactionManager transactionManager = testHelper.getTransactionManager();

            Transaction transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Sql Test" );

            AlgRoot root = null;
            for ( Node node : nodes ) {
                Pair<Node, AlgDataType> validated = processor.validate( transaction, node, true );
                Statement statement = transaction.createStatement();
                root = processor.translate( statement, ParsedQueryContext.builder()
                        .origin( "Sql Test" )
                        .query( sql )
                        .language( QueryLanguage.from( "sql" ) )
                        .queryNode( validated.left )
                        .build() );
            }
            return root;
        }


        protected final AlgDataTypeFactory getTypeFactory() {
            if ( typeFactory == null ) {
                typeFactory = createTypeFactory();
            }
            return typeFactory;
        }


        protected AlgDataTypeFactory createTypeFactory() {
            return new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        }


        @Override
        public Conformance getConformance() {
            return conformance;
        }


        private SqlValidator createValidator( AlgDataTypeFactory typeFactory ) {
            return new FarragoTestValidator( getOperatorTable(), typeFactory, getConformance() );
        }


        private OperatorTable getOperatorTable() {
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
        public void assertConvertsTo( String sql, String plan, boolean trim ) {
            String sql2 = getDiffRepos().expand( "sql", sql );
            AlgNode alg = convertSqlToAlg( sql2 ).project();

            assertNotNull( alg );
            assertValid( alg );

            if ( trim ) {
                final AlgBuilder algBuilder = AlgFactories.LOGICAL_BUILDER.create( alg.getCluster(), null );
                final AlgFieldTrimmer trimmer = createFieldTrimmer( algBuilder );
                alg = trimmer.trim( alg );
                assertNotNull( alg );
                assertValid( alg );
            }

            // NOTE jvs 28-Mar-2006:  insert leading newline so that plans come out nicely stacked instead of first line immediately after CDATA start
            String actual = NL + AlgOptUtil.toString( alg );
            diffRepos.assertEquals( "plan", plan, actual );
        }


        /**
         * Creates a AlgFieldTrimmer.
         *
         * @param algBuilder Builder
         * @return Field trimmer
         */
        public AlgFieldTrimmer createFieldTrimmer( AlgBuilder algBuilder ) {
            return new AlgFieldTrimmer( getValidator(), algBuilder );
        }


        private DiffRepository getDiffRepos() {
            return diffRepos;
        }


        @Override
        public SqlValidator getValidator() {
            final AlgDataTypeFactory typeFactory = getTypeFactory();
            return createValidator( typeFactory );
        }


        @Override
        public TesterImpl withDecorrelation( boolean enableDecorrelate ) {
            return this.enableDecorrelate == enableDecorrelate
                    ? this
                    : new TesterImpl( diffRepos, enableDecorrelate, enableTrim, enableExpand, enableLateDecorrelate, clusterFactory, config, conformance, context );
        }


        @Override
        public TesterImpl withConfig( Config config ) {
            return this.config == config
                    ? this
                    : new TesterImpl( diffRepos, enableDecorrelate, enableTrim, enableExpand, enableLateDecorrelate, clusterFactory, config, conformance, context );
        }


        @Override
        public Tester withExpand( boolean enableExpand ) {
            return this.enableExpand == enableExpand
                    ? this
                    : new TesterImpl( diffRepos, enableDecorrelate, enableTrim, enableExpand, enableLateDecorrelate, clusterFactory, config, conformance, context );
        }


        @Override
        public Tester withConformance( Conformance conformance ) {
            return new TesterImpl( diffRepos, enableDecorrelate, false, enableExpand, enableLateDecorrelate, clusterFactory, config, conformance, context );
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

        FarragoTestValidator( OperatorTable opTab, AlgDataTypeFactory typeFactory, Conformance conformance ) {
            super( opTab, Catalog.snapshot(), typeFactory, conformance );
        }


        // override SqlValidator
        @Override
        public boolean shouldExpandIdentifiers() {
            return true;
        }

    }

}
