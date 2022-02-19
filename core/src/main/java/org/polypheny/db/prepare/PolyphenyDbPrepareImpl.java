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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.prepare;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.AvaticaType;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BinaryExpression;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberExpression;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlg.Prefer;
import org.polypheny.db.adapter.enumerable.EnumerableBindable.EnumerableToBindableConverterRule;
import org.polypheny.db.adapter.enumerable.EnumerableCalc;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableInterpretable;
import org.polypheny.db.adapter.enumerable.EnumerableInterpreterRule;
import org.polypheny.db.adapter.enumerable.EnumerableRules;
import org.polypheny.db.adapter.enumerable.RexToLixTranslator;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.rules.AggregateExpandDistinctAggregatesRule;
import org.polypheny.db.algebra.rules.AggregateReduceFunctionsRule;
import org.polypheny.db.algebra.rules.AggregateValuesRule;
import org.polypheny.db.algebra.rules.FilterAggregateTransposeRule;
import org.polypheny.db.algebra.rules.FilterJoinRule;
import org.polypheny.db.algebra.rules.FilterProjectTransposeRule;
import org.polypheny.db.algebra.rules.FilterTableScanRule;
import org.polypheny.db.algebra.rules.JoinAssociateRule;
import org.polypheny.db.algebra.rules.JoinCommuteRule;
import org.polypheny.db.algebra.rules.JoinPushExpressionsRule;
import org.polypheny.db.algebra.rules.JoinPushThroughJoinRule;
import org.polypheny.db.algebra.rules.ProjectFilterTransposeRule;
import org.polypheny.db.algebra.rules.ProjectMergeRule;
import org.polypheny.db.algebra.rules.ProjectTableScanRule;
import org.polypheny.db.algebra.rules.ProjectWindowTransposeRule;
import org.polypheny.db.algebra.rules.ReduceExpressionsRule;
import org.polypheny.db.algebra.rules.SortJoinTransposeRule;
import org.polypheny.db.algebra.rules.SortProjectTransposeRule;
import org.polypheny.db.algebra.rules.SortRemoveConstantKeysRule;
import org.polypheny.db.algebra.rules.SortUnionTransposeRule;
import org.polypheny.db.algebra.rules.TableScanRule;
import org.polypheny.db.algebra.rules.ValuesReduceRule;
import org.polypheny.db.algebra.stream.StreamRules;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.interpreter.Bindables;
import org.polypheny.db.interpreter.Interpreters;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.NodeToAlgConverter;
import org.polypheny.db.languages.NodeToAlgConverter.ConfigBuilder;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.RexConvertletTable;
import org.polypheny.db.nodes.BinaryOperator;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCostFactory;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.plan.hep.HepProgramBuilder;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.prepare.Prepare.PreparedExplain;
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.tools.Frameworks.PrepareAction;
import org.polypheny.db.type.ExtraPolyTypes;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * This class is public so that projects that create their own JDBC driver and server can fine-tune preferences.
 * However, this class and its methods are subject to change without notice.
 */
public class PolyphenyDbPrepareImpl implements PolyphenyDbPrepare {

    /**
     * Whether to enable the collation trait. Some extra optimizations are possible if enabled, but queries should work
     * either way. At some point this will become a preference, or we will run multiple phases: first disabled, then enabled.
     */
    private static final boolean ENABLE_COLLATION_TRAIT = true;

    /**
     * Whether the bindable convention should be the root convention of any plan. If not, enumerable convention is the default.
     */
    public final boolean enableBindable = Hook.ENABLE_BINDABLE.get( false );

    /**
     * Whether the enumerable convention is enabled.
     */
    public static final boolean ENABLE_ENUMERABLE = true;

    /**
     * Whether the streaming is enabled.
     */
    public static final boolean ENABLE_STREAM = true;

    private static final Set<String> SIMPLE_SQLS =
            ImmutableSet.of(
                    "SELECT 1",
                    "select 1",
                    "SELECT 1 FROM DUAL",
                    "select 1 from dual",
                    "values 1",
                    "VALUES 1" );

    public static final List<AlgOptRule> ENUMERABLE_RULES =
            ImmutableList.of(
                    EnumerableRules.ENUMERABLE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                    EnumerableRules.ENUMERABLE_CONDITIONAL_EXECUTE_RULE,
                    EnumerableRules.ENUMERABLE_CONDITIONAL_EXECUTE_TRUE_RULE,
                    EnumerableRules.ENUMERABLE_CONDITIONAL_EXECUTE_FALSE_RULE,
                    EnumerableRules.ENUMERABLE_STREAMER_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_MODIFY_TO_STREAMER_RULE,
                    EnumerableRules.ENUMERABLE_BATCH_ITERATOR_RULE,
                    EnumerableRules.ENUMERABLE_CONSTRAINT_ENFORCER_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_RULE,
                    EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                    EnumerableRules.ENUMERABLE_SORT_RULE,
                    EnumerableRules.ENUMERABLE_LIMIT_RULE,
                    EnumerableRules.ENUMERABLE_COLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNION_RULE,
                    EnumerableRules.ENUMERABLE_MODIFY_COLLECT_RULE,
                    EnumerableRules.ENUMERABLE_INTERSECT_RULE,
                    EnumerableRules.ENUMERABLE_MINUS_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
                    EnumerableRules.ENUMERABLE_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_WINDOW_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE );

    public static final List<AlgOptRule> DEFAULT_RULES =
            ImmutableList.of(
                    TableScanRule.INSTANCE,
                    RuntimeConfig.JOIN_COMMUTE.getBoolean()
                            ? JoinAssociateRule.INSTANCE
                            : ProjectMergeRule.INSTANCE,
                    FilterTableScanRule.INSTANCE,
                    ProjectFilterTransposeRule.INSTANCE,
                    FilterProjectTransposeRule.INSTANCE,
                    FilterJoinRule.FILTER_ON_JOIN,
                    JoinPushExpressionsRule.INSTANCE,
                    AggregateExpandDistinctAggregatesRule.INSTANCE,
                    AggregateReduceFunctionsRule.INSTANCE,
                    FilterAggregateTransposeRule.INSTANCE,
                    ProjectWindowTransposeRule.INSTANCE,
                    JoinCommuteRule.INSTANCE,
                    JoinPushThroughJoinRule.RIGHT,
                    JoinPushThroughJoinRule.LEFT,
                    SortProjectTransposeRule.INSTANCE,
                    SortJoinTransposeRule.INSTANCE,
                    SortRemoveConstantKeysRule.INSTANCE,
                    SortUnionTransposeRule.INSTANCE );

    public static final List<AlgOptRule> CONSTANT_REDUCTION_RULES =
            ImmutableList.of(
                    ReduceExpressionsRule.PROJECT_INSTANCE,
                    ReduceExpressionsRule.FILTER_INSTANCE,
                    ReduceExpressionsRule.CALC_INSTANCE,
                    ReduceExpressionsRule.JOIN_INSTANCE,
                    ValuesReduceRule.FILTER_INSTANCE,
                    ValuesReduceRule.PROJECT_FILTER_INSTANCE,
                    ValuesReduceRule.PROJECT_INSTANCE,
                    AggregateValuesRule.INSTANCE );


    public PolyphenyDbPrepareImpl() {
    }


    @Override
    public ParseResult parse( Context context, String sql ) {
        return parse_( context, sql, false, false, false );
    }


    @Override
    public ConvertResult convert( Context context, String sql ) {
        return (ConvertResult) parse_( context, sql, true, false, false );
    }


    /**
     * Shared implementation for {@link #parse} and {@link #convert}.
     */
    private ParseResult parse_( Context context, String sql, boolean convert, boolean analyze, boolean fail ) {
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        PolyphenyDbCatalogReader catalogReader = new PolyphenyDbCatalogReader(
                context.getRootSchema(),
                context.getDefaultSchemaPath(),
                typeFactory );
        Parser parser = createParser( sql );
        Node sqlNode;
        try {
            sqlNode = parser.parseStmt();
        } catch ( NodeParseException e ) {
            throw new RuntimeException( "parse failed", e );
        }
        final Validator validator = LanguageManager.getInstance().createValidator( QueryLanguage.SQL, context, catalogReader );
        Node sqlNode1 = validator.validate( sqlNode );
        if ( convert ) {
            return convert_( context, sql, analyze, fail, catalogReader, validator, sqlNode1 );
        }
        return new ParseResult( this, validator, sql, sqlNode1, validator.getValidatedNodeType( sqlNode1 ) );
    }


    private ParseResult convert_( Context context, String sql, boolean analyze, boolean fail, PolyphenyDbCatalogReader catalogReader, Validator validator, Node sqlNode1 ) {
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final Convention resultConvention =
                enableBindable
                        ? BindableConvention.INSTANCE
                        : EnumerableConvention.INSTANCE;
        final HepPlanner planner = new HepPlanner( new HepProgramBuilder().build() );
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        final ConfigBuilder configBuilder = NodeToAlgConverter.configBuilder().trimUnusedFields( true );
        if ( analyze ) {
            configBuilder.convertTableAccess( false );
        }

        final PolyphenyDbPreparingStmt preparingStmt = new PolyphenyDbPreparingStmt(
                this,
                context,
                catalogReader,
                typeFactory,
                context.getRootSchema(),
                null,
                planner,
                resultConvention,
                createConvertletTable() );
        final NodeToAlgConverter converter = preparingStmt.getSqlToRelConverter( validator, catalogReader, new ConfigBuilder().build() );

        final AlgRoot root = converter.convertQuery( sqlNode1, false, true );
        return new ConvertResult( this, validator, sql, sqlNode1, validator.getValidatedNodeType( sqlNode1 ), root );
    }


    @Override
    public void executeDdl( Context context, Node node ) {
        if ( node instanceof ExecutableStatement ) {
            ExecutableStatement statement = (ExecutableStatement) node;
            statement.execute( context, null, null );
            return;
        }
        throw new UnsupportedOperationException();
    }


    /**
     * Factory method for default SQL parser.
     */
    protected Parser createParser( String sql ) {
        return createParser( sql, createParserConfig() );
    }


    /**
     * Factory method for SQL parser with a given configuration.
     */
    protected Parser createParser( String sql, Parser.ConfigBuilder parserConfig ) {
        return Parser.create( sql, parserConfig.build() );
    }


    /**
     * Factory method for SQL parser configuration.
     */
    protected Parser.ConfigBuilder createParserConfig() {
        return Parser.configBuilder();
    }


    /**
     * Factory method for default convertlet table.
     */
    protected RexConvertletTable createConvertletTable() {
        return LanguageManager.getInstance().getStandardConvertlet();
    }


    /**
     * Factory method for cluster.
     */
    protected AlgOptCluster createCluster( AlgOptPlanner planner, RexBuilder rexBuilder ) {
        return AlgOptCluster.create( planner, rexBuilder );
    }


    /**
     * Creates a collection of planner factories.
     * <p>
     * The collection must have at least one factory, and each factory must create a planner. If the collection has more
     * than one planner, Polypheny-DB will try each planner in turn.
     * <p>
     * One of the things you can do with this mechanism is to try a simpler, faster, planner with a smaller rule set first,
     * then fall back to a more complex planner for complex and costly queries.
     * <p>
     * The default implementation returns a factory that calls {@link #createPlanner(Context)}.
     */
    protected List<Function1<Context, AlgOptPlanner>> createPlannerFactories() {
        return Collections.singletonList( context -> createPlanner( context, null, null ) );
    }


    /**
     * Creates a query planner and initializes it with a default set of rules.
     */
    protected AlgOptPlanner createPlanner( Context prepareContext ) {
        return createPlanner( prepareContext, null, null );
    }


    /**
     * Creates a query planner and initializes it with a default set of rules.
     */
    protected AlgOptPlanner createPlanner( final Context prepareContext, org.polypheny.db.plan.Context externalContext, AlgOptCostFactory costFactory ) {
        if ( externalContext == null ) {
            externalContext = Contexts.of( prepareContext.config() );
        }
        final VolcanoPlanner planner = new VolcanoPlanner( costFactory, externalContext );
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );
        if ( ENABLE_COLLATION_TRAIT ) {
            planner.addAlgTraitDef( AlgCollationTraitDef.INSTANCE );
            planner.registerAbstractRelationalRules();
        }
        AlgOptUtil.registerAbstractAlgs( planner );
        for ( AlgOptRule rule : DEFAULT_RULES ) {
            planner.addRule( rule );
        }
        if ( enableBindable ) {
            for ( AlgOptRule rule : Bindables.RULES ) {
                planner.addRule( rule );
            }
        }
        planner.addRule( Bindables.BINDABLE_TABLE_SCAN_RULE );
        planner.addRule( ProjectTableScanRule.INSTANCE );
        planner.addRule( ProjectTableScanRule.INTERPRETER );

        if ( ENABLE_ENUMERABLE ) {
            for ( AlgOptRule rule : ENUMERABLE_RULES ) {
                planner.addRule( rule );
            }
            planner.addRule( EnumerableInterpreterRule.INSTANCE );
        }

        if ( enableBindable && ENABLE_ENUMERABLE ) {
            planner.addRule( EnumerableToBindableConverterRule.INSTANCE );
        }

        if ( ENABLE_STREAM ) {
            for ( AlgOptRule rule : StreamRules.RULES ) {
                planner.addRule( rule );
            }
        }

        // Change the below to enable constant-reduction.
        if ( false ) {
            for ( AlgOptRule rule : CONSTANT_REDUCTION_RULES ) {
                planner.addRule( rule );
            }
        }

        Hook.PLANNER.run( planner ); // allow test to add or remove rules

        return planner;
    }


    /**
     * Deduces the broad type of statement. Currently returns SELECT for most statement types, but this may change.
     *
     * @param kind Kind of statement
     */
    private StatementType getStatementType( Kind kind ) {
        switch ( kind ) {
            case INSERT:
            case DELETE:
            case UPDATE:
                return StatementType.IS_DML;
            default:
                return StatementType.SELECT;
        }
    }


    /**
     * Deduces the broad type of statement for a prepare result.
     * Currently returns SELECT for most statement types, but this may change.
     *
     * @param preparedResult Prepare result
     */
    private StatementType getStatementType( PreparedResult preparedResult ) {
        if ( preparedResult.isDml() ) {
            return StatementType.IS_DML;
        } else {
            return StatementType.SELECT;
        }
    }


    /*<T> PolyphenyDbSignature<T> prepare2_( Context context, Query<T> query, Type elementType, long maxRowCount, PolyphenyDbCatalogReader catalogReader, AlgOptPlanner planner ) {
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final Prefer prefer;
        if ( elementType == Object[].class ) {
            prefer = Prefer.ARRAY;
        } else {
            prefer = Prefer.CUSTOM;
        }
        final Convention resultConvention =
                enableBindable
                        ? BindableConvention.INSTANCE
                        : EnumerableConvention.INSTANCE;
        final PolyphenyDbPreparingStmt preparingStmt = new PolyphenyDbPreparingStmt(
                this,
                context,
                catalogReader,
                typeFactory,
                context.getRootSchema(),
                prefer,
                planner,
                resultConvention,
                createConvertletTable() );

        final AlgDataType x;
        final PreparedResult preparedResult;
        final StatementType statementType;
        if ( query.sql != null ) {
            final PolyphenyDbConnectionConfig config = context.config();
            final Parser.ConfigBuilder parserConfig = createParserConfig()
                    .setQuotedCasing( config.quotedCasing() )
                    .setUnquotedCasing( config.unquotedCasing() )
                    .setQuoting( config.quoting() )
                    .setConformance( config.conformance() )
                    .setCaseSensitive( RuntimeConfig.CASE_SENSITIVE.getBoolean() );
            final ParserFactory parserFactory = config.parserFactory( ParserFactory.class, null );
            if ( parserFactory != null ) {
                parserConfig.setParserFactory( parserFactory );
            }
            Parser parser = createParser( query.sql, parserConfig );
            Node sqlNode;
            try {
                sqlNode = parser.parseStmt();
                statementType = getStatementType( sqlNode.getKind() );
            } catch ( NodeParseException e ) {
                throw new RuntimeException( "parse failed: " + e.getMessage(), e );
            }

            Hook.PARSE_TREE.run( new Object[]{ query.sql, sqlNode } );

            if ( sqlNode.getKind().belongsTo( Kind.DDL ) ) {
                executeDdl( context, sqlNode );

                return new PolyphenyDbSignature<>(
                        query.sql,
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        null,
                        ImmutableList.of(),
                        CursorFactory.OBJECT,
                        null,
                        ImmutableList.of(),
                        -1,
                        null,
                        StatementType.OTHER_DDL,
                        new ExecutionTimeMonitor(),
                        SchemaType.RELATIONAL );
            }

            final Validator validator = LanguageManager.getInstance().createValidator( QueryLanguage.SQL, context, catalogReader );
            validator.setIdentifierExpansion( true );
            validator.setDefaultNullCollation( config.defaultNullCollation() );

            preparedResult = preparingStmt.prepareSql( sqlNode, Object.class, validator, true );
            switch ( sqlNode.getKind() ) {
                case INSERT:
                case DELETE:
                case UPDATE:
                case EXPLAIN:
                    // FIXME: getValidatedNodeType is wrong for DML
                    x = AlgOptUtil.createDmlRowType( sqlNode.getKind(), typeFactory );
                    break;
                default:
                    x = validator.getValidatedNodeType( sqlNode );
            }
        } else if ( query.queryable != null ) {
            x = context.getTypeFactory().createType( elementType );
            preparedResult = preparingStmt.prepareQueryable( query.queryable, x );
            statementType = getStatementType( preparedResult );
        } else {
            assert query.alg != null;
            x = query.alg.getRowType();
            preparedResult = preparingStmt.prepareRel( query.alg );
            statementType = getStatementType( preparedResult );
        }

        final List<AvaticaParameter> parameters = new ArrayList<>();
        final AlgDataType parameterRowType = preparedResult.getParameterRowType();
        for ( AlgDataTypeField field : parameterRowType.getFieldList() ) {
            AlgDataType type = field.getType();
            parameters.add(
                    new AvaticaParameter(
                            false,
                            getPrecision( type ),
                            getScale( type ),
                            getTypeOrdinal( type ),
                            getTypeName( type ),
                            getClassName( type ),
                            field.getName() ) );
        }

        AlgDataType jdbcType = makeStruct( typeFactory, x );
        final List<List<String>> originList = preparedResult.getFieldOrigins();
        final List<ColumnMetaData> columns = getColumnMetaDataList( typeFactory, x, jdbcType, originList );
        Class resultClazz = null;
        if ( preparedResult instanceof Typed ) {
            resultClazz = (Class) ((Typed) preparedResult).getElementType();
        }
        final CursorFactory cursorFactory =
                preparingStmt.resultConvention == BindableConvention.INSTANCE
                        ? CursorFactory.ARRAY
                        : CursorFactory.deduce( columns, resultClazz );
        //noinspection unchecked
        final Bindable<T> bindable = preparedResult.getBindable( cursorFactory );
        return new PolyphenyDbSignature<>(
                query.sql,
                parameters,
                preparingStmt.internalParameters,
                jdbcType,
                columns,
                cursorFactory,
                context.getRootSchema(),
                preparedResult instanceof PreparedResultImpl
                        ? ((PreparedResultImpl) preparedResult).collations
                        : ImmutableList.of(),
                maxRowCount,
                bindable,
                statementType,
                new ExecutionTimeMonitor(),
                SchemaType.RELATIONAL );
    }*/


    private List<ColumnMetaData> getColumnMetaDataList( JavaTypeFactory typeFactory, AlgDataType x, AlgDataType jdbcType, List<List<String>> originList ) {
        final List<ColumnMetaData> columns = new ArrayList<>();
        for ( Ord<AlgDataTypeField> pair : Ord.zip( jdbcType.getFieldList() ) ) {
            final AlgDataTypeField field = pair.e;
            final AlgDataType type = field.getType();
            final AlgDataType fieldType = x.isStruct() ? x.getFieldList().get( pair.i ).getType() : type;
            columns.add( metaData( typeFactory, columns.size(), field.getName(), type, fieldType, originList.get( pair.i ) ) );
        }
        return columns;
    }


    private ColumnMetaData metaData( JavaTypeFactory typeFactory, int ordinal, String fieldName, AlgDataType type, AlgDataType fieldType, List<String> origins ) {
        final AvaticaType avaticaType = avaticaType( typeFactory, type, fieldType );
        return new ColumnMetaData(
                ordinal,
                false,
                true,
                false,
                false,
                type.isNullable()
                        ? DatabaseMetaData.columnNullable
                        : DatabaseMetaData.columnNoNulls,
                true,
                type.getPrecision(),
                fieldName,
                origin( origins, 0 ),
                origin( origins, 2 ),
                getPrecision( type ),
                getScale( type ),
                origin( origins, 1 ),
                null,
                avaticaType,
                true,
                false,
                false,
                avaticaType.columnClassName() );
    }


    private AvaticaType avaticaType( JavaTypeFactory typeFactory, AlgDataType type, AlgDataType fieldType ) {
        final String typeName = getTypeName( type );
        if ( type.getComponentType() != null ) {
            final AvaticaType componentType = avaticaType( typeFactory, type.getComponentType(), null );
            final Type clazz = typeFactory.getJavaClass( type.getComponentType() );
            final Rep rep = Rep.of( clazz );
            assert rep != null;
            return ColumnMetaData.array( componentType, typeName, rep );
        } else {
            int typeOrdinal = getTypeOrdinal( type );
            switch ( typeOrdinal ) {
                case Types.STRUCT:
                    final List<ColumnMetaData> columns = new ArrayList<>();
                    for ( AlgDataTypeField field : type.getFieldList() ) {
                        columns.add( metaData( typeFactory, field.getIndex(), field.getName(), field.getType(), null, null ) );
                    }
                    return ColumnMetaData.struct( columns );
                case ExtraPolyTypes.GEOMETRY:
                    typeOrdinal = Types.VARCHAR;
                    // fall through
                default:
                    final Type clazz = typeFactory.getJavaClass( Util.first( fieldType, type ) );
                    final Rep rep = Rep.of( clazz );
                    assert rep != null;
                    return ColumnMetaData.scalar( typeOrdinal, typeName, rep );
            }
        }
    }


    private static String origin( List<String> origins, int offsetFromEnd ) {
        return origins == null || offsetFromEnd >= origins.size()
                ? null
                : origins.get( origins.size() - 1 - offsetFromEnd );
    }


    private int getTypeOrdinal( AlgDataType type ) {
        return type.getPolyType().getJdbcOrdinal();
    }


    private static String getClassName( AlgDataType type ) {
        return Object.class.getName(); // POLYPHENYDB-2613
    }


    private static int getScale( AlgDataType type ) {
        return type.getScale() == AlgDataType.SCALE_NOT_SPECIFIED
                ? 0
                : type.getScale();
    }


    private static int getPrecision( AlgDataType type ) {
        return type.getPrecision() == AlgDataType.PRECISION_NOT_SPECIFIED
                ? 0
                : type.getPrecision();
    }


    /**
     * Returns the type name in string form. Does not include precision, scale or whether nulls are allowed.
     * Example: "DECIMAL" not "DECIMAL(7, 2)"; "INTEGER" not "JavaType(int)".
     */
    private static String getTypeName( AlgDataType type ) {
        final PolyType polyType = type.getPolyType();
        switch ( polyType ) {
            /*
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
                return type.toString(); // e.g. "INTEGER ARRAY"
             */
            case INTERVAL_YEAR_MONTH:
                return "INTERVAL_YEAR_TO_MONTH";
            case INTERVAL_DAY_HOUR:
                return "INTERVAL_DAY_TO_HOUR";
            case INTERVAL_DAY_MINUTE:
                return "INTERVAL_DAY_TO_MINUTE";
            case INTERVAL_DAY_SECOND:
                return "INTERVAL_DAY_TO_SECOND";
            case INTERVAL_HOUR_MINUTE:
                return "INTERVAL_HOUR_TO_MINUTE";
            case INTERVAL_HOUR_SECOND:
                return "INTERVAL_HOUR_TO_SECOND";
            case INTERVAL_MINUTE_SECOND:
                return "INTERVAL_MINUTE_TO_SECOND";
            default:
                return polyType.getName(); // e.g. "DECIMAL", "INTERVAL_YEAR_MONTH"
        }
    }


    private static AlgDataType makeStruct( AlgDataTypeFactory typeFactory, AlgDataType type ) {
        if ( type.isStruct() ) {
            return type;
        }
        return typeFactory.builder().add( "$0", null, type ).build();
    }


    /**
     * Executes a prepare action.
     */
    public <R> R perform( PrepareAction<R> action ) {
        final Context prepareContext = action.getConfig().getPrepareContext();
        final JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
        final PolyphenyDbSchema schema =
                action.getConfig().getDefaultSchema() != null
                        ? PolyphenyDbSchema.from( action.getConfig().getDefaultSchema() )
                        : prepareContext.getRootSchema();
        PolyphenyDbCatalogReader catalogReader = new PolyphenyDbCatalogReader( schema.root(), schema.path( null ), typeFactory );
        final RexBuilder rexBuilder = new RexBuilder( typeFactory );
        final AlgOptPlanner planner = createPlanner( prepareContext, action.getConfig().getContext(), action.getConfig().getCostFactory() );
        final AlgOptCluster cluster = createCluster( planner, rexBuilder );
        return action.apply( cluster, catalogReader, prepareContext.getRootSchema().plus() );
    }


    /**
     * Holds state for the process of preparing a SQL statement.
     */
    public static class PolyphenyDbPreparingStmt extends Prepare {

        protected final AlgOptPlanner planner;
        protected final RexBuilder rexBuilder;
        protected final PolyphenyDbPrepareImpl prepare;
        protected final PolyphenyDbSchema schema;
        protected final AlgDataTypeFactory typeFactory;
        protected final RexConvertletTable convertletTable;
        private final Prefer prefer;
        private final Map<String, Object> internalParameters = new LinkedHashMap<>();
        private int expansionDepth;
        private Validator sqlValidator;


        PolyphenyDbPreparingStmt(
                PolyphenyDbPrepareImpl prepare,
                Context context,
                CatalogReader catalogReader,
                AlgDataTypeFactory typeFactory,
                PolyphenyDbSchema schema,
                Prefer prefer,
                AlgOptPlanner planner,
                Convention resultConvention,
                RexConvertletTable convertletTable ) {
            super( context, catalogReader, resultConvention );
            this.prepare = prepare;
            this.schema = schema;
            this.prefer = prefer;
            this.planner = planner;
            this.typeFactory = typeFactory;
            this.convertletTable = convertletTable;
            this.rexBuilder = new RexBuilder( typeFactory );
        }


        @Override
        protected void init( Class runtimeContextClass ) {
        }


        public PreparedResult prepareQueryable( final Queryable queryable, AlgDataType resultType ) {
            return prepare_( () -> {
                final AlgOptCluster cluster = prepare.createCluster( planner, rexBuilder );
                return new LixToAlgTranslator( cluster ).translate( queryable );
            }, resultType );
        }


        public PreparedResult prepareRel( final AlgNode alg ) {
            return prepare_( () -> alg, alg.getRowType() );
        }


        private PreparedResult prepare_( Supplier<AlgNode> fn, AlgDataType resultType ) {
            Class runtimeContextClass = Object.class;
            init( runtimeContextClass );

            final AlgNode alg = fn.get();
            final AlgDataType rowType = alg.getRowType();
            final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
            final AlgCollation collation =
                    alg instanceof Sort
                            ? ((Sort) alg).collation
                            : AlgCollations.EMPTY;
            AlgRoot root = new AlgRoot( alg, resultType, Kind.SELECT, fields, collation );

            if ( timingTracer != null ) {
                timingTracer.traceTime( "end sql2rel" );
            }

            final AlgDataType jdbcType = makeStruct( rexBuilder.getTypeFactory(), resultType );
            fieldOrigins = Collections.nCopies( jdbcType.getFieldCount(), null );
            parameterRowType = rexBuilder.getTypeFactory().builder().build();

            // Structured type flattening, view expansion, and plugging in physical storage.
            root = root.withAlg( flattenTypes( root.alg, true ) );

            // Trim unused fields.
            root = trimUnusedFields( root );

            root = optimize( root );

            if ( timingTracer != null ) {
                timingTracer.traceTime( "end optimization" );
            }

            return implement( root );
        }


        @Override
        protected NodeToAlgConverter getSqlToRelConverter( Validator validator, CatalogReader catalogReader, NodeToAlgConverter.Config config ) {
            final AlgOptCluster cluster = prepare.createCluster( planner, rexBuilder );
            return LanguageManager.getInstance().createToRelConverter( QueryLanguage.SQL, validator, catalogReader, cluster, convertletTable, config );
        }


        @Override
        public AlgNode flattenTypes( AlgNode rootRel, boolean restructure ) {
            return rootRel;
        }


        @Override
        protected AlgNode decorrelate( NodeToAlgConverter sqlToRelConverter, Node query, AlgNode rootRel ) {
            return sqlToRelConverter.decorrelate( query, rootRel );
        }


        protected Validator createSqlValidator( CatalogReader catalogReader ) {
            return LanguageManager.getInstance().createValidator( QueryLanguage.SQL, context, (PolyphenyDbCatalogReader) catalogReader );
        }


        @Override
        protected Validator getSqlValidator() {
            if ( sqlValidator == null ) {
                sqlValidator = createSqlValidator( catalogReader );
            }
            return sqlValidator;
        }


        @Override
        protected PreparedResult createPreparedExplanation( AlgDataType resultType, AlgDataType parameterRowType, AlgRoot root, ExplainFormat format, ExplainLevel detailLevel ) {
            return new PolyphenyDbPreparedExplain( resultType, parameterRowType, root, format, detailLevel );
        }


        @Override
        protected PreparedResult implement( AlgRoot root ) {
            AlgDataType resultType = root.alg.getRowType();
            boolean isDml = root.kind.belongsTo( Kind.DML );
            final Bindable bindable;
            final String generatedCode;
            if ( resultConvention == BindableConvention.INSTANCE ) {
                bindable = Interpreters.bindable( root.alg );
                generatedCode = null;
            } else {
                EnumerableAlg enumerable = (EnumerableAlg) root.alg;
                if ( !root.isRefTrivial() ) {
                    final List<RexNode> projects = new ArrayList<>();
                    final RexBuilder rexBuilder = enumerable.getCluster().getRexBuilder();
                    for ( int field : Pair.left( root.fields ) ) {
                        projects.add( rexBuilder.makeInputRef( enumerable, field ) );
                    }
                    RexProgram program = RexProgram.create(
                            enumerable.getRowType(),
                            projects,
                            null,
                            root.validatedRowType,
                            rexBuilder );
                    enumerable = EnumerableCalc.create( enumerable, program );
                }

                try {
                    CatalogReader.THREAD_LOCAL.set( catalogReader );
                    final Conformance conformance = context.config().conformance();
                    internalParameters.put( "_conformance", conformance );
                    Pair<Bindable<Object[]>, String> implementationPair = EnumerableInterpretable.toBindable(
                            internalParameters,
                            enumerable,
                            prefer,
                            null );
                    bindable = implementationPair.left;
                    generatedCode = implementationPair.right;
                } finally {
                    CatalogReader.THREAD_LOCAL.remove();
                }
            }

            if ( timingTracer != null ) {
                timingTracer.traceTime( "end codegen" );
            }

            if ( timingTracer != null ) {
                timingTracer.traceTime( "end compilation" );
            }

            return new PreparedResultImpl(
                    resultType,
                    parameterRowType,
                    fieldOrigins,
                    root.collation.getFieldCollations().isEmpty()
                            ? ImmutableList.of()
                            : ImmutableList.of( root.collation ),
                    root.alg,
                    mapTableModOp( isDml, root.kind ),
                    isDml ) {
                @Override
                public String getCode() {
                    return generatedCode;
                }


                @Override
                public Bindable getBindable( CursorFactory cursorFactory ) {
                    return bindable;
                }


                @Override
                public Type getElementType() {
                    return ((Typed) bindable).getElementType();
                }
            };
        }

    }


    /**
     * An {@code EXPLAIN} statement, prepared and ready to execute.
     */
    private static class PolyphenyDbPreparedExplain extends PreparedExplain {

        PolyphenyDbPreparedExplain( AlgDataType resultType, AlgDataType parameterRowType, AlgRoot root, ExplainFormat format, ExplainLevel detailLevel ) {
            super( resultType, parameterRowType, root, format, detailLevel );
        }


        @Override
        public Bindable getBindable( final CursorFactory cursorFactory ) {
            final String explanation = getCode();
            return dataContext -> {
                switch ( cursorFactory.style ) {
                    case ARRAY:
                        return Linq4j.singletonEnumerable( new String[]{ explanation } );
                    case OBJECT:
                    default:
                        return Linq4j.singletonEnumerable( explanation );
                }
            };
        }

    }


    /**
     * Translator from Java AST to {@link RexNode}.
     */
    interface ScalarTranslator {

        RexNode toRex( BlockStatement statement );

        List<RexNode> toRexList( BlockStatement statement );

        RexNode toRex( Expression expression );

        ScalarTranslator bind( List<ParameterExpression> parameterList, List<RexNode> values );

    }


    /**
     * Basic translator.
     */
    static class EmptyScalarTranslator implements ScalarTranslator {

        private final RexBuilder rexBuilder;


        EmptyScalarTranslator( RexBuilder rexBuilder ) {
            this.rexBuilder = rexBuilder;
        }


        public static ScalarTranslator empty( RexBuilder builder ) {
            return new EmptyScalarTranslator( builder );
        }


        @Override
        public List<RexNode> toRexList( BlockStatement statement ) {
            final List<Expression> simpleList = simpleList( statement );
            final List<RexNode> list = new ArrayList<>();
            for ( Expression expression1 : simpleList ) {
                list.add( toRex( expression1 ) );
            }
            return list;
        }


        @Override
        public RexNode toRex( BlockStatement statement ) {
            return toRex( Blocks.simple( statement ) );
        }


        private static List<Expression> simpleList( BlockStatement statement ) {
            Expression simple = Blocks.simple( statement );
            if ( simple instanceof NewExpression ) {
                NewExpression newExpression = (NewExpression) simple;
                return newExpression.arguments;
            } else {
                return Collections.singletonList( simple );
            }
        }


        @Override
        public RexNode toRex( Expression expression ) {
            switch ( expression.getNodeType() ) {
                case MemberAccess:
                    // Case-sensitive name match because name was previously resolved.
                    return rexBuilder.makeFieldAccess(
                            toRex( ((MemberExpression) expression).expression ),
                            ((MemberExpression) expression).field.getName(),
                            true );
                case GreaterThan:
                    return binary( expression, OperatorRegistry.get( OperatorName.GREATER_THAN, BinaryOperator.class ) );
                case LessThan:
                    return binary( expression, OperatorRegistry.get( OperatorName.LESS_THAN, BinaryOperator.class ) );
                case Parameter:
                    return parameter( (ParameterExpression) expression );
                case Call:
                    MethodCallExpression call = (MethodCallExpression) expression;
                    Operator operator = RexToLixTranslator.JAVA_TO_SQL_METHOD_MAP.get( call.method );
                    if ( operator != null ) {
                        return rexBuilder.makeCall(
                                type( call ),
                                operator,
                                toRex( Expressions.<Expression>list()
                                        .appendIfNotNull( call.targetExpression )
                                        .appendAll( call.expressions ) ) );
                    }
                    throw new RuntimeException( "Could translate call to method " + call.method );
                case Constant:
                    final ConstantExpression constant = (ConstantExpression) expression;
                    Object value = constant.value;
                    if ( value instanceof Number ) {
                        Number number = (Number) value;
                        if ( value instanceof Double || value instanceof Float ) {
                            return rexBuilder.makeApproxLiteral( BigDecimal.valueOf( number.doubleValue() ) );
                        } else if ( value instanceof BigDecimal ) {
                            return rexBuilder.makeExactLiteral( (BigDecimal) value );
                        } else {
                            return rexBuilder.makeExactLiteral( BigDecimal.valueOf( number.longValue() ) );
                        }
                    } else if ( value instanceof Boolean ) {
                        return rexBuilder.makeLiteral( (Boolean) value );
                    } else {
                        return rexBuilder.makeLiteral( constant.toString() );
                    }
                default:
                    throw new UnsupportedOperationException( "unknown expression type " + expression.getNodeType() + " " + expression );
            }
        }


        private RexNode binary( Expression expression, BinaryOperator op ) {
            BinaryExpression call = (BinaryExpression) expression;
            return rexBuilder.makeCall( type( call ), op, toRex( ImmutableList.of( call.expression0, call.expression1 ) ) );
        }


        private List<RexNode> toRex( List<Expression> expressions ) {
            final List<RexNode> list = new ArrayList<>();
            for ( Expression expression : expressions ) {
                list.add( toRex( expression ) );
            }
            return list;
        }


        protected AlgDataType type( Expression expression ) {
            final Type type = expression.getType();
            return ((JavaTypeFactory) rexBuilder.getTypeFactory()).createType( type );
        }


        @Override
        public ScalarTranslator bind( List<ParameterExpression> parameterList, List<RexNode> values ) {
            return new LambdaScalarTranslator( rexBuilder, parameterList, values );
        }


        public RexNode parameter( ParameterExpression param ) {
            throw new RuntimeException( "unknown parameter " + param );
        }

    }


    /**
     * Translator that looks for parameters.
     */
    private static class LambdaScalarTranslator extends EmptyScalarTranslator {

        private final List<ParameterExpression> parameterList;
        private final List<RexNode> values;


        LambdaScalarTranslator( RexBuilder rexBuilder, List<ParameterExpression> parameterList, List<RexNode> values ) {
            super( rexBuilder );
            this.parameterList = parameterList;
            this.values = values;
        }


        @Override
        public RexNode parameter( ParameterExpression param ) {
            int i = parameterList.indexOf( param );
            if ( i >= 0 ) {
                return values.get( i );
            }
            throw new RuntimeException( "unknown parameter " + param );
        }

    }

}

