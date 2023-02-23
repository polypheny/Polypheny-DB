/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.AvaticaType;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.linq4j.Linq4j;
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
import org.polypheny.db.adapter.enumerable.EnumerableInterpretable;
import org.polypheny.db.adapter.enumerable.EnumerableInterpreterRule;
import org.polypheny.db.adapter.enumerable.EnumerableRules;
import org.polypheny.db.adapter.enumerable.RexToLixTranslator;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.rules.AggregateExpandDistinctAggregatesRule;
import org.polypheny.db.algebra.rules.AggregateReduceFunctionsRule;
import org.polypheny.db.algebra.rules.AggregateValuesRule;
import org.polypheny.db.algebra.rules.DocumentAggregateToAggregateRule;
import org.polypheny.db.algebra.rules.FilterAggregateTransposeRule;
import org.polypheny.db.algebra.rules.FilterJoinRule;
import org.polypheny.db.algebra.rules.FilterProjectTransposeRule;
import org.polypheny.db.algebra.rules.FilterScanRule;
import org.polypheny.db.algebra.rules.JoinAssociateRule;
import org.polypheny.db.algebra.rules.JoinCommuteRule;
import org.polypheny.db.algebra.rules.JoinPushExpressionsRule;
import org.polypheny.db.algebra.rules.JoinPushThroughJoinRule;
import org.polypheny.db.algebra.rules.ProjectFilterTransposeRule;
import org.polypheny.db.algebra.rules.ProjectMergeRule;
import org.polypheny.db.algebra.rules.ProjectScanRule;
import org.polypheny.db.algebra.rules.ProjectWindowTransposeRule;
import org.polypheny.db.algebra.rules.ReduceExpressionsRules;
import org.polypheny.db.algebra.rules.ScanRule;
import org.polypheny.db.algebra.rules.SortJoinTransposeRule;
import org.polypheny.db.algebra.rules.SortProjectTransposeRule;
import org.polypheny.db.algebra.rules.SortRemoveConstantKeysRule;
import org.polypheny.db.algebra.rules.SortUnionTransposeRule;
import org.polypheny.db.algebra.rules.ValuesReduceRule;
import org.polypheny.db.algebra.stream.StreamRules;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.interpreter.Bindables;
import org.polypheny.db.interpreter.Interpreters;
import org.polypheny.db.languages.NodeToAlgConverter;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
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
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.prepare.Prepare.PreparedExplain;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.schema.ModelTraitDef;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.tools.Frameworks.PrepareAction;
import org.polypheny.db.type.ExtraPolyTypes;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Conformance;
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

    private static final boolean ENABLE_MODEL_TRAIT = true;

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
                    EnumerableRules.ENUMERABLE_CONTEXT_SWITCHER_RULE,
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
                    EnumerableRules.ENUMERABLE_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_WINDOW_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE,
                    EnumerableRules.ENUMERABLE_CALC_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
                    EnumerableRules.ENUMERABLE_TRANSFORMER_RULE,
                    EnumerableRules.ENUMERABLE_GRAPH_MATCH_RULE,
                    EnumerableRules.ENUMERABLE_UNWIND_RULE,
                    EnumerableRules.ENUMERABLE_DOCUMENT_TRANSFORMER_RULE
            );

    public static final List<AlgOptRule> DEFAULT_RULES =
            ImmutableList.of(
                    ScanRule.INSTANCE,
                    RuntimeConfig.JOIN_COMMUTE.getBoolean()
                            ? JoinAssociateRule.INSTANCE
                            : ProjectMergeRule.INSTANCE,
                    FilterScanRule.INSTANCE,
                    ProjectFilterTransposeRule.INSTANCE,
                    FilterProjectTransposeRule.INSTANCE,
                    FilterJoinRule.FILTER_ON_JOIN,
                    JoinPushExpressionsRule.INSTANCE,
                    AggregateExpandDistinctAggregatesRule.INSTANCE,
                    DocumentAggregateToAggregateRule.INSTANCE,
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
                    ReduceExpressionsRules.PROJECT_INSTANCE,
                    ReduceExpressionsRules.FILTER_INSTANCE,
                    ReduceExpressionsRules.CALC_INSTANCE,
                    ReduceExpressionsRules.JOIN_INSTANCE,
                    ValuesReduceRule.FILTER_INSTANCE,
                    ValuesReduceRule.PROJECT_FILTER_INSTANCE,
                    ValuesReduceRule.PROJECT_INSTANCE,
                    AggregateValuesRule.INSTANCE );


    public PolyphenyDbPrepareImpl() {
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
     * Factory method for cluster.
     */
    protected AlgOptCluster createCluster( AlgOptPlanner planner, RexBuilder rexBuilder ) {
        return AlgOptCluster.create( planner, rexBuilder );
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
        if ( ENABLE_MODEL_TRAIT ) {
            planner.addAlgTraitDef( ModelTraitDef.INSTANCE );
            planner.registerModelRules();
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
        planner.addRule( ProjectScanRule.INSTANCE );
        planner.addRule( ProjectScanRule.INTERPRETER );

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



    /**
     * Executes a prepare action.
     */
    public <R> R perform( PrepareAction<R> action ) {
        final Context prepareContext = action.getConfig().getPrepareContext();
        final JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
        final PolyphenyDbSchema schema =
                action.getConfig().getDefaultSchema() != null
                        ? action.getConfig().getDefaultSchema()
                        : prepareContext.getRootSchema();
        PolyphenyDbCatalogReader catalogReader = new PolyphenyDbCatalogReader( schema, typeFactory );
        final RexBuilder rexBuilder = new RexBuilder( typeFactory );
        final AlgOptPlanner planner = createPlanner( prepareContext, action.getConfig().getContext(), action.getConfig().getCostFactory() );
        final AlgOptCluster cluster = createCluster( planner, rexBuilder );
        return action.apply( cluster, catalogReader, prepareContext.getRootSchema() );
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


        @Override
        public AlgNode flattenTypes( AlgNode rootRel, boolean restructure ) {
            return rootRel;
        }


        @Override
        protected AlgNode decorrelate( NodeToAlgConverter sqlToRelConverter, Node query, AlgNode rootRel ) {
            return sqlToRelConverter.decorrelate( query, rootRel );
        }


        protected Validator createSqlValidator( CatalogReader catalogReader ) {
            return QueryLanguage.from( "sql" ).getValidatorSupplier().apply( context, (PolyphenyDbCatalogReader) catalogReader );
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

