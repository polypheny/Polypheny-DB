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

package org.polypheny.db.sql.sql2alg;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import kotlin.text.Charsets;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgDecorrelator;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgFieldCollation.NullDirection;
import org.polypheny.db.algebra.AlgFieldTrimmer;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgStructuredTypeFlattener;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.JoinConditionType;
import org.polypheny.db.algebra.constant.JoinType;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.AlgFactories.FilterFactory;
import org.polypheny.db.algebra.core.Collect;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sample;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Uncollect;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.relational.LogicalAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalJoin;
import org.polypheny.db.algebra.logical.relational.LogicalMatch;
import org.polypheny.db.algebra.logical.relational.LogicalMinus;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelViewScan;
import org.polypheny.db.algebra.logical.relational.LogicalSort;
import org.polypheny.db.algebra.logical.relational.LogicalTableFunctionScan;
import org.polypheny.db.algebra.logical.relational.LogicalUnion;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.metadata.AlgColumnMapping;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.metadata.JaninoRelMetadataProvider;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.stream.Delta;
import org.polypheny.db.algebra.stream.LogicalDelta;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.NodeToAlgConverter;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.BasicNodeVisitor;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.DataTypeSpec;
import org.polypheny.db.nodes.DynamicParam;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeList;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.validate.ValidatorTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptSamplingParameters;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCallBinding;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexFieldCollation;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexPatternFieldRef;
import org.polypheny.db.rex.RexRangeRef;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.rex.RexWindowBound;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlDelete;
import org.polypheny.db.sql.language.SqlDynamicParam;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlInsert;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.SqlJoin;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlMatchRecognize;
import org.polypheny.db.sql.language.SqlMerge;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlNumericLiteral;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlOrderBy;
import org.polypheny.db.sql.language.SqlSampleSpec;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.SqlSelectKeyword;
import org.polypheny.db.sql.language.SqlSetOperator;
import org.polypheny.db.sql.language.SqlUnnestOperator;
import org.polypheny.db.sql.language.SqlUpdate;
import org.polypheny.db.sql.language.SqlUtil;
import org.polypheny.db.sql.language.SqlValuesOperator;
import org.polypheny.db.sql.language.SqlWindow;
import org.polypheny.db.sql.language.SqlWith;
import org.polypheny.db.sql.language.SqlWithItem;
import org.polypheny.db.sql.language.fun.SqlCase;
import org.polypheny.db.sql.language.fun.SqlCountAggFunction;
import org.polypheny.db.sql.language.fun.SqlInOperator;
import org.polypheny.db.sql.language.fun.SqlQuantifyOperator;
import org.polypheny.db.sql.language.fun.SqlRowOperator;
import org.polypheny.db.sql.language.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.language.validate.AggregatingSelectScope;
import org.polypheny.db.sql.language.validate.CollectNamespace;
import org.polypheny.db.sql.language.validate.DelegatingScope;
import org.polypheny.db.sql.language.validate.ListScope;
import org.polypheny.db.sql.language.validate.MatchRecognizeScope;
import org.polypheny.db.sql.language.validate.ParameterScope;
import org.polypheny.db.sql.language.validate.SelectScope;
import org.polypheny.db.sql.language.validate.SqlQualified;
import org.polypheny.db.sql.language.validate.SqlUserDefinedTableFunction;
import org.polypheny.db.sql.language.validate.SqlUserDefinedTableMacro;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorImpl;
import org.polypheny.db.sql.language.validate.SqlValidatorNamespace;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.sql.language.validate.SqlValidatorUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.type.inference.TableFunctionReturnTypeInference;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.InitializerContext;
import org.polypheny.db.util.InitializerExpressionFactory;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.NameMatcher;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.NullInitializerExpressionFactory;
import org.polypheny.db.util.NumberUtil;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.ValidatorUtil;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Converts a SQL parse tree (consisting of {@link SqlNode} objects) into an algebra expression (consisting of {@link AlgNode} objects).
 * <p>
 * The public entry points are: {@link #convertQuery}, {@link #convertExpression(SqlNode)}.
 */
public class SqlToAlgConverter implements NodeToAlgConverter {

    public static final Logger SQL2REL_LOGGER = PolyphenyDbTrace.getSqlToRelTracer();

    private static final BigDecimal TWO = BigDecimal.valueOf( 2L );

    protected final SqlValidator validator;

    @Getter
    protected final RexBuilder rexBuilder;
    protected final Snapshot snapshot;
    @Getter
    protected final AlgOptCluster cluster;
    @Setter
    private SubQueryConverter subQueryConverter;
    protected final List<AlgNode> leaves = new ArrayList<>();
    private final List<SqlDynamicParam> dynamicParamSqlNodes = new ArrayList<>();
    private final OperatorTable opTab;
    protected final AlgDataTypeFactory typeFactory;
    private final SqlNodeToRexConverter exprConverter;
    private int explainParamCount;
    public final Config config;
    private final AlgBuilder algBuilder;

    /**
     * Fields used in name resolution for correlated sub-queries.
     */
    private final Map<CorrelationId, DeferredLookup> mapCorrelToDeferred = new HashMap<>();

    /**
     * Stack of names of datasets requested by the <code>TABLE(SAMPLE(&lt;datasetName&gt;, &lt;query&gt;))</code> construct.
     */
    private final Deque<String> datasetStack = new ArrayDeque<>();

    /**
     * Mapping of non-correlated sub-queries that have been converted to their equivalent constants. Used to avoid
     * re-evaluating the sub-query if it's already been evaluated.

     */
    @Getter
    private final Map<SqlNode, RexNode> mapConvertedNonCorrSubqs = new HashMap<>();


    /* Creates a converter. */
    public SqlToAlgConverter( SqlValidator validator, Snapshot snapshot, AlgOptCluster cluster, SqlRexConvertletTable convertletTable, Config config ) {
        this.opTab =
                (validator == null)
                        ? SqlStdOperatorTable.instance()
                        : validator.getOperatorTable();
        this.validator = validator;
        this.snapshot = snapshot;
        this.subQueryConverter = new NoOpSubQueryConverter();
        this.rexBuilder = cluster.getRexBuilder();
        this.typeFactory = rexBuilder.getTypeFactory();
        this.cluster = Objects.requireNonNull( cluster );
        this.exprConverter = new SqlNodeToRexConverterImpl( convertletTable );
        this.explainParamCount = 0;
        this.config = new ConfigBuilder().config( config ).build();
        this.algBuilder = config.getAlgBuilderFactory().create( cluster, null );
    }


    /**
     * Returns the number of dynamic parameters encountered during translation; this must only be called after {@link #convertQuery}.
     *
     * @return number of dynamic parameters
     */
    public int getDynamicParamCount() {
        return dynamicParamSqlNodes.size();
    }


    /**
     * Returns the type inferred for a dynamic parameter.
     *
     * @param index 0-based index of dynamic parameter
     * @return inferred type, never null
     */
    public AlgDataType getDynamicParamType( int index ) {
        SqlNode sqlNode = dynamicParamSqlNodes.get( index );
        if ( sqlNode == null ) {
            throw Util.needToImplement( "dynamic param type inference" );
        }
        return validator.getValidatedNodeType( sqlNode );
    }


    /**
     * Returns the current count of the number of dynamic parameters in an EXPLAIN PLAN statement.
     *
     * @param increment if true, increment the count
     * @return the current count before the optional increment
     */
    public int getDynamicParamCountInExplain( boolean increment ) {
        int retVal = explainParamCount;
        if ( increment ) {
            ++explainParamCount;
        }
        return retVal;
    }


    /**
     * Adds to the current map of non-correlated converted sub-queries the elements from another map that contains non-correlated sub-queries that have been converted by another SqlToAlgConverter.
     *
     * @param alreadyConvertedNonCorrSubqs the other map
     */
    public void addConvertedNonCorrSubqs( Map<SqlNode, RexNode> alreadyConvertedNonCorrSubqs ) {
        mapConvertedNonCorrSubqs.putAll( alreadyConvertedNonCorrSubqs );
    }


    private void checkConvertedType( Node query, AlgNode result ) {
        if ( query.isA( Kind.DML ) ) {
            return;
        }
        // Verify that conversion from SQL to relational algebra did not perturb any type information.
        // (We can't do this if the SQL statement is something like an INSERT which has no
        // validator type information associated with its result, hence the namespace check above.)
        List<AlgDataTypeField> validatedFields = validator.getValidatedNodeType( query ).getFields(); // TODO DL read final
        final AlgDataType validatedRowType =
                validator.getTypeFactory().createStructType(
                        validatedFields.stream().map( AlgDataTypeField::getId ).toList(),
                        validatedFields.stream().map( AlgDataTypeField::getType ).toList(),
                        ValidatorUtil.uniquify( validatedFields.stream().map( AlgDataTypeField::getName ).toList(), false ) );

        final List<AlgDataTypeField> convertedFields = result.getTupleType().getFields().subList( 0, validatedFields.size() );
        final AlgDataType convertedRowType = validator.getTypeFactory().createStructType( convertedFields );

        if ( result.isCrossModel() ) {
            return;
        }

        if ( !AlgOptUtil.equal( "validated row type", validatedRowType, "converted row type", convertedRowType, Litmus.IGNORE ) ) {
            throw new AssertionError( "Conversion to relational algebra failed to "
                    + "preserve datatypes:\n"
                    + "validated type:\n"
                    + validatedRowType.getFullTypeString()
                    + "\nconverted type:\n"
                    + convertedRowType.getFullTypeString()
                    + "\nrel:\n"
                    + AlgOptUtil.toString( result ) );
        }
    }


    @Override
    public AlgNode flattenTypes( AlgNode rootAlg, boolean restructure ) {
        AlgStructuredTypeFlattener typeFlattener =
                new AlgStructuredTypeFlattener(
                        algBuilder,
                        rexBuilder,
                        cluster,
                        restructure );
        return typeFlattener.rewrite( rootAlg );
    }


    /**
     * If sub-query is correlated and decorrelation is enabled, performs decorrelation.
     *
     * @param query Query
     * @param rootAlg Root relational expression
     * @return New root relational expression after decorrelation
     */
    @Override
    public AlgNode decorrelate( Node query, AlgNode rootAlg ) {
        if ( !enableDecorrelation() ) {
            return rootAlg;
        }
        final AlgNode result = decorrelateQuery( rootAlg );
        if ( result != rootAlg ) {
            checkConvertedType( query, result );
        }
        return result;
    }


    /**
     * Walks over a tree of relational expressions, replacing each {@link AlgNode} with a 'slimmed down' relational expression that projects only the fields required by its consumer.
     * <p>
     * This may make things easier for the optimizer, by removing crud that would expand the search space, but is difficult for the optimizer itself to do it, because optimizer rules must preserve the number and type of
     * fields. Hence, this transform that operates on the entire tree, similar to the {@link AlgStructuredTypeFlattener type-flattening transform}.
     * <p>
     * Currently, this functionality is disabled in farrago/luciddb; the default implementation of this method does nothing.
     *
     * @param ordered Whether the relational expression must produce results in a particular order (typically because it has an ORDER BY at top level)
     * @param rootAlg Relational expression that is at the root of the tree
     * @return Trimmed relational expression
     */
    @Override
    public AlgNode trimUnusedFields( boolean ordered, AlgNode rootAlg ) {
        // Trim fields that are not used by their consumer.
        if ( isTrimUnusedFields() ) {
            final AlgFieldTrimmer trimmer = newFieldTrimmer();
            final List<AlgCollation> collations = rootAlg.getTraitSet().getTraits( AlgCollationTraitDef.INSTANCE );
            rootAlg = trimmer.trim( rootAlg );
            if ( !ordered
                    && collations != null
                    && !collations.isEmpty()
                    && !collations.equals( ImmutableList.of( AlgCollations.EMPTY ) ) ) {
                final AlgTraitSet traitSet = rootAlg.getTraitSet().replace( AlgCollationTraitDef.INSTANCE, collations );
                rootAlg = rootAlg.copy( traitSet, rootAlg.getInputs() );
            }
            if ( SQL2REL_LOGGER.isDebugEnabled() ) {
                SQL2REL_LOGGER.debug(
                        AlgOptUtil.dumpPlan(
                                "Plan after trimming unused fields",
                                rootAlg,
                                ExplainFormat.TEXT,
                                ExplainLevel.EXPPLAN_ATTRIBUTES ) );
            }
        }
        return rootAlg;
    }


    /**
     * Creates a AlgFieldTrimmer.
     *
     * @return Field trimmer
     */
    @Override
    public AlgFieldTrimmer newFieldTrimmer() {
        return new AlgFieldTrimmer( validator, algBuilder );
    }


    /**
     * Converts an unvalidated query's parse tree into a relational expression.
     *
     * @param rawQuery Query to convert
     * @param needsValidation Whether to validate the query before converting; <code>false</code> if the query has already been validated.
     * @param top Whether the query is top-level, say if its result will become a JDBC result set; <code>false</code> if the query will be part of a view.
     */
    @Override
    public AlgRoot convertQuery( Node rawQuery, final boolean needsValidation, final boolean top ) {
        SqlNode query = (SqlNode) rawQuery;
        if ( needsValidation ) {
            query = validator.validateSql( query );
        }

        AlgMetadataQuery.THREAD_PROVIDERS.set( JaninoRelMetadataProvider.of( cluster.getMetadataProvider() ) );
        AlgNode result = convertQueryRecursive( query, top, null ).alg;
        if ( top ) {
            if ( isStream( query ) ) {
                result = new LogicalDelta( cluster, result.getTraitSet(), result );
            }
        }
        AlgCollation collation = AlgCollations.EMPTY;

        if ( !query.isA( Kind.DML ) ) {
            if ( isOrdered( query ) ) {
                collation = requiredCollation( result );
            }
        }
        checkConvertedType( query, result );

        if ( SQL2REL_LOGGER.isDebugEnabled() ) {
            SQL2REL_LOGGER.debug(
                    AlgOptUtil.dumpPlan(
                            "Plan after converting SqlNode to AlgNode",
                            result,
                            ExplainFormat.TEXT,
                            ExplainLevel.EXPPLAN_ATTRIBUTES ) );
        }

        final AlgDataType validatedRowType = validator.getValidatedNodeType( query );

        return AlgRoot.of( result, validatedRowType, query.getKind() ).withCollation( collation );
    }


    private static boolean isStream( SqlNode query ) {
        return query instanceof SqlSelect && ((SqlSelect) query).isKeywordPresent( SqlSelectKeyword.STREAM );
    }


    public static boolean isOrdered( SqlNode query ) {
        return switch ( query.getKind() ) {
            case SELECT -> ((SqlSelect) query).getOrderList() != null && ((SqlSelect) query).getOrderList().size() > 0;
            case WITH -> isOrdered( ((SqlWith) query).body );
            case ORDER_BY -> ((SqlOrderBy) query).orderList.size() > 0;
            default -> false;
        };
    }


    private AlgCollation requiredCollation( AlgNode r ) {
        if ( r instanceof Sort ) {
            return ((Sort) r).collation;
        }
        if ( r instanceof Project ) {
            return requiredCollation( ((Project) r).getInput() );
        }
        if ( r instanceof Delta ) {
            return requiredCollation( ((Delta) r).getInput() );
        }
        if ( r instanceof LogicalRelViewScan ) {
            return ((LogicalRelViewScan) r).getAlgCollation();
        }
        throw new AssertionError();
    }


    /**
     * Converts a SELECT statement's parse tree into a relational expression.
     */
    public AlgNode convertSelect( SqlSelect select, boolean top ) {
        final SqlValidatorScope selectScope = validator.getWhereScope( select );
        final Blackboard bb = createBlackboard( selectScope, null, top );
        convertSelectImpl( bb, select );
        return bb.root;
    }


    /**
     * Factory method for creating translation workspace.
     */
    protected Blackboard createBlackboard( SqlValidatorScope scope, Map<String, RexNode> nameToNodeMap, boolean top ) {
        return new Blackboard( scope, nameToNodeMap, top );
    }


    /**
     * Implementation of {@link #convertSelect(SqlSelect, boolean)}; derived class may override.
     */
    protected void convertSelectImpl( final Blackboard bb, SqlSelect select ) {
        convertFrom( bb, select.getSqlFrom() );
        convertWhere( bb, select.getWhere() );

        final List<SqlNode> orderExprList = new ArrayList<>();
        final List<AlgFieldCollation> collationList = new ArrayList<>();
        gatherOrderExprs( bb, select, select.getOrderList(), orderExprList, collationList );
        final AlgCollation collation = cluster.traitSet().canonize( AlgCollations.of( collationList ) );

        if ( validator.isAggregate( select ) ) {
            convertAgg( bb, select, orderExprList );
        } else {
            convertSelectList( bb, select, orderExprList );
        }

        if ( select.isDistinct() ) {
            distinctify( bb, true );
        }
        convertOrder( select, bb, collation, orderExprList, select.getOffset(), select.getFetch() );
        bb.setRoot( bb.root, true );
    }


    /**
     * Having translated 'SELECT ... FROM ... [GROUP BY ...] [HAVING ...]', adds a relational expression to make the results unique.
     * <p>
     * If the SELECT clause contains duplicate expressions, adds {@link LogicalProject}s so that we are grouping on the minimal set of keys. The performance gain isn't huge, but
     * it is difficult to detect these duplicate expressions later.
     *
     * @param bb Blackboard
     * @param checkForDupExprs Check for duplicate expressions
     */
    private void distinctify( Blackboard bb, boolean checkForDupExprs ) {
        // Look for duplicate expressions in the project.// Say we have 'select x, y, x, z'.
        // Then dups will be {[2, 0]} and oldToNew will be {[0, 0], [1, 1], [2, 0], [3, 2]}
        AlgNode alg = bb.root;
        if ( checkForDupExprs && (alg instanceof LogicalProject project) ) {
            final List<RexNode> projectExprs = project.getProjects();
            final List<Integer> origins = new ArrayList<>();
            int dupCount = 0;
            for ( int i = 0; i < projectExprs.size(); i++ ) {
                int x = projectExprs.indexOf( projectExprs.get( i ) );
                if ( x >= 0 && x < i ) {
                    origins.add( x );
                    ++dupCount;
                } else {
                    origins.add( i );
                }
            }
            if ( dupCount == 0 ) {
                distinctify( bb, false );
                return;
            }

            final Map<Integer, Integer> squished = new HashMap<>();
            final List<AlgDataTypeField> fields = alg.getTupleType().getFields();
            final List<Pair<RexNode, String>> newProjects = new ArrayList<>();
            for ( int i = 0; i < fields.size(); i++ ) {
                if ( origins.get( i ) == i ) {
                    squished.put( i, newProjects.size() );
                    newProjects.add( RexIndexRef.of2( i, fields ) );
                }
            }
            alg = LogicalProject.create( alg, Pair.left( newProjects ), Pair.right( newProjects ) );
            bb.root = alg;
            distinctify( bb, false );
            alg = bb.root;

            // Create the expressions to reverse the mapping.
            // Project($0, $1, $0, $2).
            final List<Pair<RexNode, String>> undoProjects = new ArrayList<>();
            for ( int i = 0; i < fields.size(); i++ ) {
                final int origin = origins.get( i );
                AlgDataTypeField field = fields.get( i );
                undoProjects.add(
                        Pair.of(
                                new RexIndexRef( squished.get( origin ), field.getType() ),
                                field.getName() ) );
            }

            alg = LogicalProject.create( alg, Pair.left( undoProjects ), Pair.right( undoProjects ) );
            bb.setRoot( alg, false );

            return;
        }

        // Usual case: all of the expressions in the SELECT clause are different.
        final ImmutableBitSet groupSet = ImmutableBitSet.range( alg.getTupleType().getFieldCount() );
        alg = createAggregate( bb, groupSet, ImmutableList.of( groupSet ), ImmutableList.of() );

        bb.setRoot( alg, false );
    }


    /**
     * Converts a query's ORDER BY clause, if any.
     * <p>
     * Ignores the ORDER BY clause if the query is not top-level and FETCH or OFFSET are not present.
     *
     * @param select Query
     * @param bb Blackboard
     * @param collation Collation list
     * @param orderExprList Method populates this list with orderBy expressions not present in selectList
     * @param offset Expression for number of rows to discard before returning first row
     * @param fetch Expression for number of rows to fetch
     */
    protected void convertOrder( SqlSelect select, Blackboard bb, AlgCollation collation, List<SqlNode> orderExprList, SqlNode offset, SqlNode fetch ) {
        if ( !bb.top
                || select.getOrderList() == null
                || select.getOrderList().getList().isEmpty() ) {
            assert !bb.top || collation.getFieldCollations().isEmpty();
            if ( (offset == null
                    || (offset instanceof SqlLiteral
                    && ((SqlLiteral) offset).bigDecimalValue().equals( BigDecimal.ZERO )))
                    && fetch == null ) {
                return;
            }
        }

        // Create a sorter using the previously constructed collations.
        bb.setRoot(
                LogicalSort.create(
                        bb.root,
                        collation,
                        offset == null ? null : convertExpression( offset ),
                        fetch == null ? null : convertExpression( fetch ) ),
                false );

        // If extra expressions were added to the project list for sorting, add another project to remove them. But make the collation empty, because we can't represent the real collation.
        //
        // If it is the top node, use the real collation, but don't trim fields.
        if ( !orderExprList.isEmpty() ) {//&& !bb.top ) {
            final List<RexNode> exprs = new ArrayList<>();
            final AlgDataType rowType = bb.root.getTupleType();
            final int fieldCount = rowType.getFieldCount() - orderExprList.size();
            for ( int i = 0; i < fieldCount; i++ ) {
                exprs.add( rexBuilder.makeInputRef( bb.root, i ) );
            }
            bb.setRoot(
                    LogicalProject.create( bb.root, exprs, rowType.getFieldNames().subList( 0, fieldCount ) ),
                    false );
        }
    }


    private boolean expressionsDifferent( List<SqlNode> list, List<SqlNode> others ) {
        for ( SqlNode node : list ) {
            if ( !others.contains( node ) ) {
                return true;
            }
        }

        for ( SqlNode node : others ) {
            if ( !list.contains( node ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Returns whether a given node contains a {@link SqlInOperator}.
     *
     * @param node a RexNode tree
     */
    private static boolean containsInOperator( SqlNode node ) {
        try {
            NodeVisitor<Void> visitor =
                    new BasicNodeVisitor<Void>() {
                        @Override
                        public Void visit( Call call ) {
                            if ( call.getOperator() instanceof SqlInOperator ) {
                                throw new Util.FoundOne( call );
                            }
                            return super.visit( call );
                        }
                    };
            node.accept( visitor );
            return false;
        } catch ( Util.FoundOne e ) {
            Util.swallow( e, null );
            return true;
        }
    }


    /**
     * Push down all the NOT logical operators into any IN/NOT IN operators.
     *
     * @param scope Scope where {@code sqlNode} occurs
     * @param sqlNode the root node from which to look for NOT operators
     * @return the transformed SqlNode representation with NOT pushed down.
     */
    private static SqlNode pushDownNotForIn( SqlValidatorScope scope, SqlNode sqlNode ) {
        if ( !(sqlNode instanceof SqlCall sqlCall) || !containsInOperator( sqlNode ) ) {
            return sqlNode;
        }
        switch ( sqlCall.getKind() ) {
            case AND:
            case OR:
                final List<SqlNode> operands = new ArrayList<>();
                for ( Node operand : sqlCall.getOperandList() ) {
                    operands.add( pushDownNotForIn( scope, (SqlNode) operand ) );
                }
                final SqlCall newCall = (SqlCall) sqlCall.getOperator().createCall( sqlCall.getPos(), operands );
                return reg( scope, newCall );

            case NOT:
                assert sqlCall.operand( 0 ) instanceof SqlCall;
                final SqlCall call = sqlCall.operand( 0 );
                switch ( sqlCall.operand( 0 ).getKind() ) {
                    case CASE:
                        final SqlCase caseNode = (SqlCase) call;
                        final SqlNodeList thenOperands = new SqlNodeList( ParserPos.ZERO );

                        for ( SqlNode thenOperand : caseNode.getThenOperands().getSqlList() ) {
                            final SqlCall not = (SqlCall) OperatorRegistry.get( OperatorName.NOT ).createCall( ParserPos.ZERO, thenOperand );
                            thenOperands.add( pushDownNotForIn( scope, reg( scope, not ) ) );
                        }
                        SqlNode elseOperand = caseNode.getElseOperand();
                        if ( !SqlUtil.isNull( elseOperand ) ) {
                            // "not(unknown)" is "unknown", so no need to simplify
                            final SqlCall not = (SqlCall) OperatorRegistry.get( OperatorName.NOT ).createCall( ParserPos.ZERO, elseOperand );
                            elseOperand = pushDownNotForIn( scope, reg( scope, not ) );
                        }

                        return reg(
                                scope,
                                (SqlNode) OperatorRegistry.get( OperatorName.CASE ).createCall(
                                        ParserPos.ZERO,
                                        caseNode.getValueOperand(),
                                        caseNode.getWhenOperands(),
                                        thenOperands,
                                        elseOperand ) );

                    case AND:
                        final List<SqlNode> orOperands = new ArrayList<>();
                        for ( Node operand : call.getOperandList() ) {
                            orOperands.add(
                                    pushDownNotForIn(
                                            scope,
                                            reg( scope, (SqlNode) OperatorRegistry.get( OperatorName.NOT ).createCall( ParserPos.ZERO, operand ) ) ) );
                        }
                        return reg( scope, (SqlNode) OperatorRegistry.get( OperatorName.OR ).createCall( ParserPos.ZERO, orOperands ) );

                    case OR:
                        final List<Node> andOperands = new ArrayList<>();
                        for ( Node operand : call.getOperandList() ) {
                            andOperands.add(
                                    pushDownNotForIn(
                                            scope,
                                            reg( scope, (SqlNode) OperatorRegistry.get( OperatorName.NOT ).createCall( ParserPos.ZERO, operand ) ) ) );
                        }
                        return reg( scope, (SqlNode) OperatorRegistry.get( OperatorName.AND ).createCall( ParserPos.ZERO, andOperands ) );

                    case NOT:
                        assert call.operandCount() == 1;
                        return pushDownNotForIn( scope, call.operand( 0 ) );

                    case NOT_IN:
                        return reg( scope, (SqlNode) OperatorRegistry.get( OperatorName.IN ).createCall( ParserPos.ZERO, call.getOperandList() ) );

                    case IN:
                        return reg( scope, (SqlNode) OperatorRegistry.get( OperatorName.NOT_IN ).createCall( ParserPos.ZERO, call.getOperandList() ) );
                }
        }
        return sqlNode;
    }


    /**
     * Registers with the validator a {@link SqlNode} that has been created during the Sql-to-Rel process.
     */
    private static SqlNode reg( SqlValidatorScope scope, SqlNode e ) {
        scope.getValidator().deriveType( scope, e );
        return e;
    }


    /**
     * Converts a WHERE clause.
     *
     * @param bb Blackboard
     * @param where WHERE clause, may be null
     */
    private void convertWhere( final Blackboard bb, final SqlNode where ) {
        if ( where == null ) {
            return;
        }
        SqlNode newWhere = pushDownNotForIn( bb.scope, where );
        replaceSubQueries( bb, newWhere, AlgOptUtil.Logic.UNKNOWN_AS_FALSE );
        final RexNode convertedWhere = bb.convertExpression( newWhere );
        final RexNode convertedWhere2 = RexUtil.removeNullabilityCast( typeFactory, convertedWhere );

        // only allocate filter if the condition is not TRUE
        if ( convertedWhere2.isAlwaysTrue() ) {
            return;
        }

        final FilterFactory factory = AlgFactories.DEFAULT_FILTER_FACTORY;
        final AlgNode filter = factory.createFilter( bb.root, convertedWhere2 );
        final AlgNode r;
        final CorrelationUse p = getCorrelationUse( bb, filter );
        if ( p != null ) {
            assert p.r instanceof Filter;
            Filter f = (Filter) p.r;
            r = LogicalFilter.create( f.getInput(), f.getCondition(), ImmutableSet.of( p.id ) );
        } else {
            r = filter;
        }

        bb.setRoot( r, false );
    }


    private void replaceSubQueries( final Blackboard bb, final SqlNode expr, AlgOptUtil.Logic logic ) {
        findSubQueries( bb, expr, logic, false );
        for ( SubQuery node : bb.subQueryList ) {
            substituteSubQuery( bb, node );
        }
    }


    private void substituteSubQuery( Blackboard bb, SubQuery subQuery ) {
        final RexNode expr = subQuery.expr;
        if ( expr != null ) {
            // Already done.
            return;
        }

        final SqlBasicCall call;
        final AlgNode alg;
        final SqlNode query;
        final AlgOptUtil.Exists converted;
        switch ( subQuery.node.getKind() ) {
            case CURSOR:
                convertCursor( bb, subQuery );
                return;

            case MULTISET_QUERY_CONSTRUCTOR:
            case MULTISET_VALUE_CONSTRUCTOR:
            case ARRAY_QUERY_CONSTRUCTOR:
                alg = convertMultisets( ImmutableList.of( subQuery.node ), bb );
                subQuery.expr = bb.register( alg, JoinAlgType.INNER );
                return;

            case IN:
            case NOT_IN:
            case SOME:
            case ALL:
                call = (SqlBasicCall) subQuery.node;
                query = call.operand( 1 );
                if ( !config.isExpand() && !(query instanceof SqlNodeList) ) {
                    return;
                }
                final SqlNode leftKeyNode = call.operand( 0 );

                final List<RexNode> leftKeys;
                if ( Objects.requireNonNull( leftKeyNode.getKind() ) == Kind.ROW ) {
                    leftKeys = new ArrayList<>();
                    for ( Node sqlExpr : ((SqlBasicCall) leftKeyNode).getOperandList() ) {
                        leftKeys.add( bb.convertExpression( sqlExpr ) );
                    }
                } else {
                    leftKeys = ImmutableList.of( bb.convertExpression( leftKeyNode ) );
                }

                if ( query instanceof SqlNodeList valueList ) {
                    if ( !containsNullLiteral( valueList ) && valueList.size() < config.getInSubQueryThreshold() ) {
                        // We're under the threshold, so convert to OR.
                        subQuery.expr = convertInToOr( bb, leftKeys, valueList, (SqlInOperator) call.getOperator() );
                        return;
                    }

                    // Otherwise, let convertExists translate values list into an inline table for the reference to Q below.
                }

                // Project out the search columns from the left side

                // Q1:
                // "select from emp where emp.deptno in (select col1 from T)"
                //
                // is converted to
                //
                // "select from
                //   emp inner join (select distinct col1 from T)) q
                //   on emp.deptno = q.col1
                //
                // Q2:
                // "select from emp where emp.deptno not in (Q)"
                //
                // is converted to
                //
                // "select from
                //   emp left outer join (select distinct col1, TRUE from T) q
                //   on emp.deptno = q.col1
                //   where emp.deptno <> null
                //         and q.indicator <> TRUE"
                //
                final AlgDataType targetRowType =
                        PolyTypeUtil.promoteToRowType(
                                typeFactory,
                                validator.getValidatedNodeType( leftKeyNode ),
                                null );
                final boolean notIn = call.getOperator().getKind() == Kind.NOT_IN;
                converted = convertExists(
                        query,
                        AlgOptUtil.SubQueryType.IN,
                        subQuery.logic,
                        notIn,
                        targetRowType );
                if ( converted.indicator ) {
                    // Generate
                    //    emp CROSS JOIN (SELECT COUNT(*) AS c,
                    //                       COUNT(deptno) AS ck FROM dept)
                    final AlgDataType longType = typeFactory.createPolyType( PolyType.BIGINT );
                    final AlgNode seek = converted.r.getInput( 0 ); // fragile
                    final int keyCount = leftKeys.size();
                    final List<Integer> args = IntStream.range( 0, keyCount ).boxed().toList();
                    LogicalAggregate aggregate =
                            LogicalAggregate.create( seek, ImmutableBitSet.of(), null,
                                    ImmutableList.of(
                                            AggregateCall.create(
                                                    OperatorRegistry.getAgg( OperatorName.COUNT ),
                                                    false,
                                                    false,
                                                    ImmutableList.of(),
                                                    -1,
                                                    AlgCollations.EMPTY,
                                                    longType,
                                                    null ),
                                            AggregateCall.create(
                                                    OperatorRegistry.getAgg( OperatorName.COUNT ),
                                                    false,
                                                    false,
                                                    args,
                                                    -1,
                                                    AlgCollations.EMPTY,
                                                    longType,
                                                    null ) ) );
                    LogicalJoin join =
                            LogicalJoin.create(
                                    bb.root,
                                    aggregate,
                                    rexBuilder.makeLiteral( true ),
                                    ImmutableSet.of(),
                                    JoinAlgType.INNER );
                    bb.setRoot( join, false );
                }
                final RexNode rex =
                        bb.register(
                                converted.r,
                                converted.outerJoin
                                        ? JoinAlgType.LEFT
                                        : JoinAlgType.INNER,
                                leftKeys );

                AlgOptUtil.Logic logic = subQuery.logic;
                switch ( logic ) {
                    case TRUE_FALSE_UNKNOWN:
                    case UNKNOWN_AS_TRUE:
                        if ( !converted.indicator ) {
                            logic = AlgOptUtil.Logic.TRUE_FALSE;
                        }
                }
                subQuery.expr = translateIn( logic, bb.root, rex );
                if ( notIn ) {
                    subQuery.expr = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.NOT ), subQuery.expr );
                }
                return;

            case EXISTS:
                // "select from emp where exists (select a from T)"
                //
                // is converted to the following if the sub-query is correlated:
                //
                // "select from emp left outer join (select AGG_TRUE() as indicator from T group by corr_var) q where q.indicator is true"
                //
                // If there is no correlation, the expression is replaced with a boolean indicating whether the sub-query returned 0 or >= 1 row.
                call = (SqlBasicCall) subQuery.node;
                query = call.operand( 0 );
                if ( !config.isExpand() ) {
                    return;
                }
                converted = convertExists(
                        query,
                        AlgOptUtil.SubQueryType.EXISTS,
                        subQuery.logic,
                        true,
                        null );
                assert !converted.indicator;
                if ( convertNonCorrelatedSubQuery( subQuery, bb, converted.r, true ) ) {
                    return;
                }
                subQuery.expr = bb.register( converted.r, JoinAlgType.LEFT );
                return;

            case SCALAR_QUERY:
                // Convert the sub-query.  If it's non-correlated, convert it to a constant expression.
                if ( !config.isExpand() ) {
                    return;
                }
                call = (SqlBasicCall) subQuery.node;
                query = call.operand( 0 );
                converted = convertExists(
                        query,
                        AlgOptUtil.SubQueryType.SCALAR,
                        subQuery.logic,
                        true,
                        null );
                assert !converted.indicator;
                if ( convertNonCorrelatedSubQuery( subQuery, bb, converted.r, false ) ) {
                    return;
                }
                alg = convertToSingleValueSubq( query, converted.r );
                subQuery.expr = bb.register( alg, JoinAlgType.LEFT );
                return;

            case SELECT:
                // This is used when converting multiset queries:
                //
                // select * from unnest(select multiset[deptno] from emps);
                //
                converted = convertExists(
                        subQuery.node,
                        AlgOptUtil.SubQueryType.SCALAR,
                        subQuery.logic,
                        true,
                        null );
                assert !converted.indicator;
                subQuery.expr = bb.register( converted.r, JoinAlgType.LEFT );
                return;

            default:
                throw new AssertionError( "unexpected kind of sub-query: " + subQuery.node );
        }
    }


    private RexNode translateIn( AlgOptUtil.Logic logic, AlgNode root, final RexNode rex ) {
        switch ( logic ) {
            case TRUE:
                return rexBuilder.makeLiteral( true );

            case TRUE_FALSE:
            case UNKNOWN_AS_FALSE:
                assert rex instanceof RexRangeRef;
                final int fieldCount = rex.getType().getFieldCount();
                RexNode rexNode = rexBuilder.makeFieldAccess( rex, fieldCount - 1 );
                rexNode = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_TRUE ), rexNode );

                // Then append the IS NOT NULL(leftKeysForIn).
                //
                // RexRangeRef contains the following fields:
                //   leftKeysForIn,
                //   rightKeysForIn (the original sub-query select list),
                //   nullIndicator
                //
                // The first two lists contain the same number of fields.
                final int k = (fieldCount - 1) / 2;
                for ( int i = 0; i < k; i++ ) {
                    rexNode =
                            rexBuilder.makeCall(
                                    OperatorRegistry.get( OperatorName.AND ),
                                    rexNode,
                                    rexBuilder.makeCall(
                                            OperatorRegistry.get( OperatorName.IS_NOT_NULL ),
                                            rexBuilder.makeFieldAccess( rex, i ) ) );
                }
                return rexNode;

            case TRUE_FALSE_UNKNOWN:
            case UNKNOWN_AS_TRUE:
                // select e.deptno,
                //   case
                //   when ct.c = 0 then false
                //   when dt.i is not null then true
                //   when e.deptno is null then null
                //   when ct.ck < ct.c then null
                //   else false
                //   end
                // from e
                // cross join (select count(*) as c, count(deptno) as ck from v) as ct
                // left join (select distinct deptno, true as i from v) as dt
                //   on e.deptno = dt.deptno
                final Join join = (Join) root;
                final Project left = (Project) join.getLeft();
                final AlgNode leftLeft = ((Join) left.getInput()).getLeft();
                final int leftLeftCount = leftLeft.getTupleType().getFieldCount();
                final AlgDataType longType = typeFactory.createPolyType( PolyType.BIGINT );
                final RexNode cRef = rexBuilder.makeInputRef( root, leftLeftCount );
                final RexNode ckRef = rexBuilder.makeInputRef( root, leftLeftCount + 1 );
                final RexNode iRef = rexBuilder.makeInputRef( root, root.getTupleType().getFieldCount() - 1 );

                final RexLiteral zero = rexBuilder.makeExactLiteral( BigDecimal.ZERO, longType );
                final RexLiteral trueLiteral = rexBuilder.makeLiteral( true );
                final RexLiteral falseLiteral = rexBuilder.makeLiteral( false );
                final RexNode unknownLiteral = rexBuilder.makeNullLiteral( trueLiteral.getType() );

                final ImmutableList.Builder<RexNode> args = ImmutableList.builder();
                args.add(
                        rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), cRef, zero ),
                        falseLiteral,
                        rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), iRef ),
                        trueLiteral );
                final JoinInfo joinInfo = join.analyzeCondition();
                for ( int leftKey : joinInfo.leftKeys ) {
                    final RexNode kRef = rexBuilder.makeInputRef( root, leftKey );
                    args.add( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), kRef ), unknownLiteral );
                }
                args.add( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.LESS_THAN ), ckRef, cRef ), unknownLiteral, falseLiteral );

                return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.CASE ), args.build() );

            default:
                throw new AssertionError( logic );
        }
    }


    private static boolean containsNullLiteral( SqlNodeList valueList ) {
        for ( SqlNode node : valueList.getSqlList() ) {
            if ( node instanceof SqlLiteral lit ) {
                if ( lit.getValue() == null ) {
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * Determines if a sub-query is non-correlated and if so, converts it to a constant.
     *
     * @param subQuery the call that references the sub-query
     * @param bb blackboard used to convert the sub-query
     * @param converted {@link AlgNode} tree corresponding to the sub-query
     * @param isExists true if the sub-query is part of an EXISTS expression
     * @return Whether the sub-query can be converted to a constant
     */
    private boolean convertNonCorrelatedSubQuery( SubQuery subQuery, Blackboard bb, AlgNode converted, boolean isExists ) {
        SqlCall call = (SqlBasicCall) subQuery.node;
        if ( subQueryConverter.canConvertSubQuery() && isSubQueryNonCorrelated( converted, bb ) ) {
            // First check if the sub-query has already been converted because it's a nested sub-query.  If so, don't re-evaluate it again.
            RexNode constExpr = mapConvertedNonCorrSubqs.get( call );
            if ( constExpr == null ) {
                constExpr = subQueryConverter.convertSubQuery( call, this, isExists, config.isExplain() );
            }
            if ( constExpr != null ) {
                subQuery.expr = constExpr;
                mapConvertedNonCorrSubqs.put( call, constExpr );
                return true;
            }
        }
        return false;
    }


    /**
     * Converts the {@link AlgNode} tree for a select statement to a select that produces a single value.
     *
     * @param query the query
     * @param plan the original {@link AlgNode} tree corresponding to the statement
     * @return the converted {@link AlgNode} tree
     */
    public AlgNode convertToSingleValueSubq( SqlNode query, AlgNode plan ) {
        // Check whether query is guaranteed to produce a single value.
        if ( query instanceof SqlSelect select ) {
            SqlNodeList selectList = select.getSqlSelectList();
            SqlNodeList groupList = select.getGroup();

            if ( (selectList.size() == 1) && ((groupList == null) || (groupList.size() == 0)) ) {
                SqlNode selectExpr = selectList.getSqlList().get( 0 );
                if ( selectExpr instanceof SqlCall selectExprCall ) {
                    if ( Util.isSingleValue( selectExprCall ) ) {
                        return plan;
                    }
                }

                // If there is a limit with 0 or 1, it is ensured to produce a single value
                if ( select.getFetch() != null && select.getFetch() instanceof SqlNumericLiteral limitNum ) {
                    if ( limitNum.getValue().asNumber().intValue() < 2 ) {
                        return plan;
                    }
                }
            }
        } else if ( query instanceof SqlCall exprCall ) {
            // If the query is (values ...), it is necessary to look into the operands to determine whether SingleValueAgg is necessary
            if ( exprCall.getOperator() instanceof SqlValuesOperator && Util.isSingleValue( exprCall ) ) {
                return plan;
            }
        }

        // If not, project SingleValueAgg
        return AlgOptUtil.createSingleValueAggAlg( cluster, plan );
    }


    /**
     * Converts "x IN (1, 2, ...)" to "x=1 OR x=2 OR ...".
     *
     * @param leftKeys LHS
     * @param valuesList RHS
     * @param op The operator (IN, NOT IN, &gt; SOME, ...)
     * @return converted expression
     */
    private RexNode convertInToOr( final Blackboard bb, final List<RexNode> leftKeys, SqlNodeList valuesList, SqlInOperator op ) {
        final List<RexNode> comparisons = new ArrayList<>();
        for ( SqlNode rightVals : valuesList.getSqlList() ) {
            RexNode rexComparison;
            final SqlOperator comparisonOp;
            if ( op instanceof SqlQuantifyOperator ) {
                comparisonOp = (SqlOperator) AlgOptUtil.op( ((SqlQuantifyOperator) op).comparisonKind, OperatorRegistry.get( OperatorName.EQUALS ) );
            } else {
                comparisonOp = (SqlOperator) OperatorRegistry.get( OperatorName.EQUALS );
            }
            if ( leftKeys.size() == 1 ) {
                rexComparison =
                        rexBuilder.makeCall(
                                comparisonOp,
                                leftKeys.get( 0 ),
                                ensureSqlType(
                                        leftKeys.get( 0 ).getType(),
                                        bb.convertExpression( rightVals ) ) );
            } else {
                assert rightVals instanceof SqlCall;
                final SqlBasicCall call = (SqlBasicCall) rightVals;
                assert (call.getOperator() instanceof SqlRowOperator) && call.operandCount() == leftKeys.size();
                rexComparison =
                        RexUtil.composeConjunction(
                                rexBuilder,
                                Pair.zip(
                                        leftKeys,
                                        call.getOperandList() ).stream().map( pair -> rexBuilder.makeCall(
                                        comparisonOp,
                                        pair.left,
                                        ensureSqlType(
                                                pair.left.getType(),
                                                bb.convertExpression( pair.right ) ) ) ).collect( Collectors.toList() ) );
            }
            comparisons.add( rexComparison );
        }

        return switch ( op.kind ) {
            case ALL -> RexUtil.composeConjunction(
                    rexBuilder,
                    comparisons,
                    true );
            case NOT_IN -> rexBuilder.makeCall(
                    OperatorRegistry.get( OperatorName.NOT ),
                    RexUtil.composeDisjunction( rexBuilder, comparisons, true ) );
            case IN, SOME -> RexUtil.composeDisjunction( rexBuilder, comparisons, true );
            default -> throw new AssertionError();
        };
    }


    /**
     * Ensures that an expression has a given {@link PolyType}, applying a cast if necessary. If the expression already has the right type family, returns the expression unchanged.
     */
    private RexNode ensureSqlType( AlgDataType type, RexNode node ) {
        if ( type.getPolyType() == node.getType().getPolyType()
                || (type.getPolyType() == PolyType.VARCHAR && node.getType().getPolyType() == PolyType.CHAR) ) {
            return node;
        }
        return rexBuilder.ensureType( type, node, true );
    }


    /**
     * Converts an EXISTS or IN predicate into a join. For EXISTS, the sub-query produces an indicator variable,
     * and the result is a relational expression which outer joins that indicator to the original query. After performing
     * the outer join, the condition will be TRUE if the EXISTS condition holds, NULL otherwise.
     *
     * @param seek A query, for example 'select * from emp' or 'values (1,2,3)' or '('Foo', 34)'.
     * @param subQueryType Whether sub-query is IN, EXISTS or scalar
     * @param logic Whether the answer needs to be in full 3-valued logic (TRUE, FALSE, UNKNOWN) will be required,
     * or whether we can accept an approximation (say representing UNKNOWN as FALSE)
     * @param notIn Whether the operation is NOT IN
     * @return join expression
     */
    private AlgOptUtil.Exists convertExists( SqlNode seek, AlgOptUtil.SubQueryType subQueryType, AlgOptUtil.Logic logic, boolean notIn, AlgDataType targetDataType ) {
        final SqlValidatorScope seekScope =
                (seek instanceof SqlSelect)
                        ? validator.getSelectScope( (SqlSelect) seek )
                        : null;
        final Blackboard seekBb = createBlackboard( seekScope, null, false );
        AlgNode seekAlg = convertQueryOrInList( seekBb, seek, targetDataType );

        return AlgOptUtil.createExistsPlan( seekAlg, subQueryType, logic, notIn, algBuilder );
    }


    private AlgNode convertQueryOrInList( Blackboard bb, SqlNode seek, AlgDataType targetRowType ) {
        // NOTE: Once we start accepting single-row queries as row constructors, there will be an ambiguity here for a case like X IN ((SELECT Y FROM Z)).  The SQL standard resolves the ambiguity by saying that a lone
        // select should be interpreted as a table expression, not a row expression.  The semantic difference is that a table expression can return multiple rows.
        if ( seek instanceof SqlNodeList ) {
            return convertRowValues(
                    bb,
                    seek,
                    ((SqlNodeList) seek).getSqlList(),
                    false,
                    targetRowType );
        } else {
            return convertQueryRecursive( seek, false, null ).project();
        }
    }


    private AlgNode convertRowValues( Blackboard bb, SqlNode rowList, Collection<SqlNode> rows, boolean allowLiteralsOnly, AlgDataType targetRowType ) {
        // NOTE jvs 30-Apr-2006: We combine all rows consisting entirely of literals into a single LogicalValues; this gives the optimizer a smaller input tree.  For everything else (computed expressions, row
        // sub-queries), we union each row in as a projection on top of a LogicalOneRow.

        final ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
        final AlgDataType rowType;
        if ( targetRowType != null ) {
            rowType = targetRowType;
        } else {
            rowType = PolyTypeUtil.promoteToRowType( typeFactory, validator.getValidatedNodeType( rowList ), null );
        }

        final List<AlgNode> unionInputs = new ArrayList<>();
        for ( SqlNode node : rows ) {
            SqlBasicCall call;
            if ( isRowConstructor( node ) ) {
                call = (SqlBasicCall) node;
                ImmutableList.Builder<RexLiteral> tuple = ImmutableList.builder();
                for ( Ord<SqlNode> operand : Ord.zip( call.operands ) ) {
                    RexLiteral rexLiteral = convertLiteralInValuesList( operand.e, bb, rowType, operand.i );
                    if ( (rexLiteral == null) && allowLiteralsOnly ) {
                        return null;
                    }
                    if ( (rexLiteral == null) || !config.isCreateValuesAlg() ) {
                        // fallback to convertRowConstructor
                        tuple = null;
                        break;
                    }
                    tuple.add( rexLiteral );
                }
                if ( tuple != null ) {
                    tuples.add( tuple.build() );
                    continue;
                }
            } else {
                RexLiteral rexLiteral = convertLiteralInValuesList( node, bb, rowType, 0 );
                if ( (rexLiteral != null) && config.isCreateValuesAlg() ) {
                    tuples.add( ImmutableList.of( rexLiteral ) );
                    continue;
                } else {
                    if ( (rexLiteral == null) && allowLiteralsOnly ) {
                        return null;
                    }
                }

                // convert "1" to "row(1)"
                call = (SqlBasicCall) OperatorRegistry.get( OperatorName.ROW ).createCall( ParserPos.ZERO, node );
            }
            unionInputs.add( convertRowConstructor( bb, call ) );
        }
        LogicalValues values = LogicalValues.create( cluster, rowType, tuples.build() );
        AlgNode resultRel;
        if ( unionInputs.isEmpty() ) {
            resultRel = values;
        } else {
            if ( !values.getTuples().isEmpty() ) {
                unionInputs.add( values );
            }
            resultRel = LogicalUnion.create( unionInputs, true );
        }
        leaves.add( resultRel );
        return resultRel;
    }


    private RexLiteral convertLiteralInValuesList( SqlNode sqlNode, Blackboard bb, AlgDataType rowType, int iField ) {
        if ( !(sqlNode instanceof SqlLiteral) ) {
            return null;
        }
        AlgDataTypeField field = rowType.getFields().get( iField );
        AlgDataType type = field.getType();
        if ( type.isStruct() ) {
            // null literals for weird stuff like UDT's need special handling during type flattening, so don't use LogicalValues for those
            return null;
        }

        RexNode literalExpr = exprConverter.convertLiteral( bb, (SqlLiteral) sqlNode );

        if ( !(literalExpr instanceof RexLiteral literal) ) {
            assert literalExpr.isA( Kind.CAST );
            RexNode child = ((RexCall) literalExpr).getOperands().get( 0 );
            assert RexLiteral.isNullLiteral( child );

            // NOTE jvs 22-Nov-2006:  we preserve type info in LogicalValues digest, so it's OK to lose it here
            return (RexLiteral) child;
        }

        PolyValue value = literal.getValue();

        if ( value == null ) {
            return (RexLiteral) rexBuilder.makeLiteral( null, type, false );
        }

        if ( PolyTypeUtil.isExactNumeric( type ) && PolyTypeUtil.hasScale( type ) ) {
            BigDecimal roundedValue = NumberUtil.rescaleBigDecimal( value.asBigDecimal().value, type.getScale() );
            return rexBuilder.makeExactLiteral( roundedValue, type );
        }

        if ( type.getPolyType() == PolyType.CHAR ) {
            // pad fixed character type
            PolyString unpadded = value.asString();
            return rexBuilder.makeCharLiteral(
                    new NlsString(
                            Spaces.padRight( unpadded.value, type.getPrecision() ),
                            String.valueOf( Charsets.UTF_8 ),
                            Collation.COERCIBLE ) );
        }
        return literal;
    }


    private boolean isRowConstructor( SqlNode node ) {
        if ( !(node.getKind() == Kind.ROW) ) {
            return false;
        }
        SqlCall call = (SqlCall) node;
        return call.getOperator().getName().equalsIgnoreCase( "row" );
    }


    /**
     * Builds a list of all <code>IN</code> or <code>EXISTS</code> operators inside SQL parse tree. Does not traverse inside queries.
     *
     * @param bb blackboard
     * @param node the SQL parse tree
     * @param logic Whether the answer needs to be in full 3-valued logic (TRUE, FALSE, UNKNOWN) will be required, or whether we can accept an approximation (say representing UNKNOWN as FALSE)
     * @param registerOnlyScalarSubQueries if set to true and the parse tree corresponds to a variation of a select node, only register it if it's a scalar sub-query
     */
    private void findSubQueries( Blackboard bb, SqlNode node, AlgOptUtil.Logic logic, boolean registerOnlyScalarSubQueries ) {
        final Kind kind = node.getKind();
        switch ( kind ) {
            case EXISTS:
            case SELECT:
            case MULTISET_QUERY_CONSTRUCTOR:
            case MULTISET_VALUE_CONSTRUCTOR:
            case ARRAY_QUERY_CONSTRUCTOR:
            case CURSOR:
            case SCALAR_QUERY:
                if ( !registerOnlyScalarSubQueries || (kind == Kind.SCALAR_QUERY) ) {
                    bb.registerSubQuery( node, AlgOptUtil.Logic.TRUE_FALSE );
                }
                return;
            case IN:
                break;
            case NOT_IN:
            case NOT:
                logic = logic.negate();
                break;
        }
        if ( node instanceof SqlCall ) {
            switch ( kind ) {
                // Do no change logic for AND, IN and NOT IN expressions; but do change logic for OR, NOT and others; EXISTS was handled already.
                case AND:
                case IN:
                case NOT_IN:
                    break;
                default:
                    logic = AlgOptUtil.Logic.TRUE_FALSE_UNKNOWN;
                    break;
            }
            for ( SqlNode operand : ((SqlCall) node).getSqlOperandList() ) {
                if ( operand != null ) {
                    // In the case of an IN expression, locate scalar sub-queries so we can convert them to constants
                    findSubQueries(
                            bb,
                            operand,
                            logic,
                            kind == Kind.IN || kind == Kind.NOT_IN || kind == Kind.SOME || kind == Kind.ALL || registerOnlyScalarSubQueries );
                }
            }
        } else if ( node instanceof SqlNodeList ) {
            for ( SqlNode child : ((SqlNodeList) node).getSqlList() ) {
                findSubQueries(
                        bb,
                        child,
                        logic,
                        kind == Kind.IN || kind == Kind.NOT_IN || kind == Kind.SOME || kind == Kind.ALL || registerOnlyScalarSubQueries );
            }
        }

        // Now that we've located any scalar sub-queries inside the IN expression, register the IN expression itself.  We need to
        // register the scalar sub-queries first so they can be converted before the IN expression is converted.
        switch ( kind ) {
            case IN:
            case NOT_IN:
            case SOME:
            case ALL:
                switch ( logic ) {
                    case TRUE_FALSE_UNKNOWN:
                        if ( validator.getValidatedNodeType( node ).isNullable() ) {
                            break;
                        } else if ( true ) {
                            break;
                        }
                        // fall through
                    case UNKNOWN_AS_FALSE:
                        logic = AlgOptUtil.Logic.TRUE;
                }
                bb.registerSubQuery( node, logic );
                break;
        }
    }


    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format.
     *
     * @param node Expression to translate
     * @return Converted expression
     */
    public RexNode convertExpression( SqlNode node ) {
        Map<String, AlgDataType> nameToTypeMap = Collections.emptyMap();
        final ParameterScope scope = new ParameterScope( (SqlValidatorImpl) validator, nameToTypeMap );
        final Blackboard bb = createBlackboard( scope, null, false );
        return bb.convertExpression( node );
    }


    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format, mapping identifier references to predefined expressions.
     *
     * @param node Expression to translate
     * @param nameToNodeMap map from String to {@link RexNode}; when an {@link SqlIdentifier} is encountered, it is used as a key and translated to the corresponding value from this map
     * @return Converted expression
     */
    public RexNode convertExpression( SqlNode node, Map<String, RexNode> nameToNodeMap ) {
        final Map<String, AlgDataType> nameToTypeMap = new HashMap<>();
        for ( Map.Entry<String, RexNode> entry : nameToNodeMap.entrySet() ) {
            nameToTypeMap.put( entry.getKey(), entry.getValue().getType() );
        }
        final ParameterScope scope = new ParameterScope( (SqlValidatorImpl) validator, nameToTypeMap );
        final Blackboard bb = createBlackboard( scope, nameToNodeMap, false );
        return bb.convertExpression( node );
    }


    /**
     * Converts a non-standard expression.
     * <p>
     * This method is an extension-point that derived classes can override. If this method returns a null result, the normal expression translation process will proceed. The default implementation always returns null.
     *
     * @param node Expression
     * @param bb Blackboard
     * @return null to proceed with the usual expression translation process
     */
    protected RexNode convertExtendedExpression( SqlNode node, Blackboard bb ) {
        return null;
    }


    private RexNode convertOver( Blackboard bb, SqlNode node ) {
        SqlCall call = (SqlCall) node;
        SqlCall aggCall = call.operand( 0 );
        SqlNode windowOrRef = call.operand( 1 );
        final SqlWindow window = validator.resolveWindow( windowOrRef, bb.scope, true );

        // ROW_NUMBER() expects specific kind of framing.
        if ( aggCall.getKind() == Kind.ROW_NUMBER ) {
            window.setLowerBound( SqlWindow.createUnboundedPreceding( ParserPos.ZERO ) );
            window.setUpperBound( SqlWindow.createCurrentRow( ParserPos.ZERO ) );
            window.setRows( SqlLiteral.createBoolean( true, ParserPos.ZERO ) );
        }
        final SqlNodeList partitionList = window.getPartitionList();
        final ImmutableList.Builder<RexNode> partitionKeys = ImmutableList.builder();
        for ( SqlNode partition : partitionList.getSqlList() ) {
            partitionKeys.add( bb.convertExpression( partition ) );
        }
        RexNode lowerBound = bb.convertExpression( window.getLowerBound() );
        RexNode upperBound = bb.convertExpression( window.getUpperBound() );
        SqlNodeList orderList = window.getOrderList();
        if ( (orderList.size() == 0) && !window.isRows() ) {
            // A logical range requires an ORDER BY clause. Use the implicit ordering of this relation. There must be one, otherwise it would have failed validation.
            orderList = bb.scope.getOrderList();
            if ( orderList == null ) {
                throw new AssertionError( "Relation should have sort key for implicit ORDER BY" );
            }
        }

        final ImmutableList.Builder<RexFieldCollation> orderKeys = ImmutableList.builder();
        for ( SqlNode order : orderList.getSqlList() ) {
            orderKeys.add(
                    bb.convertSortExpression(
                            order,
                            AlgFieldCollation.Direction.ASCENDING,
                            AlgFieldCollation.NullDirection.UNSPECIFIED ) );
        }

        try {
            Preconditions.checkArgument( bb.window == null, "already in window agg mode" );
            bb.window = window;
            RexNode rexAgg = exprConverter.convertCall( bb, aggCall );
            rexAgg =
                    rexBuilder.ensureType(
                            validator.getValidatedNodeType( call ),
                            rexAgg,
                            false );

            // Walk over the tree and apply 'over' to all agg functions. This is necessary because the returned expression is not necessarily a call to an agg function. For example, AVG(x) becomes SUM(x) / COUNT(x).

            final SqlLiteral q = aggCall.getFunctionQuantifier();
            final boolean isDistinct = q != null && q.value.asSymbol().value == SqlSelectKeyword.DISTINCT;

            final RexShuttle visitor =
                    new HistogramShuttle(
                            partitionKeys.build(),
                            orderKeys.build(),
                            RexWindowBound.create( window.getLowerBound(), lowerBound ),
                            RexWindowBound.create( window.getUpperBound(), upperBound ),
                            window,
                            isDistinct );

            return rexAgg.accept( visitor );
        } finally {
            bb.window = null;
        }
    }


    /**
     * Converts a FROM clause into a relational expression.
     *
     * @param bb Scope within which to resolve identifiers
     * @param from FROM clause of a query. Examples include:
     *
     * <ul>
     * <li>a single table ("SALES.EMP"),</li>
     * <li>an aliased table ("EMP AS E"),</li>
     * <li>a list of tables ("EMP, DEPT"),</li>
     * <li>an ANSI Join expression ("EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO"),</li>
     * <li>a VALUES clause ("VALUES ('Fred', 20)"),</li>
     * <li>a query ("(SELECT * FROM EMP WHERE GENDER = 'F')"),</li>
     * <li>or any combination of the above.</li>
     * </ul>
     */
    protected void convertFrom( Blackboard bb, SqlNode from ) {
        if ( from == null ) {
            bb.setRoot( LogicalValues.createOneRow( cluster ), false );
            return;
        }

        final SqlCall call;
        final SqlNode[] operands;
        switch ( from.getKind() ) {
            case MATCH_RECOGNIZE:
                convertMatchRecognize( bb, (SqlCall) from );
                return;

            case AS:
                call = (SqlCall) from;
                convertFrom( bb, call.operand( 0 ) );
                if ( call.operandCount() > 2 && (bb.root instanceof Values || bb.root instanceof Uncollect) ) {
                    final List<String> fieldNames = new ArrayList<>();
                    for ( SqlNode node : Util.skip( call.getSqlOperandList(), 2 ) ) {
                        fieldNames.add( ((SqlIdentifier) node).getSimple() );
                    }
                    bb.setRoot( algBuilder.push( bb.root ).rename( fieldNames ).build(), true );
                }
                return;

            case WITH_ITEM:
                convertFrom( bb, ((SqlWithItem) from).query );
                return;

            case WITH:
                convertFrom( bb, ((SqlWith) from).body );
                return;

            case TABLESAMPLE:
                operands = ((SqlBasicCall) from).getOperands();
                SqlSampleSpec sampleSpec = SqlLiteral.sampleValue( operands[1] );
                if ( sampleSpec instanceof SqlSampleSpec.SqlSubstitutionSampleSpec ) {
                    String sampleName = ((SqlSampleSpec.SqlSubstitutionSampleSpec) sampleSpec).getName();
                    datasetStack.push( sampleName );
                    convertFrom( bb, operands[0] );
                    datasetStack.pop();
                } else if ( sampleSpec instanceof SqlSampleSpec.SqlTableSampleSpec tableSampleSpec ) {
                    convertFrom( bb, operands[0] );
                    AlgOptSamplingParameters params =
                            new AlgOptSamplingParameters(
                                    tableSampleSpec.isBernoulli(),
                                    tableSampleSpec.getSamplePercentage(),
                                    tableSampleSpec.isRepeatable(),
                                    tableSampleSpec.getRepeatableSeed() );
                    bb.setRoot( new Sample( cluster, bb.root, params ), false );
                } else {
                    throw new AssertionError( "unknown TABLESAMPLE type: " + sampleSpec );
                }
                return;

            case IDENTIFIER:
                convertIdentifier( bb, (SqlIdentifier) from, null );
                return;

            case EXTEND:
                call = (SqlCall) from;
                SqlIdentifier id = (SqlIdentifier) call.getOperandList().get( 0 );
                SqlNodeList extendedColumns = (SqlNodeList) call.getOperandList().get( 1 );
                convertIdentifier( bb, id, extendedColumns );
                return;

            case JOIN:
                final SqlJoin join = (SqlJoin) from;
                final SqlValidatorScope scope = validator.getJoinScope( from );
                final Blackboard fromBlackboard = createBlackboard( scope, null, false );
                SqlNode left = join.getLeft();
                SqlNode right = join.getRight();
                final boolean isNatural = join.isNatural();
                final JoinType joinType = join.getJoinType();
                final SqlValidatorScope leftScope = Util.first( validator.getJoinScope( left ), ((DelegatingScope) bb.scope).getParent() );
                final Blackboard leftBlackboard = createBlackboard( leftScope, null, false );
                final SqlValidatorScope rightScope = Util.first( validator.getJoinScope( right ), ((DelegatingScope) bb.scope).getParent() );
                final Blackboard rightBlackboard = createBlackboard( rightScope, null, false );
                convertFrom( leftBlackboard, left );
                AlgNode leftRel = leftBlackboard.root;
                convertFrom( rightBlackboard, right );
                AlgNode rightRel = rightBlackboard.root;
                JoinAlgType convertedJoinType = convertJoinType( joinType );
                RexNode conditionExp;
                final SqlValidatorNamespace leftNamespace = validator.getSqlNamespace( left );
                final SqlValidatorNamespace rightNamespace = validator.getSqlNamespace( right );
                if ( isNatural ) {
                    final AlgDataType leftRowType = leftNamespace.getRowType();
                    final AlgDataType rightRowType = rightNamespace.getRowType();
                    final List<String> columnList = SqlValidatorUtil.deriveNaturalJoinColumnList( snapshot.nameMatcher, leftRowType, rightRowType );
                    conditionExp = convertUsing( leftNamespace, rightNamespace, columnList );
                } else {
                    conditionExp =
                            convertJoinCondition(
                                    fromBlackboard,
                                    leftNamespace,
                                    rightNamespace,
                                    join.getCondition(),
                                    join.getConditionType(),
                                    leftRel,
                                    rightRel );
                }

                final AlgNode joinRel =
                        createJoin(
                                fromBlackboard,
                                leftRel,
                                rightRel,
                                conditionExp,
                                convertedJoinType );
                bb.setRoot( joinRel, false );
                return;

            case SELECT:
            case INTERSECT:
            case EXCEPT:
            case UNION:
                final AlgNode alg = convertQueryRecursive( from, false, null ).project();
                bb.setRoot( alg, true );
                return;

            case VALUES:
                convertValuesImpl( bb, (SqlCall) from, null );
                return;

            case UNNEST:
                call = (SqlCall) from;
                final List<SqlNode> nodes = call.getSqlOperandList();
                final SqlUnnestOperator operator = (SqlUnnestOperator) call.getOperator();
                for ( SqlNode node : nodes ) {
                    replaceSubQueries( bb, node, AlgOptUtil.Logic.TRUE_FALSE_UNKNOWN );
                }
                final List<RexNode> exprs = new ArrayList<>();
                final List<String> fieldNames = new ArrayList<>();
                for ( Ord<SqlNode> node : Ord.zip( nodes ) ) {
                    exprs.add( bb.convertExpression( node.e ) );
                    fieldNames.add( validator.deriveAlias( node.e, node.i ) );
                }
                AlgNode child = (null != bb.root) ? bb.root : LogicalValues.createOneRow( cluster );
                algBuilder.push( child ).projectNamed( exprs, fieldNames, false );

                Uncollect uncollect =
                        new Uncollect(
                                cluster,
                                cluster.traitSetOf( Convention.NONE ),
                                algBuilder.build(),
                                operator.withOrdinality );
                bb.setRoot( uncollect, true );
                return;

            case COLLECTION_TABLE:
                call = (SqlCall) from;

                // Dig out real call; TABLE() wrapper is just syntactic.
                assert call.getOperandList().size() == 1;
                final SqlCall call2 = call.operand( 0 );
                convertCollectionTable( bb, call2 );
                return;

            default:
                throw new AssertionError( "not a join operator " + from );
        }
    }


    protected void convertMatchRecognize( Blackboard bb, SqlCall call ) {
        final SqlMatchRecognize matchRecognize = (SqlMatchRecognize) call;
        final SqlValidatorNamespace ns = validator.getSqlNamespace( matchRecognize );
        final SqlValidatorScope scope = validator.getMatchRecognizeScope( matchRecognize );

        final Blackboard matchBb = createBlackboard( scope, null, false );
        final AlgDataType rowType = ns.getRowType();
        // convert inner query, could be a table name or a derived table
        SqlNode expr = matchRecognize.getTableRef();
        convertFrom( matchBb, expr );
        final AlgNode input = matchBb.root;

        // PARTITION BY
        final SqlNodeList partitionList = matchRecognize.getPartitionList();
        final List<RexNode> partitionKeys = new ArrayList<>();
        for ( SqlNode partition : partitionList.getSqlList() ) {
            RexNode e = matchBb.convertExpression( partition );
            partitionKeys.add( e );
        }

        // ORDER BY
        final SqlNodeList orderList = matchRecognize.getOrderList();
        final List<AlgFieldCollation> orderKeys = new ArrayList<>();
        for ( SqlNode order : orderList.getSqlList() ) {
            final AlgFieldCollation.Direction direction;
            switch ( order.getKind() ) {
                case DESCENDING:
                    direction = AlgFieldCollation.Direction.DESCENDING;
                    order = ((SqlCall) order).operand( 0 );
                    break;
                case NULLS_FIRST:
                case NULLS_LAST:
                    throw new AssertionError();
                default:
                    direction = AlgFieldCollation.Direction.ASCENDING;
                    break;
            }
            final AlgFieldCollation.NullDirection nullDirection =
                    validator.getDefaultNullCollation().last( desc( direction ) )
                            ? AlgFieldCollation.NullDirection.LAST
                            : AlgFieldCollation.NullDirection.FIRST;
            RexNode e = matchBb.convertExpression( order );
            orderKeys.add( new AlgFieldCollation( ((RexIndexRef) e).getIndex(), direction, nullDirection ) );
        }
        final AlgCollation orders = cluster.traitSet().canonize( AlgCollations.of( orderKeys ) );

        // convert pattern
        final Set<String> patternVarsSet = new HashSet<>();
        SqlNode pattern = matchRecognize.getPattern();
        final BasicNodeVisitor<RexNode> patternVarVisitor =
                new BasicNodeVisitor<RexNode>() {
                    @Override
                    public RexNode visit( Call call ) {
                        List<SqlNode> operands = ((SqlCall) call).getSqlOperandList();
                        List<RexNode> newOperands = new ArrayList<>();
                        for ( SqlNode node : operands ) {
                            newOperands.add( node.accept( this ) );
                        }
                        return rexBuilder.makeCall( validator.getUnknownType(), call.getOperator(), newOperands );
                    }


                    @Override
                    public RexNode visit( Identifier id ) {
                        assert id.isSimple();
                        patternVarsSet.add( id.getSimple() );
                        return rexBuilder.makeLiteral( id.getSimple() );
                    }


                    @Override
                    public RexNode visit( Literal rawLiteral ) {
                        SqlLiteral literal = (SqlLiteral) rawLiteral;
                        if ( literal instanceof SqlNumericLiteral ) {
                            return rexBuilder.makeExactLiteral( BigDecimal.valueOf( literal.intValue( true ) ) );
                        } else {
                            return rexBuilder.makeLiteral( literal.booleanValue() );
                        }
                    }
                };
        final RexNode patternNode = pattern.accept( patternVarVisitor );

        SqlLiteral interval = matchRecognize.getInterval();
        RexNode intervalNode = null;
        if ( interval != null ) {
            intervalNode = matchBb.convertLiteral( interval );
        }

        // convert subset
        final SqlNodeList subsets = matchRecognize.getSubsetList();
        final Map<String, TreeSet<String>> subsetMap = new HashMap<>();
        for ( SqlNode node : subsets.getSqlList() ) {
            List<SqlNode> operands = ((SqlCall) node).getSqlOperandList();
            SqlIdentifier left = (SqlIdentifier) operands.get( 0 );
            patternVarsSet.add( left.getSimple() );
            SqlNodeList rights = (SqlNodeList) operands.get( 1 );
            final TreeSet<String> list = new TreeSet<>();
            for ( SqlNode right : rights.getSqlList() ) {
                assert right instanceof SqlIdentifier;
                list.add( ((SqlIdentifier) right).getSimple() );
            }
            subsetMap.put( left.getSimple(), list );
        }

        SqlNode afterMatch = matchRecognize.getAfter();
        if ( afterMatch == null ) {
            afterMatch = SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW.symbol( ParserPos.ZERO );
        }

        final RexNode after;
        if ( afterMatch instanceof SqlCall ) {
            List<SqlNode> operands = ((SqlCall) afterMatch).getSqlOperandList();
            Operator operator = ((SqlCall) afterMatch).getOperator();
            assert operands.size() == 1;
            SqlIdentifier id = (SqlIdentifier) operands.get( 0 );
            assert patternVarsSet.contains( id.getSimple() ) : id.getSimple() + " not defined in pattern";
            RexNode rex = rexBuilder.makeLiteral( id.getSimple() );
            after = rexBuilder.makeCall( validator.getUnknownType(), operator, ImmutableList.of( rex ) );
        } else {
            after = matchBb.convertExpression( afterMatch );
        }

        matchBb.setPatternVarRef( true );

        // convert measures
        final ImmutableMap.Builder<String, RexNode> measureNodes = ImmutableMap.builder();
        for ( SqlNode measure : matchRecognize.getMeasureList().getSqlList() ) {
            List<Node> operands = ((SqlCall) measure).getOperandList();
            String alias = ((SqlIdentifier) operands.get( 1 )).getSimple();
            RexNode rex = matchBb.convertExpression( operands.get( 0 ) );
            measureNodes.put( alias, rex );
        }

        // convert definitions
        final ImmutableMap.Builder<String, RexNode> definitionNodes = ImmutableMap.builder();
        for ( SqlNode def : matchRecognize.getPatternDefList().getSqlList() ) {
            List<Node> operands = ((SqlCall) def).getOperandList();
            String alias = ((SqlIdentifier) operands.get( 1 )).getSimple();
            RexNode rex = matchBb.convertExpression( operands.get( 0 ) );
            definitionNodes.put( alias, rex );
        }

        final SqlLiteral rowsPerMatch = matchRecognize.getRowsPerMatch();
        final boolean allRows = rowsPerMatch != null && rowsPerMatch.value.asSymbol().value == SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS;

        matchBb.setPatternVarRef( false );

        final AlgFactories.MatchFactory factory = AlgFactories.DEFAULT_MATCH_FACTORY;
        final AlgNode alg =
                factory.createMatch(
                        input,
                        patternNode,
                        rowType,
                        matchRecognize.getStrictStart().booleanValue(),
                        matchRecognize.getStrictEnd().booleanValue(),
                        definitionNodes.build(),
                        measureNodes.build(),
                        after,
                        subsetMap,
                        allRows,
                        partitionKeys,
                        orders,
                        intervalNode );
        bb.setRoot( alg, false );
    }


    private void convertIdentifier( Blackboard bb, SqlIdentifier id, SqlNodeList extendedColumns ) {
        final SqlValidatorNamespace fromNamespace = validator.getSqlNamespace( id ).resolve();
        if ( fromNamespace.getNode() != null ) {
            convertFrom( bb, fromNamespace.getNode() );
            return;
        }
        final String datasetName = datasetStack.isEmpty() ? null : datasetStack.peek();
        final boolean[] usedDataset = { false };
        Entity table = SqlValidatorUtil.getLogicalEntity( fromNamespace, snapshot, datasetName, usedDataset );
        if ( extendedColumns != null && extendedColumns.size() > 0 ) {
            assert table != null;
            final ValidatorTable validatorTable = table.unwrap( ValidatorTable.class ).orElseThrow();
            final List<AlgDataTypeField> extendedFields = SqlValidatorUtil.getExtendedColumns( validator.getTypeFactory(), table, extendedColumns );
            // table.extend( extendedFields ); todo dl
        }
        final AlgNode tableRel;
        if ( config.isConvertTableAccess() ) {
            tableRel = toAlg( table );
        } else if ( table.entityType == EntityType.VIEW ) {
            tableRel = LogicalRelViewScan.create( cluster, table );
        } else {
            tableRel = LogicalRelScan.create( cluster, table );
        }
        bb.setRoot( tableRel, true );
        if ( usedDataset[0] ) {
            bb.setDataset( datasetName );
        }
    }


    protected void convertCollectionTable( Blackboard bb, SqlCall call ) {
        final Operator operator = call.getOperator();
        if ( operator.getOperatorName() == OperatorName.TABLESAMPLE ) {
            final String sampleName = SqlLiteral.unchain( call.operand( 0 ) ).getValueAs( String.class );
            datasetStack.push( sampleName );
            SqlCall cursorCall = call.operand( 1 );
            SqlNode query = cursorCall.operand( 0 );
            AlgNode converted = convertQuery( query, false, false ).alg;
            bb.setRoot( converted, false );
            datasetStack.pop();
            return;
        }

        replaceSubQueries( bb, call, AlgOptUtil.Logic.TRUE_FALSE_UNKNOWN );

        // Expand table macro if possible. It's more efficient than LogicalTableFunctionScan.
        final SqlCallBinding callBinding = new SqlCallBinding( bb.scope.getValidator(), bb.scope, call );
        if ( operator instanceof SqlUserDefinedTableMacro udf ) {
            //final TranslatableEntity table = udf.getTable( typeFactory, callBinding.sqlOperands() );
            //final LogicalTable catalogTable = Catalog.getInstance().getTable( table.getId() );
            //final AlgDataType rowType = table.getRowType( typeFactory );
            //AlgOptEntity algOptEntity = AlgOptEntityImpl.create( null, rowType, table, catalogTable, null );
            //AlgNode converted = toAlg( algOptEntity );
            //bb.setRoot( converted, true );
            return;
        }

        Type elementType;
        if ( operator instanceof SqlUserDefinedTableFunction udtf ) {
            elementType = udtf.getElementType( typeFactory, callBinding.sqlOperands() );
        } else {
            elementType = null;
        }

        RexNode rexCall = bb.convertExpression( call );
        final List<AlgNode> inputs = bb.retrieveCursors();
        Set<AlgColumnMapping> columnMappings = getColumnMappings( (SqlOperator) operator );
        LogicalTableFunctionScan callRel =
                LogicalTableFunctionScan.create(
                        cluster,
                        inputs,
                        rexCall,
                        elementType,
                        validator.getValidatedNodeType( call ),
                        columnMappings );
        bb.setRoot( callRel, true );

        afterTableFunction( bb, call, callRel );

    }


    protected void afterTableFunction(
            SqlToAlgConverter.Blackboard bb,
            SqlCall call,
            LogicalTableFunctionScan callRel ) {
    }


    private Set<AlgColumnMapping> getColumnMappings( SqlOperator op ) {
        PolyReturnTypeInference rti = op.getReturnTypeInference();
        if ( rti == null ) {
            return null;
        }
        if ( rti instanceof TableFunctionReturnTypeInference tfrti ) {
            return tfrti.getColumnMappings();
        } else {
            return null;
        }
    }


    protected AlgNode createJoin( Blackboard bb, AlgNode leftRel, AlgNode rightRel, RexNode joinCond, JoinAlgType joinType ) {
        assert joinCond != null;

        final CorrelationUse p = getCorrelationUse( bb, rightRel );
        if ( p != null ) {
            LogicalCorrelate corr = LogicalCorrelate.create( leftRel, p.r, p.id, p.requiredColumns, SemiJoinType.of( joinType ) );
            if ( !joinCond.isAlwaysTrue() ) {
                final AlgFactories.FilterFactory factory = AlgFactories.DEFAULT_FILTER_FACTORY;
                return factory.createFilter( corr, joinCond );
            }
            return corr;
        }

        final Join originalJoin = (Join) AlgFactories.DEFAULT_JOIN_FACTORY.createJoin( leftRel, rightRel, joinCond, ImmutableSet.of(), joinType, false );

        return AlgOptUtil.pushDownJoinConditions( originalJoin, algBuilder );
    }


    private CorrelationUse getCorrelationUse( Blackboard bb, final AlgNode r0 ) {
        final Set<CorrelationId> correlatedVariables = AlgOptUtil.getVariablesUsed( r0 );
        if ( correlatedVariables.isEmpty() ) {
            return null;
        }
        final ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();
        final List<CorrelationId> correlNames = new ArrayList<>();

        // All correlations must refer the same namespace since correlation produces exactly one correlation source.
        // The same source might be referenced by different variables since
        // DeferredLookups are not de-duplicated at create time.
        SqlValidatorNamespace prevNs = null;

        for ( CorrelationId correlName : correlatedVariables ) {
            DeferredLookup lookup = mapCorrelToDeferred.get( correlName );
            RexFieldAccess fieldAccess = lookup.getFieldAccess( correlName );
            String originalRelName = lookup.getOriginalRelName();
            String originalFieldName = fieldAccess.getField().getName();

            final NameMatcher nameMatcher = Snapshot.nameMatcher;
            final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
            lookup.bb.scope.resolve( ImmutableList.of( originalRelName ), nameMatcher, false, resolved );
            assert resolved.count() == 1;
            final SqlValidatorScope.Resolve resolve = resolved.only();
            final SqlValidatorNamespace foundNs = resolve.namespace;
            final AlgDataType rowType = resolve.rowType();
            final int childNamespaceIndex = resolve.path.steps().get( 0 ).i;
            final SqlValidatorScope ancestorScope = resolve.scope;
            boolean correlInCurrentScope = bb.scope.isWithin( ancestorScope );

            if ( !correlInCurrentScope ) {
                continue;
            }

            if ( prevNs == null ) {
                prevNs = foundNs;
            } else {
                assert prevNs == foundNs : "All correlation variables should resolve to the same namespace." + " Prev ns=" + prevNs + ", new ns=" + foundNs;
            }

            int namespaceOffset = 0;
            if ( childNamespaceIndex > 0 ) {
                // If not the first child, need to figure out the width of output types from all the preceding namespaces
                assert ancestorScope instanceof ListScope;
                List<SqlValidatorNamespace> children = ((ListScope) ancestorScope).getChildren();

                for ( int i = 0; i < childNamespaceIndex; i++ ) {
                    SqlValidatorNamespace child = children.get( i );
                    namespaceOffset += child.getRowType().getFieldCount();
                }
            }

            RexFieldAccess topLevelFieldAccess = fieldAccess;
            while ( topLevelFieldAccess.getReferenceExpr() instanceof RexFieldAccess ) {
                topLevelFieldAccess = (RexFieldAccess) topLevelFieldAccess.getReferenceExpr();
            }
            final AlgDataTypeField field = rowType.getFields().get( topLevelFieldAccess.getField().getIndex() - namespaceOffset );
            int pos = namespaceOffset + field.getIndex();

            assert field.getType() == topLevelFieldAccess.getField().getType();

            assert pos != -1;

            if ( bb.mapRootRelToFieldProjection.containsKey( bb.root ) ) {
                // bb.root is an aggregate and only projects group by keys.
                Map<Integer, Integer> exprProjection = bb.mapRootRelToFieldProjection.get( bb.root );

                // sub-query can reference group by keys projected from the root of the outer relation.
                if ( exprProjection.containsKey( pos ) ) {
                    pos = exprProjection.get( pos );
                } else {
                    // correl not grouped
                    throw new AssertionError( "Identifier '" + originalRelName + "." + originalFieldName + "' is not a group expr" );
                }
            }

            requiredColumns.set( pos );
            correlNames.add( correlName );
        }

        if ( correlNames.isEmpty() ) {
            // None of the correlating variables originated in this scope.
            return null;
        }

        AlgNode r = r0;
        if ( correlNames.size() > 1 ) {
            // The same table was referenced more than once.
            // So we deduplicate.
            r = DeduplicateCorrelateVariables.go( rexBuilder, correlNames.get( 0 ), Util.skip( correlNames ), r0 );
            // Add new node to leaves.
            leaves.add( r );
        }
        return new CorrelationUse( correlNames.get( 0 ), requiredColumns.build(), r );
    }


    /**
     * Determines whether a sub-query is non-correlated. Note that a non-correlated sub-query can contain correlated references, provided those references do not reference select statements that are parents of the sub-query.
     *
     * @param subq the sub-query
     * @param bb blackboard used while converting the sub-query, i.e., the blackboard of the parent query of this sub-query
     * @return true if the sub-query is non-correlated
     */
    private boolean isSubQueryNonCorrelated( AlgNode subq, Blackboard bb ) {
        Set<CorrelationId> correlatedVariables = AlgOptUtil.getVariablesUsed( subq );
        for ( CorrelationId correlName : correlatedVariables ) {
            DeferredLookup lookup = mapCorrelToDeferred.get( correlName );
            String originalRelName = lookup.getOriginalRelName();

            final NameMatcher nameMatcher = Snapshot.nameMatcher;
            final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
            lookup.bb.scope.resolve( ImmutableList.of( originalRelName ), nameMatcher, false, resolved );

            SqlValidatorScope ancestorScope = resolved.only().scope;

            // If the correlated reference is in a scope that's "above" the sub-query, then this is a correlated sub-query.
            SqlValidatorScope parentScope = bb.scope;
            do {
                if ( ancestorScope == parentScope ) {
                    return false;
                }
                if ( parentScope instanceof DelegatingScope ) {
                    parentScope = ((DelegatingScope) parentScope).getParent();
                } else {
                    break;
                }
            } while ( parentScope != null );
        }
        return true;
    }


    /**
     * Returns a list of fields to be prefixed to each relational expression.
     *
     * @return List of system fields
     */
    protected List<AlgDataTypeField> getSystemFields() {
        return Collections.emptyList();
    }


    private RexNode convertJoinCondition( Blackboard bb, SqlValidatorNamespace leftNamespace, SqlValidatorNamespace rightNamespace, SqlNode condition, JoinConditionType conditionType, AlgNode leftRel, AlgNode rightRel ) {
        if ( condition == null ) {
            return rexBuilder.makeLiteral( true );
        }
        bb.setRoot( ImmutableList.of( leftRel, rightRel ) );
        replaceSubQueries( bb, condition, AlgOptUtil.Logic.UNKNOWN_AS_FALSE );
        switch ( conditionType ) {
            case ON:
                bb.setRoot( ImmutableList.of( leftRel, rightRel ) );
                return bb.convertExpression( condition );
            case USING:
                final SqlNodeList list = (SqlNodeList) condition;
                final List<String> nameList = new ArrayList<>();
                for ( SqlNode columnName : list.getSqlList() ) {
                    final SqlIdentifier id = (SqlIdentifier) columnName;
                    String name = id.getSimple();
                    nameList.add( name );
                }
                return convertUsing( leftNamespace, rightNamespace, nameList );
            default:
                throw Util.unexpected( conditionType );
        }
    }


    /**
     * Returns an expression for matching columns of a USING clause or inferred from NATURAL JOIN. "a JOIN b USING (x, y)" becomes "a.x = b.x AND a.y = b.y". Returns null if the column list is empty.
     *
     * @param leftNamespace Namespace of left input to join
     * @param rightNamespace Namespace of right input to join
     * @param nameList List of column names to join on
     * @return Expression to match columns from name list, or true if name list is empty
     */
    private @Nonnull
    RexNode convertUsing( SqlValidatorNamespace leftNamespace, SqlValidatorNamespace rightNamespace, List<String> nameList ) {
        final NameMatcher nameMatcher = snapshot.nameMatcher;
        final List<RexNode> list = new ArrayList<>();
        for ( String name : nameList ) {
            List<RexNode> operands = new ArrayList<>();
            int offset = 0;
            for ( SqlValidatorNamespace n : ImmutableList.of( leftNamespace, rightNamespace ) ) {
                final AlgDataType rowType = n.getRowType();
                final AlgDataTypeField field = nameMatcher.field( rowType, name );
                operands.add( rexBuilder.makeInputRef( field.getType(), offset + field.getIndex() ) );
                offset += rowType.getFields().size();
            }
            list.add( rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), operands ) );
        }
        return RexUtil.composeConjunction( rexBuilder, list );
    }


    private static JoinAlgType convertJoinType( JoinType joinType ) {
        return switch ( joinType ) {
            case COMMA, INNER, CROSS -> JoinAlgType.INNER;
            case FULL -> JoinAlgType.FULL;
            case LEFT -> JoinAlgType.LEFT;
            case RIGHT -> JoinAlgType.RIGHT;
            default -> throw Util.unexpected( joinType );
        };
    }


    /**
     * Converts the SELECT, GROUP BY and HAVING clauses of an aggregate query.
     * <p>
     * This method extracts SELECT, GROUP BY and HAVING clauses, and creates an {@link AggConverter}, then delegates to {@link #createAggImpl}.
     * Derived class may override this method to change any of those clauses or specify a different {@link AggConverter}.
     *
     * @param bb Scope within which to resolve identifiers
     * @param select Query
     * @param orderExprList Additional expressions needed to implement ORDER BY
     */
    protected void convertAgg( Blackboard bb, SqlSelect select, List<SqlNode> orderExprList ) {
        assert bb.root != null : "precondition: child != null";
        SqlNodeList groupList = select.getGroup();
        SqlNodeList selectList = select.getSqlSelectList();
        SqlNode having = select.getHaving();

        final AggConverter aggConverter = new AggConverter( bb, select );
        createAggImpl(
                bb,
                aggConverter,
                selectList,
                groupList,
                having,
                orderExprList );
    }


    protected final void createAggImpl( Blackboard bb, final AggConverter aggConverter, SqlNodeList selectList, SqlNodeList groupList, SqlNode having, List<SqlNode> orderExprList ) {
        // Find aggregate functions in SELECT and HAVING clause
        final AggregateFinder aggregateFinder = new AggregateFinder();
        selectList.accept( aggregateFinder );
        if ( having != null ) {
            having.accept( aggregateFinder );
        }

        // first replace the sub-queries inside the aggregates because they will provide input rows to the aggregates.
        replaceSubQueries( bb, aggregateFinder.list, AlgOptUtil.Logic.TRUE_FALSE_UNKNOWN );

        // also replace sub-queries inside filters in the aggregates
        replaceSubQueries( bb, aggregateFinder.filterList, AlgOptUtil.Logic.TRUE_FALSE_UNKNOWN );

        // also replace sub-queries inside ordering spec in the aggregates
        replaceSubQueries( bb, aggregateFinder.orderList, AlgOptUtil.Logic.TRUE_FALSE_UNKNOWN );

        // If group-by clause is missing, pretend that it has zero elements.
        if ( groupList == null ) {
            groupList = SqlNodeList.EMPTY;
        }

        replaceSubQueries( bb, groupList, AlgOptUtil.Logic.TRUE_FALSE_UNKNOWN );

        // register the group exprs

        // build a map to remember the projections from the top scope to the output of the current root.
        //
        // Polypheny-DB allows expressions, not just column references in group by list. This is not SQL 2003 compliant, but hey.

        final AggregatingSelectScope scope = aggConverter.aggregatingSelectScope;
        final AggregatingSelectScope.Resolved r = scope.resolved.get();
        for ( SqlNode groupExpr : r.groupExprList ) {
            aggConverter.addGroupExpr( groupExpr );
        }

        final RexNode havingExpr;
        final List<Pair<RexNode, String>> projects = new ArrayList<>();

        try {
            Preconditions.checkArgument( bb.agg == null, "already in agg mode" );
            bb.agg = aggConverter;

            // convert the select and having expressions, so that the agg converter knows which aggregations are required

            selectList.accept( aggConverter );
            // Assert we don't have dangling items left in the stack
            assert !aggConverter.inOver;
            for ( SqlNode expr : orderExprList ) {
                expr.accept( aggConverter );
                assert !aggConverter.inOver;
            }
            if ( having != null ) {
                having.accept( aggConverter );
                assert !aggConverter.inOver;
            }

            // compute inputs to the aggregator
            List<Pair<RexNode, String>> preExprs = aggConverter.getPreExprs();

            if ( preExprs.isEmpty() ) {
                // Special case for COUNT(*), where we can end up with no inputs at all.  The rest of the system doesn't like 0-tuples, so we select a dummy constant here.
                final RexNode zero = rexBuilder.makeExactLiteral( BigDecimal.ZERO );
                preExprs = ImmutableList.of( Pair.of( zero, null ) );
            }

            final AlgNode inputRel = bb.root;

            // Project the expressions required by agg and having.
            bb.setRoot(
                    algBuilder.push( inputRel )
                            .projectNamed( Pair.left( preExprs ), Pair.right( preExprs ), false )
                            .build(),
                    false );
            bb.mapRootRelToFieldProjection.put( bb.root, r.groupExprProjection );

            // REVIEW jvs: doesn't the declaration of monotonicity here assume sort-based aggregation at the physical level?

            // Tell bb which of group columns are sorted.
            bb.columnMonotonicities.clear();
            for ( SqlNode groupItem : groupList.getSqlList() ) {
                bb.columnMonotonicities.add( bb.scope.getMonotonicity( groupItem ) );
            }

            // Add the aggregator
            bb.setRoot(
                    createAggregate( bb, r.groupSet, r.groupSets, aggConverter.getAggCalls() ),
                    false );

            bb.mapRootRelToFieldProjection.put( bb.root, r.groupExprProjection );

            // Replace sub-queries in having here and modify having to use the replaced expressions
            if ( having != null ) {
                SqlNode newHaving = pushDownNotForIn( bb.scope, having );
                replaceSubQueries( bb, newHaving, AlgOptUtil.Logic.UNKNOWN_AS_FALSE );
                havingExpr = bb.convertExpression( newHaving );
            } else {
                havingExpr = algBuilder.literal( true );
            }

            // Now convert the other sub-queries in the select list.
            // This needs to be done separately from the sub-query inside any aggregate in the select list, and after the aggregate alg is allocated.
            replaceSubQueries( bb, selectList, AlgOptUtil.Logic.TRUE_FALSE_UNKNOWN );

            // Now sub-queries in the entire select list have been converted.
            // Convert the select expressions to get the final list to be projected.
            int k = 0;

            // For select expressions, use the field names previously assigned by the validator. If we derive afresh, we might generate names like "EXPR$2" that don't match the names generated by the
            // validator. This is especially the case when there are system fields; system fields appear in the relnode's rowtype but do not (yet) appear in the validator type.
            final SelectScope selectScope = SqlValidatorUtil.getEnclosingSelectScope( bb.scope );
            assert selectScope != null;
            final SqlValidatorNamespace selectNamespace = validator.getSqlNamespace( selectScope.getNode() );
            final List<String> names = selectNamespace.getRowType().getFieldNames();
            int sysFieldCount = selectList.size() - names.size();
            for ( SqlNode expr : selectList.getSqlList() ) {
                projects.add(
                        Pair.of(
                                bb.convertExpression( expr ),
                                k < sysFieldCount
                                        ? validator.deriveAlias( expr, k++ )
                                        : names.get( k++ - sysFieldCount ) ) );
            }

            for ( SqlNode expr : orderExprList ) {
                projects.add(
                        Pair.of(
                                bb.convertExpression( expr ),
                                validator.deriveAlias( expr, k++ ) ) );
            }
        } finally {
            bb.agg = null;
        }

        // implement HAVING (we have already checked that it is non-trivial)
        algBuilder.push( bb.root );
        if ( havingExpr != null ) {
            algBuilder.filter( havingExpr );
        }

        // implement the SELECT list
        algBuilder.project( Pair.left( projects ), Pair.right( projects ) ).rename( Pair.right( projects ) );
        bb.setRoot( algBuilder.build(), false );

        // Tell bb which of group columns are sorted.
        bb.columnMonotonicities.clear();
        for ( SqlNode selectItem : selectList.getSqlList() ) {
            bb.columnMonotonicities.add( bb.scope.getMonotonicity( selectItem ) );
        }
    }


    /**
     * Creates an Aggregate.
     * <p>
     * In case the aggregate alg changes the order in which it projects fields, the <code>groupExprProjection</code> parameter is provided, and the implementation of this method may modify it.
     * <p>
     * The <code>sortedCount</code> parameter is the number of expressions known to be monotonic. These expressions must be on the leading edge of the grouping keys. The default implementation of this method ignores this parameter.
     *
     * @param bb Blackboard
     * @param groupSet Bit set of ordinals of grouping columns
     * @param groupSets Grouping sets
     * @param aggCalls Array of calls to aggregate functions
     * @return LogicalAggregate
     */
    protected AlgNode createAggregate( Blackboard bb, ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        return LogicalAggregate.create( bb.root, groupSet, groupSets, aggCalls );
    }


    public RexDynamicParam convertDynamicParam( final SqlDynamicParam dynamicParam ) {
        // REVIEW jvs: dynamic params may be encountered out of order.  Should probably cross-check with the count from the parser at the end and make sure they all got filled in.  Why doesn't List have a resize() method?!?  Make this a utility.
        while ( dynamicParam.getIndex() >= dynamicParamSqlNodes.size() ) {
            dynamicParamSqlNodes.add( null );
        }

        dynamicParamSqlNodes.set(
                dynamicParam.getIndex(),
                dynamicParam );
        return rexBuilder.makeDynamicParam(
                getDynamicParamType( dynamicParam.getIndex() ),
                dynamicParam.getIndex() );
    }


    /**
     * Creates a list of collations required to implement the ORDER BY clause, if there is one. Populates <code>extraOrderExprs</code> with any sort expressions which are not in the select clause.
     *
     * @param bb Scope within which to resolve identifiers
     * @param select Select clause. Never null, because we invent a dummy SELECT if ORDER BY is applied to a set operation (UNION etc.)
     * @param orderList Order by clause, may be null
     * @param extraOrderExprs Sort expressions which are not in the select
     * clause (output)
     * @param collationList List of collations (output)
     */
    protected void gatherOrderExprs( Blackboard bb, SqlSelect select, SqlNodeList orderList, List<SqlNode> extraOrderExprs, List<AlgFieldCollation> collationList ) {
        // TODO: add validation rules to SqlValidator also
        assert bb.root != null : "precondition: child != null";
        assert select != null;
        if ( orderList == null ) {
            return;
        }

        if ( !bb.top ) {
            SqlNode offset = select.getOffset();
            if ( (offset == null
                    || (offset instanceof SqlLiteral
                    && ((SqlLiteral) offset).bigDecimalValue()
                    .equals( BigDecimal.ZERO )))
                    && select.getFetch() == null ) {
                return;
            }
        }

        for ( SqlNode orderItem : orderList.getSqlList() ) {
            collationList.add(
                    convertOrderItem(
                            select,
                            orderItem,
                            extraOrderExprs,
                            AlgFieldCollation.Direction.ASCENDING,
                            AlgFieldCollation.NullDirection.UNSPECIFIED ) );
        }
    }


    protected AlgFieldCollation convertOrderItem( SqlSelect select, SqlNode orderItem, List<SqlNode> extraExprs, AlgFieldCollation.Direction direction, AlgFieldCollation.NullDirection nullDirection ) {
        assert select != null;
        // Handle DESC keyword, e.g. 'select a, b from t order by a desc'.
        switch ( orderItem.getKind() ) {
            case DESCENDING:
                return convertOrderItem(
                        select,
                        ((SqlCall) orderItem).operand( 0 ),
                        extraExprs,
                        AlgFieldCollation.Direction.DESCENDING,
                        nullDirection );
            case NULLS_FIRST:
                return convertOrderItem(
                        select,
                        ((SqlCall) orderItem).operand( 0 ),
                        extraExprs,
                        direction,
                        AlgFieldCollation.NullDirection.FIRST );
            case NULLS_LAST:
                return convertOrderItem(
                        select,
                        ((SqlCall) orderItem).operand( 0 ),
                        extraExprs,
                        direction,
                        AlgFieldCollation.NullDirection.LAST );
        }

        SqlNode converted = validator.expandOrderExpr( select, orderItem );

        if ( Objects.requireNonNull( nullDirection ) == NullDirection.UNSPECIFIED ) {
            nullDirection = validator.getDefaultNullCollation().last( desc( direction ) )
                    ? NullDirection.LAST
                    : NullDirection.FIRST;
        }

        // Scan the select list and order exprs for an identical expression.
        final SelectScope selectScope = validator.getRawSelectScope( select );
        int ordinal = -1;
        for ( SqlNode selectItem : selectScope.getExpandedSelectList() ) {
            ++ordinal;
            if ( converted.equalsDeep( SqlUtil.stripAs( selectItem ), Litmus.IGNORE ) ) {
                return new AlgFieldCollation( ordinal, direction, nullDirection );
            }
        }

        for ( SqlNode extraExpr : extraExprs ) {
            ++ordinal;
            if ( converted.equalsDeep( extraExpr, Litmus.IGNORE ) ) {
                return new AlgFieldCollation( ordinal, direction, nullDirection );
            }
        }

        // TODO: handle collation sequence
        // TODO: flag expressions as non-standard

        extraExprs.add( converted );
        return new AlgFieldCollation( ordinal + 1, direction, nullDirection );
    }


    private static boolean desc( AlgFieldCollation.Direction direction ) {
        return switch ( direction ) {
            case DESCENDING, STRICTLY_DESCENDING -> true;
            default -> false;
        };
    }


    @Deprecated // to be removed before 2.0
    protected boolean enableDecorrelation() {
        // disable sub-query decorrelation when needed. e.g. if outer joins are not supported.
        return config.isDecorrelationEnabled();
    }


    protected AlgNode decorrelateQuery( AlgNode rootRel ) {
        return AlgDecorrelator.decorrelateQuery( rootRel, algBuilder );
    }


    /**
     * Returns whether to trim unused fields as part of the conversion process.
     *
     * @return Whether to trim unused fields
     */
    @Override
    @Deprecated // to be removed before 2.0
    public boolean isTrimUnusedFields() {
        return config.isTrimUnusedFields();
    }


    /**
     * Recursively converts a query to a relational expression.
     *
     * @param query Query
     * @param top Whether this query is the top-level query of the statement
     * @param targetRowType Target row type, or null
     * @return Relational expression
     */
    protected AlgRoot convertQueryRecursive( SqlNode query, boolean top, AlgDataType targetRowType ) {
        final Kind kind = query.getKind();
        return switch ( kind ) {
            case SELECT -> AlgRoot.of( convertSelect( (SqlSelect) query, top ), kind );
            case INSERT -> AlgRoot.of( convertInsert( (SqlInsert) query ), kind );
            case DELETE -> AlgRoot.of( convertDelete( (SqlDelete) query ), kind );
            case UPDATE -> AlgRoot.of( convertUpdate( (SqlUpdate) query ), kind );
            case MERGE -> AlgRoot.of( convertMerge( (SqlMerge) query ), kind );
            case UNION, INTERSECT, EXCEPT -> AlgRoot.of( convertSetOp( (SqlCall) query ), kind );
            case WITH -> convertWith( (SqlWith) query, top );
            case VALUES -> AlgRoot.of( convertValues( (SqlCall) query, targetRowType ), kind );
            default -> throw new AssertionError( "not a query: " + query );
        };
    }


    /**
     * Converts a set operation (UNION, INTERSECT, MINUS) into relational expressions.
     *
     * @param call Call to set operator
     * @return Relational expression
     */
    protected AlgNode convertSetOp( SqlCall call ) {
        final AlgNode left = convertQueryRecursive( call.operand( 0 ), false, null ).project();
        final AlgNode right = convertQueryRecursive( call.operand( 1 ), false, null ).project();
        return switch ( call.getKind() ) {
            case UNION -> LogicalUnion.create( ImmutableList.of( left, right ), all( call ) );
            case INTERSECT -> LogicalIntersect.create( ImmutableList.of( left, right ), all( call ) );
            case EXCEPT -> LogicalMinus.create( ImmutableList.of( left, right ), all( call ) );
            default -> throw Util.unexpected( call.getKind() );
        };
    }


    private boolean all( SqlCall call ) {
        return ((SqlSetOperator) call.getOperator()).isAll();
    }


    protected AlgNode convertInsert( SqlInsert call ) {
        Entity targetTable = getTargetTable( call );

        final AlgDataType targetRowType = validator.getValidatedNodeType( call );
        assert targetRowType != null;
        AlgNode sourceRel = convertQueryRecursive( call.getSource(), false, targetRowType ).project();
        AlgNode massagedRel = convertColumnList( call, sourceRel );

        return createModify( targetTable, massagedRel );
    }


    /**
     * Creates a relational expression to modify a table or modifiable view.
     */
    private AlgNode createModify( Entity targetTable, AlgNode source ) {
        Optional<ModifiableTable> oModifiableTable = targetTable.unwrap( ModifiableTable.class );
        if ( oModifiableTable.isPresent() && oModifiableTable.get() == targetTable.unwrap( Entity.class ).orElse( null ) ) {
            return oModifiableTable.get().toModificationTable(
                    cluster,
                    source.getTraitSet(),
                    targetTable,
                    source,
                    Modify.Operation.INSERT,
                    null,
                    null );
        }
        return LogicalRelModify.create(
                targetTable,
                source,
                Modify.Operation.INSERT,
                null,
                null,
                false );
    }


    public AlgNode toAlg( final Entity table ) {
        final AlgNode scan = table.unwrap( TranslatableEntity.class ).orElseThrow().toAlg( cluster, cluster.traitSet() );
        final InitializerExpressionFactory ief = table.unwrap( InitializerExpressionFactory.class ).orElse( NullInitializerExpressionFactory.INSTANCE );

        // Lazily create a blackboard that contains all non-generated columns.
        final Supplier<Blackboard> bb = () -> {
            RexNode sourceRef = rexBuilder.makeRangeReference( scan );
            return createInsertBlackboard( table, sourceRef, table.getRowType().getFieldNames() );
        };

        int virtualCount = 0;
        final List<RexNode> list = new ArrayList<>();
        for ( AlgDataTypeField f : table.getRowType().getFields() ) {
            final ColumnStrategy strategy = ief.generationStrategy( table, f.getIndex() );
            if ( Objects.requireNonNull( strategy ) == ColumnStrategy.VIRTUAL ) {
                list.add( ief.newColumnDefaultValue( table, f.getIndex(), bb.get() ) );
                ++virtualCount;
            } else {
                list.add( rexBuilder.makeInputRef( scan, f.getIndex() ) );
            }
        }
        if ( virtualCount > 0 ) {
            algBuilder.push( scan );
            algBuilder.project( list );
            return algBuilder.build();
        }
        return scan;
    }


    protected Entity getTargetTable( SqlNode call ) {
        final SqlValidatorNamespace targetNs = validator.getSqlNamespace( call );
        if ( targetNs.isWrapperFor( SqlValidatorImpl.DmlNamespace.class ) ) {
            final SqlValidatorImpl.DmlNamespace dmlNamespace = targetNs.unwrap( SqlValidatorImpl.DmlNamespace.class );
            return SqlValidatorUtil.getLogicalEntity( dmlNamespace, snapshot, null, null );
        }
        final SqlValidatorNamespace resolvedNamespace = targetNs.resolve();
        return SqlValidatorUtil.getLogicalEntity( resolvedNamespace, snapshot, null, null );
    }


    /**
     * Creates a source for an INSERT statement.
     * <p>
     * If the column list is not specified, source expressions match target columns in order.
     * <p>
     * If the column list is specified, Source expressions are mapped to target columns by name via targetColumnList, and may
     * not cover the entire target table. So, we'll make up a full row, using a combination of default values and the source
     * expressions provided.
     *
     * @param call Insert expression
     * @param source Source relational expression
     * @return Converted INSERT statement
     */
    protected AlgNode convertColumnList( final SqlInsert call, AlgNode source ) {
        AlgDataType sourceRowType = source.getTupleType();
        final RexNode sourceRef = rexBuilder.makeRangeReference( sourceRowType, 0, false );
        final List<String> targetColumnNames = new ArrayList<>();
        final List<RexNode> columnExprs = new ArrayList<>();
        collectInsertTargets( call, sourceRef, targetColumnNames, columnExprs );

        final Entity targetTable = getTargetTable( call );
        final AlgDataType targetRowType = targetTable.getRowType();//AlgOptEntityImpl.realRowType( targetTable );
        final List<AlgDataTypeField> targetFields = targetRowType.getFields();
        boolean isDocument = call.getSchemaType() == DataModel.DOCUMENT;

        List<RexNode> sourceExps = new ArrayList<>( Collections.nCopies( targetFields.size(), null ) );
        List<String> fieldNames = new ArrayList<>( Collections.nCopies( targetFields.size(), null ) ); // TODO DL: reevaluate and make final again?
        if ( isDocument ) {
            int size = (int) (targetFields.size() + targetColumnNames.stream().filter( name -> !targetFields.stream().map( AlgDataTypeField::getName ).collect( Collectors.toList() ).contains( name ) ).count());
            sourceExps = new ArrayList<>( Collections.nCopies( size, null ) );
            fieldNames = new ArrayList<>( Collections.nCopies( size, null ) );
        }

        final InitializerExpressionFactory initializerFactory = getInitializerFactory( validator.getSqlNamespace( call ).getTable() );

        // Walk the name list and place the associated value in the expression list according to the ordinal value returned from the table construct, leaving nulls in the list for columns
        // that are not referenced.
        final NameMatcher nameMatcher = snapshot.nameMatcher;

        for ( Pair<String, RexNode> p : Pair.zip( targetColumnNames, columnExprs ) ) {
            AlgDataTypeField field = nameMatcher.field( targetRowType, p.left );

            assert field != null : "column " + p.left + " not found";
            sourceExps.set( field.getIndex(), p.right );
        }

        // Lazily create a blackboard that contains all non-generated columns.
        final Supplier<Blackboard> bb = () -> createInsertBlackboard( targetTable, sourceRef, targetColumnNames );

        // Walk the expression list and get default values for any columns that were not supplied in the statement. Get field names too.
        for ( int i = 0; i < targetFields.size(); ++i ) {
            final AlgDataTypeField field = targetFields.get( i );
            final String fieldName = field.getName();
            fieldNames.set( i, fieldName );
            if ( sourceExps.get( i ) == null || sourceExps.get( i ).getKind() == Kind.DEFAULT ) {
                sourceExps.set( i, initializerFactory.newColumnDefaultValue( targetTable, i, bb.get() ) );

                // bare nulls are dangerous in the wrong hands
                sourceExps.set( i, castNullLiteralIfNeeded( sourceExps.get( i ), field.getType() ) );
            }
        }

        return algBuilder.push( source )
                .projectNamed( sourceExps, fieldNames, false )
                .build();
    }


    /**
     * Creates a blackboard for translating the expressions of generated columns in an INSERT statement.
     */
    private Blackboard createInsertBlackboard( Entity targetTable, RexNode sourceRef, List<String> targetColumnNames ) {
        final Map<String, RexNode> nameToNodeMap = new HashMap<>();
        int j = 0;

        // Assign expressions for non-generated columns.
        final List<ColumnStrategy> strategies = targetTable.unwrap( LogicalTable.class ).orElseThrow().getColumnStrategies();
        final List<String> targetFields = targetTable.getRowType().getFieldNames();
        for ( String targetColumnName : targetColumnNames ) {
            final int i = targetFields.indexOf( targetColumnName );
            switch ( strategies.get( i ) ) {
                case STORED:
                case VIRTUAL:
                    break;
                default:
                    nameToNodeMap.put( targetColumnName, rexBuilder.makeFieldAccess( sourceRef, j++ ) );
            }
        }
        return createBlackboard( null, nameToNodeMap, false );
    }


    private InitializerExpressionFactory getInitializerFactory( Entity validatorTable ) {
        // We might unwrap a null instead of a InitializerExpressionFactory.
        Optional<Entity> oEntity = validatorTable.unwrap( Entity.class );
        if ( oEntity.isPresent() ) {
            Optional<InitializerExpressionFactory> f = validatorTable.unwrap( InitializerExpressionFactory.class );
            if ( f.isPresent() ) {
                return f.get();
            }
        }
        return NullInitializerExpressionFactory.INSTANCE;
    }


    private RexNode castNullLiteralIfNeeded( RexNode node, AlgDataType type ) {
        if ( !RexLiteral.isNullLiteral( node ) ) {
            return node;
        }
        return rexBuilder.makeCast( type, node );
    }


    /**
     * Given an INSERT statement, collects the list of names to be populated and the expressions to put in them.
     *
     * @param call Insert statement
     * @param sourceRef Expression representing a row from the source relational expression
     * @param targetColumnNames List of target column names, to be populated
     * @param columnExprs List of expressions, to be populated
     */
    protected void collectInsertTargets( SqlInsert call, final RexNode sourceRef, final List<String> targetColumnNames, List<RexNode> columnExprs ) {
        final Entity targetTable = getTargetTable( call );
        final AlgDataType tableRowType = targetTable.getRowType();
        SqlNodeList targetColumnList = call.getTargetColumnList();
        if ( targetColumnList == null ) {
            if ( validator.getConformance().isInsertSubsetColumnsAllowed() ) {
                final AlgDataType targetRowType =
                        typeFactory.createStructType(
                                tableRowType.getFields()
                                        .subList( 0, sourceRef.getType().getFieldCount() ) );
                targetColumnNames.addAll( targetRowType.getFieldNames() );
            } else {
                targetColumnNames.addAll( tableRowType.getFieldNames() );
            }
        } else {

            boolean allowDynamic = call.getSchemaType() == DataModel.DOCUMENT;

            for ( int i = 0; i < targetColumnList.size(); i++ ) {
                SqlIdentifier id = (SqlIdentifier) targetColumnList.get( i );
                AlgDataTypeField field =
                        SqlValidatorUtil.getTargetField(
                                tableRowType,
                                typeFactory,
                                id,
                                snapshot,
                                targetTable,
                                allowDynamic );
                assert field != null : "column " + id.toString() + " not found";

                targetColumnNames.add( field.getName() );
            }
        }

        final Blackboard bb = createInsertBlackboard( targetTable, sourceRef, targetColumnNames );

        // Next, assign expressions for generated columns.
        final List<ColumnStrategy> strategies = targetTable.unwrap( LogicalTable.class ).orElseThrow().getColumnStrategies();
        for ( String columnName : targetColumnNames ) {
            final int i = tableRowType.getFieldNames().indexOf( columnName );
            final RexNode expr;
            if ( i != -1 ) {
                expr = switch ( strategies.get( i ) ) {
                    case STORED -> {
                        final InitializerExpressionFactory f = targetTable.unwrap( InitializerExpressionFactory.class ).orElse( NullInitializerExpressionFactory.INSTANCE );
                        yield f.newColumnDefaultValue( targetTable, i, bb );
                    }
                    case VIRTUAL -> null;
                    default -> bb.nameToNodeMap.get( columnName );
                };
            } else {
                expr = bb.nameToNodeMap.get( columnName );
            }
            columnExprs.add( expr );
        }

        // Remove virtual columns from the list.
        for ( int i = 0; i < targetColumnNames.size(); i++ ) {
            if ( columnExprs.get( i ) == null ) {
                columnExprs.remove( i );
                targetColumnNames.remove( i );
                --i;
            }
        }
    }


    private AlgNode convertDelete( SqlDelete call ) {
        Entity targetTable = getTargetTable( call );
        AlgNode sourceRel = convertSelect( call.getSourceSelect(), false );
        return LogicalRelModify.create(
                targetTable,
                sourceRel,
                Modify.Operation.DELETE,
                null,
                null,
                false );
    }


    private AlgNode convertUpdate( SqlUpdate call ) {
        final SqlValidatorScope scope = validator.getWhereScope( call.getSourceSelect() );
        Blackboard bb = createBlackboard( scope, null, false );

        Builder<RexNode> rexNodeSourceExpressionListBuilder = ImmutableList.builder();
        for ( SqlNode n : call.getSourceExpressionList().getSqlList() ) {
            RexNode rn = bb.convertExpression( n );
            rexNodeSourceExpressionListBuilder.add( rn );
        }

        Entity targetTable = getTargetTable( call );

        // convert update column list from SqlIdentifier to String
        final List<String> targetColumnNameList = new ArrayList<>();
        final AlgDataType targetRowType = targetTable.getRowType();
        for ( SqlNode node : call.getTargetColumnList().getSqlList() ) {
            SqlIdentifier id = (SqlIdentifier) node;
            AlgDataTypeField field = SqlValidatorUtil.getTargetField( targetRowType, typeFactory, id, snapshot, targetTable );
            assert field != null : "column " + id.toString() + " not found";
            targetColumnNameList.add( field.getName() );
        }

        AlgNode sourceRel = convertSelect( call.getSourceSelect(), false );

        return LogicalRelModify.create(
                targetTable,
                sourceRel,
                Modify.Operation.UPDATE,
                targetColumnNameList,
                rexNodeSourceExpressionListBuilder.build(),
                false );
    }


    private AlgNode convertMerge( SqlMerge call ) {
        Entity targetTable = getTargetTable( call );

        // convert update column list from SqlIdentifier to String
        final List<String> targetColumnNameList = new ArrayList<>();
        final AlgDataType targetRowType = targetTable.getRowType();
        SqlUpdate updateCall = call.getUpdateCall();
        if ( updateCall != null ) {
            for ( SqlNode targetColumn : updateCall.getTargetColumnList().getSqlList() ) {
                SqlIdentifier id = (SqlIdentifier) targetColumn;
                AlgDataTypeField field =
                        SqlValidatorUtil.getTargetField(
                                targetRowType,
                                typeFactory,
                                id,
                                snapshot,
                                targetTable );
                assert field != null : "column " + id.toString() + " not found";
                targetColumnNameList.add( field.getName() );
            }
        }

        // replace the projection of the source select with a projection that contains the following:
        // 1) the expressions corresponding to the new insert row (if there is an insert)
        // 2) all columns from the target table (if there is an update)
        // 3) the set expressions in the update call (if there is an update)

        // first, convert the merge's source select to construct the columns from the target table and the set expressions in the update call
        AlgNode mergeSourceRel = convertSelect( call.getSourceSelect(), false );

        // then, convert the insert statement so we can get the insert values expressions
        SqlInsert insertCall = call.getInsertCall();
        int nLevel1Exprs = 0;
        List<RexNode> level1InsertExprs = null;
        List<RexNode> level2InsertExprs = null;
        if ( insertCall != null ) {
            AlgNode insertRel = convertInsert( insertCall );

            // if there are 2 level of projections in the insert source, combine them into a single project; level1 refers to the topmost project;
            // the level1 projection contains references to the level2 expressions, except in the case where no target expression was provided, in which case, the expression is the default value for
            // the column; or if the expressions directly map to the source table
            level1InsertExprs = ((LogicalProject) insertRel.getInput( 0 )).getProjects();
            if ( insertRel.getInput( 0 ).getInput( 0 ) instanceof LogicalProject ) {
                level2InsertExprs = ((LogicalProject) insertRel.getInput( 0 ).getInput( 0 )).getProjects();
            }
            nLevel1Exprs = level1InsertExprs.size();
        }

        LogicalJoin join = (LogicalJoin) mergeSourceRel.getInput( 0 );
        int nSourceFields = join.getLeft().getTupleType().getFieldCount();
        final List<RexNode> projects = new ArrayList<>();
        for ( int level1Idx = 0; level1Idx < nLevel1Exprs; level1Idx++ ) {
            if ( (level2InsertExprs != null) && (level1InsertExprs.get( level1Idx ) instanceof RexIndexRef) ) {
                int level2Idx = ((RexIndexRef) level1InsertExprs.get( level1Idx )).getIndex();
                projects.add( level2InsertExprs.get( level2Idx ) );
            } else {
                projects.add( level1InsertExprs.get( level1Idx ) );
            }
        }
        if ( updateCall != null ) {
            final LogicalProject project = (LogicalProject) mergeSourceRel;
            projects.addAll( Util.skip( project.getProjects(), nSourceFields ) );
        }

        algBuilder.push( join ).project( projects );

        return LogicalRelModify.create(
                targetTable,
                algBuilder.build(),
                Modify.Operation.MERGE,
                targetColumnNameList,
                null,
                false );
    }


    /**
     * Converts an identifier into an expression in a given scope. For example, the "empno" in "select empno from emp join dept" becomes "emp.empno".
     */
    private RexNode convertIdentifier( Blackboard bb, SqlIdentifier identifier ) {
        // first check for reserved identifiers like CURRENT_USER
        final SqlCall call = SqlUtil.makeCall( opTab, identifier );
        if ( call != null ) {
            return bb.convertExpression( call );
        }

        String pv = null;
        if ( bb.isPatternVarRef && identifier.names.size() > 1 ) {
            pv = identifier.names.get( 0 );
        }

        final SqlQualified qualified;
        if ( bb.scope != null ) {
            qualified = bb.scope.fullyQualify( identifier );
        } else {
            qualified = SqlQualified.create( null, 1, null, identifier );
        }
        final Pair<RexNode, Map<String, Integer>> e0 = bb.lookupExp( qualified );
        RexNode e = e0.left;
        for ( String name : qualified.suffix() ) {
            if ( e == e0.left && e0.right != null ) {
                int i = e0.right.get( name );
                e = rexBuilder.makeFieldAccess( e, i );
            } else {
                final boolean caseSensitive = true; // name already fully-qualified
                if ( identifier.isStar() && bb.scope instanceof MatchRecognizeScope ) {
                    e = rexBuilder.makeFieldAccess( e, 0 );
                } else {
                    e = rexBuilder.makeFieldAccess( e, name, caseSensitive );
                }
            }
        }
        if ( e instanceof RexIndexRef ) {
            // adjust the type to account for nulls introduced by outer joins
            e = adjustInputRef( bb, (RexIndexRef) e );
            if ( pv != null ) {
                e = RexPatternFieldRef.of( pv, (RexIndexRef) e );
            }
        }

        if ( e0.left instanceof RexCorrelVariable ) {
            assert e instanceof RexFieldAccess;
            final RexNode prev = bb.mapCorrelateToRex.put( ((RexCorrelVariable) e0.left).id, (RexFieldAccess) e );
            assert prev == null;
        }
        return e;
    }


    /**
     * Adjusts the type of a reference to an input field to account for nulls introduced by outer joins; and adjusts the offset to match the physical implementation.
     *
     * @param bb Blackboard
     * @param inputRef Input ref
     * @return Adjusted input ref
     */
    protected RexNode adjustInputRef( Blackboard bb, RexIndexRef inputRef ) {
        AlgDataTypeField field = bb.getRootField( inputRef );
        if ( field != null ) {
            return rexBuilder.makeInputRef( field.getType(), inputRef.getIndex() );
        }
        return inputRef;
    }


    /**
     * Converts a row constructor into a relational expression.
     *
     * @param bb Blackboard
     * @param rowConstructor Row constructor expression
     * @return Relational expression which returns a single row.
     */
    private AlgNode convertRowConstructor( Blackboard bb, SqlCall rowConstructor ) {
        Preconditions.checkArgument( isRowConstructor( rowConstructor ) );
        final List<SqlNode> operands = rowConstructor.getSqlOperandList();
        return convertMultisets( operands, bb );
    }


    private AlgNode convertCursor( Blackboard bb, SubQuery subQuery ) {
        final SqlCall cursorCall = (SqlCall) subQuery.node;
        assert cursorCall.operandCount() == 1;
        SqlNode query = cursorCall.operand( 0 );
        AlgNode converted = convertQuery( query, false, false ).alg;
        int iCursor = bb.cursors.size();
        bb.cursors.add( converted );
        subQuery.expr = new RexIndexRef( iCursor, converted.getTupleType() );
        return converted;
    }


    private AlgNode convertMultisets( final List<SqlNode> operands, Blackboard bb ) {
        // NOTE: Wael 2/04/05: this implementation is not the most efficient in terms of planning since it generates XOs that can be reduced.
        final List<Object> joinList = new ArrayList<>();
        List<SqlNode> lastList = new ArrayList<>();
        for ( int i = 0; i < operands.size(); i++ ) {
            SqlNode operand = operands.get( i );
            if ( !(operand instanceof SqlCall call) ) {
                lastList.add( operand );
                continue;
            }

            final AlgNode input;
            switch ( call.getKind() ) {
                case MULTISET_VALUE_CONSTRUCTOR:
                case ARRAY_VALUE_CONSTRUCTOR:
                    final SqlNodeList list = new SqlNodeList( call.getSqlOperandList(), call.getPos() );
                    CollectNamespace nss = (CollectNamespace) validator.getSqlNamespace( call );
                    Blackboard usedBb;
                    if ( null != nss ) {
                        usedBb = createBlackboard( nss.getScope(), null, false );
                    } else {
                        usedBb = createBlackboard(
                                new ListScope( bb.scope ) {
                                    @Override
                                    public SqlNode getNode() {
                                        return call;
                                    }
                                },
                                null,
                                false );
                    }
                    AlgDataType multisetType = validator.getValidatedNodeType( call );
                    ((SqlValidatorImpl) validator).setValidatedNodeType( list, multisetType.getComponentType() );
                    input = convertQueryOrInList( usedBb, list, null );
                    break;
                case MULTISET_QUERY_CONSTRUCTOR:
                case ARRAY_QUERY_CONSTRUCTOR:
                    final AlgRoot root = convertQuery( call.operand( 0 ), false, true );
                    input = root.alg;
                    break;
                default:
                    lastList.add( operand );
                    continue;
            }

            if ( !lastList.isEmpty() ) {
                joinList.add( lastList );
            }
            lastList = new ArrayList<>();
            Collect collect = new Collect( cluster, cluster.traitSetOf( Convention.NONE ), input, validator.deriveAlias( call, i ) );
            joinList.add( collect );
        }

        if ( joinList.isEmpty() ) {
            joinList.add( lastList );
        }

        for ( int i = 0; i < joinList.size(); i++ ) {
            Object o = joinList.get( i );
            if ( o instanceof List ) {
                @SuppressWarnings("unchecked")
                List<SqlNode> projectList = (List<SqlNode>) o;
                final List<RexNode> selectList = new ArrayList<>();
                final List<String> fieldNameList = new ArrayList<>();
                for ( int j = 0; j < projectList.size(); j++ ) {
                    SqlNode operand = projectList.get( j );
                    selectList.add( bb.convertExpression( operand ) );

                    // REVIEW angel 5-June-2005: Use deriveAliasFromOrdinal instead of deriveAlias to match field names from SqlRowOperator. Otherwise, get error   Type
                    // 'RecordType(INTEGER EMPNO)' has no field 'EXPR$0' when doing select * from unnest( select multiset[empno] from sales.emps);

                    fieldNameList.add( CoreUtil.deriveAliasFromOrdinal( j ) );
                }

                algBuilder.push( LogicalValues.createOneRow( cluster ) ).projectNamed( selectList, fieldNameList, true );

                joinList.set( i, algBuilder.build() );
            }
        }

        AlgNode ret = (AlgNode) joinList.get( 0 );
        for ( int i = 1; i < joinList.size(); i++ ) {
            AlgNode algNode = (AlgNode) joinList.get( i );
            ret = AlgFactories.DEFAULT_JOIN_FACTORY.createJoin(
                    ret,
                    algNode,
                    rexBuilder.makeLiteral( true ),
                    ImmutableSet.of(),
                    JoinAlgType.INNER,
                    false );
        }
        return ret;
    }


    private void convertSelectList( Blackboard bb, SqlSelect select, List<SqlNode> orderList ) {
        SqlNodeList selectList = select.getSqlSelectList();
        selectList = validator.expandStar( selectList, select, false );

        replaceSubQueries( bb, selectList, AlgOptUtil.Logic.TRUE_FALSE_UNKNOWN );

        List<String> fieldNames = new ArrayList<>();
        final List<RexNode> exprs = new ArrayList<>();
        final Collection<String> aliases = new TreeSet<>();

        // Project any system fields. (Must be done before regular select items, because offsets may be affected.)
        final List<Monotonicity> columnMonotonicityList = new ArrayList<>();
        extraSelectItems(
                bb,
                select,
                exprs,
                fieldNames,
                aliases,
                columnMonotonicityList );

        // Project select clause.
        int i = -1;
        for ( SqlNode expr : selectList.getSqlList() ) {
            ++i;
            exprs.add( bb.convertExpression( expr ) );
            fieldNames.add( deriveAlias( expr, aliases, i ) );
        }

        // Project extra fields for sorting. todo do we need to have them?
        for ( SqlNode expr : orderList ) {
            ++i;
            SqlNode expr2 = validator.expandOrderExpr( select, expr );
            exprs.add( bb.convertExpression( expr2 ) );
            fieldNames.add( deriveAlias( expr, aliases, i ) );
        }

        fieldNames = ValidatorUtil.uniquify( fieldNames, snapshot.nameMatcher.isCaseSensitive() );

        algBuilder.push( bb.root ).projectNamed( exprs, fieldNames, true );
        bb.setRoot( algBuilder.build(), false );

        assert bb.columnMonotonicities.isEmpty();
        bb.columnMonotonicities.addAll( columnMonotonicityList );
        for ( SqlNode selectItem : selectList.getSqlList() ) {
            bb.columnMonotonicities.add( selectItem.getMonotonicity( bb.scope ) );
        }
    }


    /**
     * Adds extra select items. The default implementation adds nothing; derived classes may add columns to exprList, nameList, aliasList and columnMonotonicityList.
     *
     * @param bb Blackboard
     * @param select Select statement being translated
     * @param exprList List of expressions in select clause
     * @param nameList List of names, one per column
     * @param aliasList Collection of aliases that have been used already
     * @param columnMonotonicityList List of monotonicity, one per column
     */
    protected void extraSelectItems( Blackboard bb, SqlSelect select, List<RexNode> exprList, List<String> nameList, Collection<String> aliasList, List<Monotonicity> columnMonotonicityList ) {
    }


    private String deriveAlias( final SqlNode node, Collection<String> aliases, final int ordinal ) {
        String alias = validator.deriveAlias( node, ordinal );
        if ( (alias == null) || aliases.contains( alias ) ) {
            String aliasBase = (alias == null) ? "EXPR$" : alias;
            for ( int j = 0; ; j++ ) {
                alias = aliasBase + j;
                if ( !aliases.contains( alias ) ) {
                    break;
                }
            }
        }
        aliases.add( alias );
        return alias;
    }


    /**
     * Converts a WITH sub-query into a relational expression.
     */
    public AlgRoot convertWith( SqlWith with, boolean top ) {
        return convertQuery( with.body, false, top );
    }


    /**
     * Converts a SELECT statement's parse tree into a relational expression.
     */
    public AlgNode convertValues( SqlCall values, AlgDataType targetRowType ) {
        final SqlValidatorScope scope = validator.getOverScope( values );
        assert scope != null;
        final Blackboard bb = createBlackboard( scope, null, false );
        convertValuesImpl( bb, values, targetRowType );
        return bb.root;
    }


    /**
     * Converts a values clause (as in "INSERT INTO T(x,y) VALUES (1,2)") into a relational expression.
     *
     * @param bb Blackboard
     * @param values Call to SQL VALUES operator
     * @param targetRowType Target row type
     */
    private void convertValuesImpl( Blackboard bb, SqlCall values, AlgDataType targetRowType ) {
        // Attempt direct conversion to LogicalValues; if that fails, deal with fancy stuff like sub-queries below.
        AlgNode valuesRel =
                convertRowValues(
                        bb,
                        values,
                        values.getSqlOperandList(),
                        true,
                        targetRowType );
        if ( valuesRel != null ) {
            bb.setRoot( valuesRel, true );
            return;
        }

        final List<AlgNode> unionRels = new ArrayList<>();
        for ( SqlNode rowConstructor1 : values.getSqlOperandList() ) {
            SqlCall rowConstructor = (SqlCall) rowConstructor1;
            Blackboard tmpBb = createBlackboard( bb.scope, null, false );
            replaceSubQueries( tmpBb, rowConstructor, AlgOptUtil.Logic.TRUE_FALSE_UNKNOWN );
            final List<Pair<RexNode, String>> exps = new ArrayList<>();
            for ( Ord<SqlNode> operand : Ord.zip( rowConstructor.getSqlOperandList() ) ) {
                exps.add(
                        Pair.of(
                                tmpBb.convertExpression( operand.e ),
                                validator.deriveAlias( operand.e, operand.i ) ) );
            }
            AlgNode in =
                    (null == tmpBb.root)
                            ? LogicalValues.createOneRow( cluster )
                            : tmpBb.root;
            unionRels.add(
                    algBuilder.push( in )
                            .project( Pair.left( exps ), Pair.right( exps ) )
                            .build() );
        }

        if ( unionRels.isEmpty() ) {
            throw new AssertionError( "empty values clause" );
        } else if ( unionRels.size() == 1 ) {
            bb.setRoot( unionRels.get( 0 ), true );
        } else {
            bb.setRoot( LogicalUnion.create( unionRels, true ), true );
        }

        // REVIEW jvs 22-Jan-2004:  should I add mapScopeToLux.put(validator.getScope(values),bb.root); ?
    }


    /**
     * Workspace for translating an individual SELECT statement (or sub-SELECT).
     */
    protected class Blackboard implements SqlRexContext, NodeVisitor<RexNode>, InitializerContext {

        /**
         * Collection of {@link AlgNode} objects which correspond to a SELECT statement.
         */
        public final SqlValidatorScope scope;
        private final Map<String, RexNode> nameToNodeMap;
        public AlgNode root;
        private List<AlgNode> inputs;
        private final Map<CorrelationId, RexFieldAccess> mapCorrelateToRex = new HashMap<>();

        private boolean isPatternVarRef = false;

        final List<AlgNode> cursors = new ArrayList<>();

        /**
         * List of <code>IN</code> and <code>EXISTS</code> nodes inside this <code>SELECT</code> statement (but not inside sub-queries).
         */
        private final Set<SubQuery> subQueryList = new LinkedHashSet<>();

        /**
         * Workspace for building aggregates.
         */
        AggConverter agg;

        /**
         * When converting window aggregate, we need to know if the window is guaranteed to be non-empty.
         */
        SqlWindow window;

        /**
         * Project the groupby expressions out of the root of this sub-select.
         * Sub-queries can reference group by expressions projected from the "right" to the sub-query.
         */
        private final Map<AlgNode, Map<Integer, Integer>> mapRootRelToFieldProjection = new HashMap<>();

        @Getter
        private final List<Monotonicity> columnMonotonicities = new ArrayList<>();

        private final List<AlgDataTypeField> systemFieldList = new ArrayList<>();
        final boolean top;

        private final InitializerExpressionFactory initializerExpressionFactory = new NullInitializerExpressionFactory();


        /**
         * Creates a Blackboard.
         *
         * @param scope Name-resolution scope for expressions validated within this query. Can be null if this Blackboard is for a leaf node, say
         * @param nameToNodeMap Map which translates the expression to map a given parameter into, if translating expressions; null otherwise
         * @param top Whether this is the root of the query
         */
        protected Blackboard( SqlValidatorScope scope, Map<String, RexNode> nameToNodeMap, boolean top ) {
            this.scope = scope;
            this.nameToNodeMap = nameToNodeMap;
            this.top = top;
        }


        public void setPatternVarRef( boolean isVarRef ) {
            this.isPatternVarRef = isVarRef;
        }


        public RexNode register( AlgNode alg, JoinAlgType joinType ) {
            return register( alg, joinType, null );
        }


        /**
         * Registers a relational expression.
         *
         * @param alg Relational expression
         * @param joinType Join type
         * @param leftKeys LHS of IN clause, or null for expressions other than IN
         * @return Expression with which to refer to the row (or partial row) coming from this relational expression's side of the join
         */
        public RexNode register( AlgNode alg, JoinAlgType joinType, List<RexNode> leftKeys ) {
            assert joinType != null;
            if ( root == null ) {
                assert leftKeys == null;
                setRoot( alg, false );
                return rexBuilder.makeRangeReference(
                        root.getTupleType(),
                        0,
                        false );
            }

            final RexNode joinCond;
            final int origLeftInputCount = root.getTupleType().getFieldCount();
            if ( leftKeys != null ) {
                List<RexNode> newLeftInputExprs = new ArrayList<>();
                for ( int i = 0; i < origLeftInputCount; i++ ) {
                    newLeftInputExprs.add( rexBuilder.makeInputRef( root, i ) );
                }

                final List<Integer> leftJoinKeys = new ArrayList<>();
                for ( RexNode leftKey : leftKeys ) {
                    int index = newLeftInputExprs.indexOf( leftKey );
                    if ( index < 0 || joinType == JoinAlgType.LEFT ) {
                        index = newLeftInputExprs.size();
                        newLeftInputExprs.add( leftKey );
                    }
                    leftJoinKeys.add( index );
                }

                AlgNode newLeftInput =
                        algBuilder.push( root )
                                .project( newLeftInputExprs )
                                .build();

                // maintain the group by mapping in the new LogicalProject
                if ( mapRootRelToFieldProjection.containsKey( root ) ) {
                    mapRootRelToFieldProjection.put(
                            newLeftInput,
                            mapRootRelToFieldProjection.get( root ) );
                }

                setRoot( newLeftInput, false );

                // right fields appear after the LHS fields.
                final int rightOffset = root.getTupleType().getFieldCount() - newLeftInput.getTupleType().getFieldCount();
                final List<Integer> rightKeys = Util.range( rightOffset, rightOffset + leftKeys.size() );

                joinCond = AlgOptUtil.createEquiJoinCondition(
                        newLeftInput,
                        leftJoinKeys,
                        alg,
                        rightKeys,
                        rexBuilder );
            } else {
                joinCond = rexBuilder.makeLiteral( true );
            }

            int leftFieldCount = root.getTupleType().getFieldCount();
            final AlgNode join =
                    createJoin(
                            this,
                            root,
                            alg,
                            joinCond,
                            joinType );

            setRoot( join, false );

            if ( leftKeys != null && joinType == JoinAlgType.LEFT ) {
                final int leftKeyCount = leftKeys.size();
                int rightFieldLength = alg.getTupleType().getFieldCount();
                assert leftKeyCount == rightFieldLength - 1;

                final int rexRangeRefLength = leftKeyCount + rightFieldLength;
                AlgDataType returnType =
                        typeFactory.createStructType(
                                new AbstractList<>() {
                                    @Override
                                    public AlgDataTypeField get( int index ) {
                                        return join.getTupleType()
                                                .getFields()
                                                .get( origLeftInputCount + index );
                                    }


                                    @Override
                                    public int size() {
                                        return rexRangeRefLength;
                                    }
                                } );

                return rexBuilder.makeRangeReference( returnType, origLeftInputCount, false );
            } else {
                return rexBuilder.makeRangeReference( alg.getTupleType(), leftFieldCount, joinType.generatesNullsOnRight() );
            }
        }


        /**
         * Sets a new root relational expression, as the translation process backs its way further up the tree.
         *
         * @param root New root relational expression
         * @param leaf Whether the relational expression is a leaf, that is, derived from an atomic relational expression such as a table name in the from clause, or the projection on top of a select-sub-query. In particular, relational expressions derived from JOIN operators are not leaves, but set expressions are.
         */
        public void setRoot( AlgNode root, boolean leaf ) {
            setRoot( Collections.singletonList( root ), root, root instanceof LogicalJoin );
            if ( leaf ) {
                leaves.add( root );
            }
            this.columnMonotonicities.clear();
        }


        private void setRoot( List<AlgNode> inputs, AlgNode root, boolean hasSystemFields ) {
            this.inputs = inputs;
            this.root = root;
            this.systemFieldList.clear();
            if ( hasSystemFields ) {
                this.systemFieldList.addAll( getSystemFields() );
            }
        }


        /**
         * Notifies this Blackboard that the root just set using {@link #setRoot(AlgNode, boolean)} was derived using dataset substitution.
         * <p>
         * The default implementation is not interested in such notifications, and does nothing.
         *
         * @param datasetName Dataset name
         */
        public void setDataset( String datasetName ) {
        }


        void setRoot( List<AlgNode> inputs ) {
            setRoot( inputs, null, false );
        }


        /**
         * Returns an expression with which to reference a from-list item.
         *
         * @param qualified the alias of the from item
         * @return a {@link RexFieldAccess} or {@link RexRangeRef}, or null if not found
         */
        Pair<RexNode, Map<String, Integer>> lookupExp( SqlQualified qualified ) {
            if ( nameToNodeMap != null && qualified.prefixLength == 1 ) {
                RexNode node = nameToNodeMap.get( qualified.identifier.names.get( 0 ) );
                if ( node == null ) {
                    throw new AssertionError( "Unknown identifier '" + qualified.identifier + "' encountered while expanding expression" );
                }
                return Pair.of( node, null );
            }
            final NameMatcher nameMatcher = scope.getValidator().getSnapshot().nameMatcher;
            final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
            scope.resolve( qualified.prefix(), nameMatcher, false, resolved );
            if ( !(resolved.count() == 1) ) {
                return null;
            }
            final SqlValidatorScope.Resolve resolve = resolved.only();
            final AlgDataType rowType = resolve.rowType();

            // Found in current query's from list.  Find which from item.
            // We assume that the order of the from clause items has been preserved.
            final SqlValidatorScope ancestorScope = resolve.scope;
            boolean isParent = ancestorScope != scope;
            if ( (inputs != null) && !isParent ) {
                final LookupContext algs = new LookupContext( this, inputs, systemFieldList.size() );
                final RexNode node = lookup( resolve.path.steps().get( 0 ).i, algs );
                if ( node == null ) {
                    return null;
                } else {
                    final Map<String, Integer> fieldOffsets = new HashMap<>();
                    for ( AlgDataTypeField f : resolve.rowType().getFields() ) {
                        if ( !fieldOffsets.containsKey( f.getName() ) ) {
                            fieldOffsets.put( f.getName(), f.getIndex() );
                        }
                    }
                    final Map<String, Integer> map = ImmutableMap.copyOf( fieldOffsets );
                    return Pair.of( node, map );
                }
            } else {
                // We're referencing a relational expression which has not been converted yet. This occurs when from items are correlated, e.g. "select from emp as emp join emp.getDepts() as dept".
                // Create a temporary expression.
                DeferredLookup lookup = new DeferredLookup( this, qualified.identifier.names.get( 0 ) );
                final CorrelationId correlId = cluster.createCorrel();
                mapCorrelToDeferred.put( correlId, lookup );
                cluster.getMapCorrelToAlg().put( correlId, lookup.bb.root );
                if ( resolve.path.steps().get( 0 ).i < 0 ) {
                    return Pair.of( rexBuilder.makeCorrel( rowType, correlId ), null );
                } else {
                    final AlgDataTypeFactory.Builder builder = typeFactory.builder();
                    final ListScope ancestorScope1 = (ListScope) resolve.scope;
                    final ImmutableMap.Builder<String, Integer> fields = ImmutableMap.builder();
                    int i = 0;
                    int offset = 0;
                    for ( SqlValidatorNamespace c : ancestorScope1.getChildren() ) {
                        builder.addAll( c.getRowType().getFields() );
                        if ( i == resolve.path.steps().get( 0 ).i ) {
                            for ( AlgDataTypeField field : c.getRowType().getFields() ) {
                                fields.put( field.getName(), field.getIndex() + offset );
                            }
                        }
                        ++i;
                        offset += c.getRowType().getFieldCount();
                    }
                    final RexNode c = rexBuilder.makeCorrel( builder.uniquify().build(), correlId );
                    return Pair.of( c, fields.build() );
                }
            }
        }


        /**
         * Creates an expression with which to reference the expression whose offset in its from-list is {@code offset}.
         */
        RexNode lookup( int offset, LookupContext lookupContext ) {
            Pair<AlgNode, Integer> pair = lookupContext.findAlg( offset );
            return rexBuilder.makeRangeReference(
                    pair.left.getTupleType().asRelational(),
                    pair.right,
                    false );
        }


        AlgDataTypeField getRootField( RexIndexRef inputRef ) {
            if ( inputs == null ) {
                return null;
            }
            int fieldOffset = inputRef.getIndex();
            for ( AlgNode input : inputs ) {
                AlgDataType rowType = input.getTupleType();
                if ( rowType == null ) {
                    // TODO:  remove this once leastRestrictive is correctly implemented
                    return null;
                }
                if ( fieldOffset < rowType.getFieldCount() ) {
                    return rowType.getFields().get( fieldOffset );
                }
                fieldOffset -= rowType.getFieldCount();
            }
            throw new AssertionError();
        }


        public void flatten( List<AlgNode> algs, int systemFieldCount, int[] start, List<Pair<AlgNode, Integer>> algOffsetList ) {
            for ( AlgNode alg : algs ) {
                if ( leaves.contains( alg ) || alg instanceof LogicalMatch ) {
                    algOffsetList.add( Pair.of( alg, start[0] ) );
                    start[0] += alg.getTupleType().getFieldCount();
                } else {
                    if ( alg instanceof LogicalJoin || alg instanceof LogicalAggregate ) {
                        start[0] += systemFieldCount;
                    }
                    flatten( alg.getInputs(), systemFieldCount, start, algOffsetList );
                }
            }
        }


        void registerSubQuery( SqlNode node, AlgOptUtil.Logic logic ) {
            for ( SubQuery subQuery : subQueryList ) {
                if ( node.equalsDeep( subQuery.node, Litmus.IGNORE ) ) {
                    return;
                }
            }
            subQueryList.add( new SubQuery( node, logic ) );
        }


        SubQuery getSubQuery( SqlNode expr ) {
            for ( SubQuery subQuery : subQueryList ) {
                if ( expr.equalsDeep( subQuery.node, Litmus.IGNORE ) ) {
                    return subQuery;
                }
            }

            return null;
        }


        ImmutableList<AlgNode> retrieveCursors() {
            try {
                return ImmutableList.copyOf( cursors );
            } finally {
                cursors.clear();
            }
        }


        @Override
        public RexNode convertExpression( SqlNode expr ) {
            // If we're in aggregation mode and this is an expression in the GROUP BY clause, return a reference to the field.
            if ( agg != null ) {
                final SqlNode expandedGroupExpr = validator.expand( expr, scope );
                final int ref = agg.lookupGroupExpr( expandedGroupExpr );
                if ( ref >= 0 ) {
                    return rexBuilder.makeInputRef( root, ref );
                }
                if ( expr instanceof SqlCall ) {
                    final RexNode rex = agg.lookupAggregates( (SqlCall) expr );
                    if ( rex != null ) {
                        return rex;
                    }
                }
            }

            // Allow the derived class chance to override the standard behavior for special kinds of expressions.
            RexNode rex = convertExtendedExpression( expr, this );
            if ( rex != null ) {
                return rex;
            }

            // Sub-queries and OVER expressions are not like ordinary expressions.
            final Kind kind = expr.getKind();
            final SubQuery subQuery;
            if ( !config.isExpand() ) {
                final SqlCall call;
                final SqlNode query;
                final AlgRoot root;
                switch ( kind ) {
                    case IN:
                    case NOT_IN:
                    case SOME:
                    case ALL:
                        call = (SqlCall) expr;
                        query = call.operand( 1 );
                        if ( !(query instanceof SqlNodeList) ) {
                            root = convertQueryRecursive( query, false, null );
                            final SqlNode operand = call.operand( 0 );
                            List<SqlNode> nodes;
                            switch ( operand.getKind() ) {
                                case ROW:
                                    nodes = ((SqlCall) operand).getSqlOperandList();
                                    break;
                                default:
                                    nodes = ImmutableList.of( operand );
                            }
                            final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
                            for ( SqlNode node : nodes ) {
                                builder.add( convertExpression( node ) );
                            }
                            final ImmutableList<RexNode> list = builder.build();
                            return switch ( kind ) {
                                case IN -> RexSubQuery.in( root.alg, list );
                                case NOT_IN -> rexBuilder.makeCall( OperatorRegistry.get( OperatorName.NOT ), RexSubQuery.in( root.alg, list ) );
                                case SOME -> RexSubQuery.some( root.alg, list, (SqlQuantifyOperator) call.getOperator() );
                                case ALL -> rexBuilder.makeCall(
                                        OperatorRegistry.get( OperatorName.NOT ),
                                        RexSubQuery.some( root.alg, list, negate( (SqlQuantifyOperator) call.getOperator() ) ) );
                                default -> throw new AssertionError( kind );
                            };
                        }
                        break;

                    case EXISTS:
                        call = (SqlCall) expr;
                        query = Iterables.getOnlyElement( call.getSqlOperandList() );
                        root = convertQueryRecursive( query, false, null );
                        AlgNode alg = root.alg;
                        while ( alg instanceof Project
                                || alg instanceof Sort
                                && ((Sort) alg).fetch == null
                                && ((Sort) alg).offset == null ) {
                            alg = ((SingleAlg) alg).getInput();
                        }
                        return RexSubQuery.exists( alg );

                    case SCALAR_QUERY:
                        call = (SqlCall) expr;
                        query = (SqlNode) Iterables.getOnlyElement( call.getOperandList() );
                        root = convertQueryRecursive( query, false, null );
                        return RexSubQuery.scalar( root.alg );
                }
            }

            switch ( kind ) {
                case SOME:
                case ALL:
                    if ( config.isExpand() ) {
                        throw new GenericRuntimeException( kind + " is only supported if expand = false" );
                    }
                    // fall through
                case CURSOR:
                case IN:
                case NOT_IN:
                    subQuery = Objects.requireNonNull( getSubQuery( expr ) );
                    rex = Objects.requireNonNull( subQuery.expr );
                    return StandardConvertletTable.castToValidatedType( expr, rex, validator, rexBuilder );

                case SELECT:
                case EXISTS:
                case SCALAR_QUERY:
                    subQuery = getSubQuery( expr );
                    assert subQuery != null;
                    rex = subQuery.expr;
                    assert rex != null : "rex != null";

                    if ( ((kind == Kind.SCALAR_QUERY) || (kind == Kind.EXISTS)) && isConvertedSubq( rex ) ) {
                        // scalar sub-query or EXISTS has been converted to a constant
                        return rex;
                    }

                    // The indicator column is the last field of the sub-query.
                    RexNode fieldAccess = rexBuilder.makeFieldAccess( rex, rex.getType().getFieldCount() - 1 );

                    // The indicator column will be nullable if it comes from the null-generating side of the join. For EXISTS, add an "IS TRUE" check so that the result is "BOOLEAN NOT NULL".
                    if ( fieldAccess.getType().isNullable() && kind == Kind.EXISTS ) {
                        fieldAccess = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), fieldAccess );
                    }
                    return fieldAccess;

                case OVER:
                    return convertOver( this, expr );

                default:
                    // fall through
            }

            // Apply standard conversions.
            rex = expr.accept( this );
            return Objects.requireNonNull( rex );
        }


        /**
         * Converts an item in an ORDER BY clause inside a window (OVER) clause, extracting DESC, NULLS LAST and NULLS FIRST flags first.
         */
        public RexFieldCollation convertSortExpression( SqlNode expr, AlgFieldCollation.Direction direction, AlgFieldCollation.NullDirection nullDirection ) {
            switch ( expr.getKind() ) {
                case DESCENDING:
                    return convertSortExpression( ((SqlCall) expr).operand( 0 ), AlgFieldCollation.Direction.DESCENDING, nullDirection );
                case NULLS_LAST:
                    return convertSortExpression( ((SqlCall) expr).operand( 0 ), direction, AlgFieldCollation.NullDirection.LAST );
                case NULLS_FIRST:
                    return convertSortExpression( ((SqlCall) expr).operand( 0 ), direction, AlgFieldCollation.NullDirection.FIRST );
                default:
                    final Set<Kind> flags = EnumSet.noneOf( Kind.class );
                    if ( direction == Direction.DESCENDING ) {
                        flags.add( Kind.DESCENDING );
                    }
                    switch ( nullDirection ) {
                        case UNSPECIFIED:
                            final AlgFieldCollation.NullDirection nullDefaultDirection =
                                    validator.getDefaultNullCollation().last( desc( direction ) )
                                            ? AlgFieldCollation.NullDirection.LAST
                                            : AlgFieldCollation.NullDirection.FIRST;
                            if ( nullDefaultDirection != direction.defaultNullDirection() ) {
                                Kind nullDirectionKind =
                                        validator.getDefaultNullCollation().last( desc( direction ) )
                                                ? Kind.NULLS_LAST
                                                : Kind.NULLS_FIRST;
                                flags.add( nullDirectionKind );
                            }
                            break;
                        case FIRST:
                            flags.add( Kind.NULLS_FIRST );
                            break;
                        case LAST:
                            flags.add( Kind.NULLS_LAST );
                            break;
                    }
                    return new RexFieldCollation( convertExpression( expr ), flags );
            }
        }


        /**
         * Determines whether a RexNode corresponds to a sub-query that's been converted to a constant.
         *
         * @param rex the expression to be examined
         * @return true if the expression is a dynamic parameter, a literal, or a literal that is being cast
         */
        private boolean isConvertedSubq( RexNode rex ) {
            if ( (rex instanceof RexLiteral) || (rex instanceof RexDynamicParam) ) {
                return true;
            }
            if ( rex instanceof RexCall call ) {
                if ( call.getOperator().getOperatorName() == OperatorName.CAST ) {
                    RexNode operand = call.getOperands().get( 0 );
                    return operand instanceof RexLiteral;
                }
            }
            return false;
        }


        @Override
        public RexNode convertExpression( Node expr ) {
            return convertExpression( (SqlNode) expr );
        }


        @Override
        public int getGroupCount() {
            if ( agg != null ) {
                return agg.groupExprs.size();
            }
            if ( window != null ) {
                return window.isAlwaysNonEmpty() ? 1 : 0;
            }
            return -1;
        }


        @Override
        public RexBuilder getRexBuilder() {
            return rexBuilder;
        }


        @Override
        public RexRangeRef getSubQueryExpr( SqlCall call ) {
            final SubQuery subQuery = getSubQuery( call );
            assert subQuery != null;
            return (RexRangeRef) subQuery.expr;
        }


        @Override
        public AlgDataTypeFactory getTypeFactory() {
            return typeFactory;
        }


        @Override
        public InitializerExpressionFactory getInitializerExpressionFactory() {
            return initializerExpressionFactory;
        }


        @Override
        public SqlValidator getValidator() {
            return validator;
        }


        @Override
        public RexNode convertLiteral( SqlLiteral literal ) {
            return exprConverter.convertLiteral( this, literal );
        }


        public RexNode convertInterval( SqlIntervalQualifier intervalQualifier ) {
            return exprConverter.convertInterval( this, intervalQualifier );
        }


        @Override
        public RexNode visit( Literal literal ) {
            return exprConverter.convertLiteral( this, (SqlLiteral) literal );
        }


        @Override
        public RexNode visit( Call call ) {
            if ( agg != null ) {
                final Operator op = call.getOperator();
                if ( window == null
                        && (op.isAggregator()
                        || op.getKind() == Kind.FILTER
                        || op.getKind() == Kind.WITHIN_GROUP) ) {
                    return agg.lookupAggregates( (SqlCall) call );
                }
            }
            return exprConverter.convertCall( this, new SqlCallBinding( validator, scope, (SqlCall) call ).permutedCall() );
        }


        @Override
        public RexNode visit( NodeList nodeList ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public RexNode visit( Identifier id ) {
            return convertIdentifier( this, (SqlIdentifier) id );
        }


        @Override
        public RexNode visit( DataTypeSpec type ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public RexNode visit( DynamicParam param ) {
            return convertDynamicParam( (SqlDynamicParam) param );
        }


        @Override
        public RexNode visit( IntervalQualifier intervalQualifier ) {
            return convertInterval( (SqlIntervalQualifier) intervalQualifier );
        }


    }


    private SqlQuantifyOperator negate( SqlQuantifyOperator operator ) {
        assert operator.kind == Kind.ALL;
        return SqlStdOperatorTable.some( operator.comparisonKind.negateNullSafe() );
    }


    /**
     * Deferred lookup.
     */
    private static class DeferredLookup {

        Blackboard bb;
        @Getter
        String originalRelName;


        DeferredLookup( Blackboard bb, String originalRelName ) {
            this.bb = bb;
            this.originalRelName = originalRelName;
        }


        public RexFieldAccess getFieldAccess( CorrelationId name ) {
            return bb.mapCorrelateToRex.get( name );
        }


    }


    /**
     * A default implementation of SubQueryConverter that does no conversion.
     */
    private static class NoOpSubQueryConverter implements SubQueryConverter {

        @Override
        public boolean canConvertSubQuery() {
            return false;
        }


        @Override
        public RexNode convertSubQuery( SqlCall subQuery, SqlToAlgConverter parentConverter, boolean isExists, boolean isExplain ) {
            throw new IllegalArgumentException();
        }

    }


    /**
     * Converts expressions to aggregates.
     * <p>
     * Consider the expression
     *
     * <blockquote>
     * {@code SELECT deptno, SUM(2 * sal) FROM emp GROUP BY deptno}
     * </blockquote>
     *
     * Then:
     *
     * <ul>
     * <li>groupExprs = {SqlIdentifier(deptno)}</li>
     * <li>convertedInputExprs = {RexInputRef(deptno), 2 * RefInputRef(sal)}</li>
     * <li>inputRefs = {RefInputRef(#0), RexInputRef(#1)}</li>
     * <li>aggCalls = {AggCall(SUM, {1})}</li>
     * </ul>
     */
    protected class AggConverter implements NodeVisitor<Void> {

        private final Blackboard bb;
        public final AggregatingSelectScope aggregatingSelectScope;

        private final Map<String, String> nameMap = new HashMap<>();

        /**
         * The group-by expressions, in {@link SqlNode} format.
         */
        private final SqlNodeList groupExprs = new SqlNodeList( ParserPos.ZERO );

        /**
         * The auxiliary group-by expressions.
         */
        private final Map<SqlNode, Ord<AuxiliaryConverter>> auxiliaryGroupExprs = new HashMap<>();

        /**
         * Input expressions for the group columns and aggregates, in {@link RexNode} format. The first elements of the list correspond to the elements in {@link #groupExprs}; the remaining elements are for aggregates. The right field of each pair is the name of the expression,
         * where the expressions are simple mappings to input fields.
         */
        private final List<Pair<RexNode, String>> convertedInputExprs = new ArrayList<>();

        /**
         * Expressions to be evaluated as rows are being placed into the aggregate's hash table. This is when group functions such as TUMBLE cause rows to be expanded.
         */

        @Getter
        private final List<AggregateCall> aggCalls = new ArrayList<>();
        private final Map<SqlNode, RexNode> aggMapping = new HashMap<>();
        private final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

        /**
         * Are we directly inside a windowed aggregate?
         */
        private boolean inOver = false;


        /**
         * Creates an AggConverter.
         * <p>
         * The <code>select</code> parameter provides enough context to name aggregate calls which are top-level select list items.
         *
         * @param bb Blackboard
         * @param select Query being translated; provides context to give
         */
        public AggConverter( Blackboard bb, SqlSelect select ) {
            this.bb = bb;
            this.aggregatingSelectScope = (AggregatingSelectScope) bb.getValidator().getSelectScope( select );

            // Collect all expressions used in the select list so that aggregate calls can be named correctly.
            final SqlNodeList selectList = select.getSqlSelectList();
            for ( int i = 0; i < selectList.size(); i++ ) {
                SqlNode selectItem = selectList.getSqlList().get( i );
                String name = null;
                if ( SqlUtil.isCallTo( selectItem, OperatorRegistry.get( OperatorName.AS ) ) ) {
                    final SqlCall call = (SqlCall) selectItem;
                    selectItem = call.operand( 0 );
                    name = call.operand( 1 ).toString();
                }
                if ( name == null ) {
                    name = validator.deriveAlias( selectItem, i );
                }
                nameMap.put( selectItem.toString(), name );
            }

        }


        public int addGroupExpr( SqlNode expr ) {
            int ref = lookupGroupExpr( expr );
            if ( ref >= 0 ) {
                return ref;
            }
            final int index = groupExprs.size();
            groupExprs.add( expr );
            String name = nameMap.get( expr.toString() );
            RexNode convExpr = bb.convertExpression( expr );
            addExpr( convExpr, name );

            if ( expr instanceof SqlCall call ) {
                for ( Pair<SqlNode, AuxiliaryConverter> p : SqlStdOperatorTable.convertGroupToAuxiliaryCalls( call ) ) {
                    addAuxiliaryGroupExpr( p.left, index, p.right );
                }
            }

            return index;
        }


        void addAuxiliaryGroupExpr( SqlNode node, int index, AuxiliaryConverter converter ) {
            for ( SqlNode node2 : auxiliaryGroupExprs.keySet() ) {
                if ( node2.equalsDeep( node, Litmus.IGNORE ) ) {
                    return;
                }
            }
            auxiliaryGroupExprs.put( node, Ord.of( index, converter ) );
        }


        /**
         * Adds an expression, deducing an appropriate name if possible.
         *
         * @param expr Expression
         * @param name Suggested name
         */
        private void addExpr( RexNode expr, String name ) {
            if ( (name == null) && (expr instanceof RexIndexRef) ) {
                final int i = ((RexIndexRef) expr).getIndex();
                name = bb.root.getTupleType().getFields().get( i ).getName();
            }
            if ( Pair.right( convertedInputExprs ).contains( name ) ) {
                // In case like 'SELECT ... GROUP BY x, y, x', don't add name 'x' twice.
                name = null;
            }
            convertedInputExprs.add( Pair.of( expr, name ) );
        }


        @Override
        public Void visit( Identifier id ) {
            return null;
        }


        @Override
        public Void visit( NodeList nodeList ) {
            for ( int i = 0; i < nodeList.size(); i++ ) {
                nodeList.get( i ).accept( this );
            }
            return null;
        }


        @Override
        public Void visit( Literal lit ) {
            return null;
        }


        @Override
        public Void visit( DataTypeSpec type ) {
            return null;
        }


        @Override
        public Void visit( DynamicParam param ) {
            return null;
        }


        @Override
        public Void visit( IntervalQualifier intervalQualifier ) {
            return null;
        }


        @Override
        public Void visit( Call rawCall ) {
            SqlCall call = (SqlCall) rawCall;
            switch ( call.getKind() ) {
                case FILTER:
                case WITHIN_GROUP:
                    translateAgg( call );
                    return null;
                case SELECT:
                    // rchen 2006-10-17:
                    // for now do not detect aggregates in sub-queries.
                    return null;
            }
            final boolean prevInOver = inOver;
            // Ignore window aggregates and ranking functions (associated with OVER operator). However, do not ignore nested window aggregates.
            if ( call.getOperator().getKind() == Kind.OVER ) {
                // Track aggregate nesting levels only within an OVER operator.
                List<SqlNode> operandList = call.getSqlOperandList();
                assert operandList.size() == 2;

                // Ignore the top level window aggregates and ranking functions positioned as the first operand of a OVER operator
                inOver = true;
                operandList.get( 0 ).accept( this );

                // Normal translation for the second operand of a OVER operator
                inOver = false;
                operandList.get( 1 ).accept( this );
                return null;
            }

            // Do not translate the top level window aggregate. Only do so for nested aggregates, if present
            if ( call.getOperator().isAggregator() ) {
                if ( inOver ) {
                    // Add the parent aggregate level before visiting its children
                    inOver = false;
                } else {
                    // We're beyond the one ignored level
                    translateAgg( call );
                    return null;
                }
            }
            for ( SqlNode operand : call.getSqlOperandList() ) {
                // Operands are occasionally null, e.g. switched CASE arg 0.
                if ( operand != null ) {
                    operand.accept( this );
                }
            }
            // Remove the parent aggregate level after visiting its children
            inOver = prevInOver;
            return null;
        }


        private void translateAgg( SqlCall call ) {
            translateAgg( call, null, null, call );
        }


        private void translateAgg( SqlCall call, SqlNode filter, SqlNodeList orderList, SqlCall outerCall ) {
            assert bb.agg == this;
            assert outerCall != null;
            switch ( call.getKind() ) {
                case FILTER:
                    assert filter == null;
                    translateAgg( call.operand( 0 ), call.operand( 1 ), orderList, outerCall );
                    return;
                case WITHIN_GROUP:
                    assert orderList == null;
                    translateAgg( call.operand( 0 ), filter, call.operand( 1 ), outerCall );
                    return;
            }
            final List<Integer> args = new ArrayList<>();
            int filterArg = -1;
            final List<AlgDataType> argTypes =
                    call.getOperator() instanceof SqlCountAggFunction
                            ? new ArrayList<>( call.getOperandList().size() )
                            : null;
            try {
                // switch out of agg mode
                bb.agg = null;
                for ( Node operand : call.getOperandList() ) {

                    // special case for COUNT(*):  delete the *
                    if ( operand instanceof SqlIdentifier id ) {
                        if ( id.isStar() ) {
                            assert call.operandCount() == 1;
                            assert args.isEmpty();
                            break;
                        }
                    }
                    RexNode convertedExpr = bb.convertExpression( operand );
                    assert convertedExpr != null;
                    if ( argTypes != null ) {
                        argTypes.add( convertedExpr.getType() );
                    }
                    args.add( lookupOrCreateGroupExpr( convertedExpr ) );
                }

                if ( filter != null ) {
                    RexNode convertedExpr = bb.convertExpression( filter );
                    assert convertedExpr != null;
                    if ( convertedExpr.getType().isNullable() ) {
                        convertedExpr = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_TRUE ), convertedExpr );
                    }
                    filterArg = lookupOrCreateGroupExpr( convertedExpr );
                }
            } finally {
                // switch back into agg mode
                bb.agg = this;
            }

            SqlAggFunction aggFunction = (SqlAggFunction) call.getOperator();
            final AlgDataType type = validator.deriveType( bb.scope, call );
            boolean distinct = false;
            SqlLiteral quantifier = call.getFunctionQuantifier();
            if ( (null != quantifier) && (quantifier.value.asSymbol().value == SqlSelectKeyword.DISTINCT) ) {
                distinct = true;
            }
            boolean approximate = false;
            if ( aggFunction.equals( OperatorRegistry.get( OperatorName.APPROX_COUNT_DISTINCT ) ) ) {
                aggFunction = (SqlAggFunction) OperatorRegistry.getAgg( OperatorName.COUNT );
                distinct = true;
                approximate = true;
            }

            final AlgCollation collation;
            if ( orderList == null || orderList.size() == 0 ) {
                collation = AlgCollations.EMPTY;
            } else {
                collation = AlgCollations.of(
                        orderList.getSqlList()
                                .stream()
                                .map( order ->
                                        bb.convertSortExpression(
                                                order,
                                                AlgFieldCollation.Direction.ASCENDING,
                                                AlgFieldCollation.NullDirection.UNSPECIFIED ) )
                                .map( fieldCollation ->
                                        new AlgFieldCollation(
                                                lookupOrCreateGroupExpr( fieldCollation.left ),
                                                fieldCollation.getDirection(),
                                                fieldCollation.getNullDirection() ) )
                                .collect( Collectors.toList() ) );
            }

            final AggregateCall aggCall =
                    AggregateCall.create(
                            aggFunction,
                            distinct,
                            approximate,
                            args,
                            filterArg,
                            collation,
                            type,
                            nameMap.get( outerCall.toString() ) );
            final AggregatingSelectScope.Resolved r = aggregatingSelectScope.resolved.get();
            RexNode rex =
                    rexBuilder.addAggCall(
                            aggCall,
                            groupExprs.size(),
                            false,
                            aggCalls,
                            aggCallMapping,
                            argTypes );
            aggMapping.put( outerCall, rex );
        }


        private int lookupOrCreateGroupExpr( RexNode expr ) {
            int index = 0;
            for ( RexNode convertedInputExpr : Pair.left( convertedInputExprs ) ) {
                if ( expr.equals( convertedInputExpr ) ) {
                    return index;
                }
                ++index;
            }

            // not found -- add it
            addExpr( expr, null );
            return index;
        }


        /**
         * If an expression is structurally identical to one of the group-by expressions, returns a reference to the expression, otherwise returns null.
         */
        public int lookupGroupExpr( SqlNode expr ) {
            for ( int i = 0; i < groupExprs.size(); i++ ) {
                Node groupExpr = groupExprs.get( i );
                if ( expr.equalsDeep( groupExpr, Litmus.IGNORE ) ) {
                    return i;
                }
            }
            return -1;
        }


        public RexNode lookupAggregates( SqlCall call ) {
            // assert call.getOperator().isAggregator();
            assert bb.agg == this;

            for ( Map.Entry<SqlNode, Ord<AuxiliaryConverter>> e : auxiliaryGroupExprs.entrySet() ) {
                if ( call.equalsDeep( e.getKey(), Litmus.IGNORE ) ) {
                    AuxiliaryConverter converter = e.getValue().e;
                    final int groupOrdinal = e.getValue().i;
                    return converter.convert(
                            rexBuilder,
                            convertedInputExprs.get( groupOrdinal ).left,
                            rexBuilder.makeInputRef( bb.root, groupOrdinal ) );
                }
            }

            return aggMapping.get( call );
        }


        public List<Pair<RexNode, String>> getPreExprs() {
            return convertedInputExprs;
        }


        public AlgDataTypeFactory getTypeFactory() {
            return typeFactory;
        }

    }


    /**
     * Context to find a relational expression to a field offset.
     */
    private static class LookupContext {

        private final List<Pair<AlgNode, Integer>> algOffsetList = new ArrayList<>();


        /**
         * Creates a LookupContext with multiple input relational expressions.
         *
         * @param bb Context for translating this sub-query
         * @param algs Relational expressions
         * @param systemFieldCount Number of system fields
         */
        LookupContext( Blackboard bb, List<AlgNode> algs, int systemFieldCount ) {
            bb.flatten( algs, systemFieldCount, new int[]{ 0 }, algOffsetList );
        }


        /**
         * Returns the relational expression with a given offset, and the ordinal in the combined row of its first field.
         * <p>
         * For example, in {@code Emp JOIN Dept}, findRel(1) returns the relational expression for {@code Dept} and offset 6 (because {@code Emp} has 6 fields, therefore the first field of {@code Dept} is field 6.
         *
         * @param offset Offset of relational expression in FROM clause
         * @return Relational expression and the ordinal of its first field
         */
        Pair<AlgNode, Integer> findAlg( int offset ) {
            return algOffsetList.get( offset );
        }

    }


    /**
     * Shuttle which walks over a tree of {@link RexNode}s and applies 'over' to all agg functions.
     * <p>
     * This is necessary because the returned expression is not necessarily a call to an agg function. For example,
     *
     * <blockquote><code>AVG(x)</code></blockquote>
     *
     * becomes
     *
     * <blockquote><code>SUM(x) / COUNT(x)</code></blockquote>
     *
     * Any aggregate functions are converted to calls to the internal <code>$Histogram</code> aggregation function and accessors such as <code>$HistogramMin</code>; for example,
     *
     * <blockquote><code>MIN(x), MAX(x)</code></blockquote>
     *
     * are converted to
     *
     * <blockquote><code>$HistogramMin($Histogram(x)), $HistogramMax($Histogram(x))</code></blockquote>
     *
     * Common sub-expression elimination will ensure that only one histogram is computed.
     */
    private class HistogramShuttle extends RexShuttle {

        /**
         * Whether to convert calls to MIN(x) to HISTOGRAM_MIN(HISTOGRAM(x)). Histograms allow rolling computation, but require more space.
         */
        static final boolean ENABLE_HISTOGRAM_AGG = false;

        private final List<RexNode> partitionKeys;
        private final ImmutableList<RexFieldCollation> orderKeys;
        private final RexWindowBound lowerBound;
        private final RexWindowBound upperBound;
        private final SqlWindow window;
        private final boolean distinct;


        HistogramShuttle( List<RexNode> partitionKeys, ImmutableList<RexFieldCollation> orderKeys, RexWindowBound lowerBound, RexWindowBound upperBound, SqlWindow window, boolean distinct ) {
            this.partitionKeys = partitionKeys;
            this.orderKeys = orderKeys;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.window = window;
            this.distinct = distinct;
        }


        @Override
        public RexNode visitCall( RexCall call ) {
            final Operator op = call.getOperator();
            if ( !(op instanceof SqlAggFunction aggOp) ) {
                return super.visitCall( call );
            }
            final AlgDataType type = call.getType();
            List<RexNode> exprs = call.getOperands();

            SqlFunction histogramOp = !ENABLE_HISTOGRAM_AGG
                    ? null
                    : getHistogramOp( aggOp );

            if ( histogramOp != null ) {
                final AlgDataType histogramType = computeHistogramType( type );

                // For DECIMAL, since it's already represented as a bigint we want to do a reinterpretCast instead of a cast to avoid losing any precision.
                boolean reinterpretCast = type.getPolyType() == PolyType.DECIMAL;

                // Replace original expression with CAST of not one of the supported types
                if ( histogramType != type ) {
                    exprs = new ArrayList<>( exprs );
                    exprs.set(
                            0,
                            reinterpretCast
                                    ? rexBuilder.makeReinterpretCast( histogramType, exprs.get( 0 ), rexBuilder.makeLiteral( false ) )
                                    : rexBuilder.makeCast( histogramType, exprs.get( 0 ) ) );
                }

                RexCallBinding bind =
                        new RexCallBinding(
                                rexBuilder.getTypeFactory(),
                                OperatorRegistry.get( OperatorName.HISTOGRAM_AGG ),
                                exprs,
                                ImmutableList.of() );

                RexNode over =
                        rexBuilder.makeOver(
                                OperatorRegistry.get( OperatorName.HISTOGRAM_AGG ).inferReturnType( bind ),
                                OperatorRegistry.getAgg( OperatorName.HISTOGRAM_AGG ),
                                exprs,
                                partitionKeys,
                                orderKeys,
                                lowerBound,
                                upperBound,
                                window.isRows(),
                                window.isAllowPartial(),
                                false,
                                distinct );

                RexNode histogramCall =
                        rexBuilder.makeCall(
                                histogramType,
                                histogramOp,
                                ImmutableList.of( over ) );

                // If needed, post Cast result back to original type.
                if ( histogramType != type ) {
                    if ( reinterpretCast ) {
                        histogramCall =
                                rexBuilder.makeReinterpretCast(
                                        type,
                                        histogramCall,
                                        rexBuilder.makeLiteral( false ) );
                    } else {
                        histogramCall = rexBuilder.makeCast( type, histogramCall );
                    }
                }

                return histogramCall;
            } else {
                boolean needSum0 = aggOp.equals( OperatorRegistry.get( OperatorName.SUM ) ) && type.isNullable();
                AggFunction aggOpToUse =
                        needSum0
                                ? OperatorRegistry.getAgg( OperatorName.SUM0 )
                                : aggOp;
                return rexBuilder.makeOver(
                        type,
                        aggOpToUse,
                        exprs,
                        partitionKeys,
                        orderKeys,
                        lowerBound,
                        upperBound,
                        window.isRows(),
                        window.isAllowPartial(),
                        needSum0,
                        distinct );
            }
        }


        /**
         * Returns the histogram operator corresponding to a given aggregate function.
         * <p>
         * For example, <code>getHistogramOp ({@link OperatorRegistry MIN}}</code> returns {@link OperatorRegistry HISTOGRAM_MIN}.
         *
         * @param aggFunction An aggregate function
         * @return Its histogram function, or null
         */
        SqlFunction getHistogramOp( SqlAggFunction aggFunction ) {
            if ( aggFunction.getOperatorName() == OperatorName.MIN ) {
                return OperatorRegistry.get( OperatorName.HISTOGRAM_MIN, SqlFunction.class );
            } else if ( aggFunction.getOperatorName() == OperatorName.MAX ) {
                return OperatorRegistry.get( OperatorName.HISTOGRAM_MAX, SqlFunction.class );
            } else if ( aggFunction.getOperatorName() == OperatorName.FIRST_VALUE ) {
                return OperatorRegistry.get( OperatorName.HISTOGRAM_FIRST_VALUE, SqlFunction.class );
            } else if ( aggFunction.getOperatorName() == OperatorName.LAST_VALUE ) {
                return OperatorRegistry.get( OperatorName.HISTOGRAM_LAST_VALUE, SqlFunction.class );
            } else {
                return null;
            }


        }


        /**
         * Returns the type for a histogram function. It is either the actual type or an an approximation to it.
         */
        private AlgDataType computeHistogramType( AlgDataType type ) {
            if ( PolyTypeUtil.isExactNumeric( type ) && type.getPolyType() != PolyType.BIGINT ) {
                return typeFactory.createPolyType( PolyType.BIGINT );
            } else if ( PolyTypeUtil.isApproximateNumeric( type ) && type.getPolyType() != PolyType.DOUBLE ) {
                return typeFactory.createPolyType( PolyType.DOUBLE );
            } else {
                return type;
            }
        }

    }


    /**
     * A sub-query, whether it needs to be translated using 2- or 3-valued logic.
     */
    private static class SubQuery {

        final SqlNode node;
        final AlgOptUtil.Logic logic;
        RexNode expr;


        private SubQuery( SqlNode node, AlgOptUtil.Logic logic ) {
            this.node = node;
            this.logic = logic;
        }

    }


    /**
     * Visitor that collects all aggregate functions in a {@link SqlNode} tree.
     */
    private static class AggregateFinder extends BasicNodeVisitor<Void> {

        final SqlNodeList list = new SqlNodeList( ParserPos.ZERO );
        final SqlNodeList filterList = new SqlNodeList( ParserPos.ZERO );
        final SqlNodeList orderList = new SqlNodeList( ParserPos.ZERO );


        @Override
        public Void visit( Call call ) {
            // ignore window aggregates and ranking functions (associated with OVER operator)
            if ( call.getOperator().getKind() == Kind.OVER ) {
                return null;
            }

            if ( call.getOperator().getKind() == Kind.FILTER ) {
                // the WHERE in a FILTER must be tracked too so we can call replaceSubQueries on it.
                final Node aggCall = call.getOperandList().get( 0 );
                final Node whereCall = call.getOperandList().get( 1 );
                list.add( aggCall );
                filterList.add( whereCall );
                return null;
            }

            if ( call.getOperator().getKind() == Kind.WITHIN_GROUP ) {
                // the WHERE in a WITHIN_GROUP must be tracked too so we can call replaceSubQueries on it.
                final Node aggCall = call.getOperandList().get( 0 );
                final SqlNodeList orderList = (SqlNodeList) call.getOperandList().get( 1 );
                list.add( aggCall );
                orderList.getList().forEach( this.orderList::add );
                return null;
            }

            if ( call.getOperator().isAggregator() ) {
                list.add( call );
                return null;
            }

            // Don't traverse into sub-queries, even if they contain aggregate functions.
            if ( call instanceof SqlSelect ) {
                return null;
            }

            return call.getOperator().acceptCall( this, call );
        }

    }


    /**
     * Use of a row as a correlating variable by a given relational expression.
     *
     * @param r The relational expression that uses the variable.
     */
    private record CorrelationUse(CorrelationId id, ImmutableBitSet requiredColumns, AlgNode r) {

    }


}

