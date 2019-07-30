/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.prepare;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableBindable;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableCalc;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableInterpretable;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableInterpreterRule;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel.Prefer;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRules;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.RexToLixTranslator;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.BindableConvention;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Bindables;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Interpreters;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema.LatticeEntry;
import ch.unibas.dmi.dbis.polyphenydb.materialize.MaterializationService;
import ch.unibas.dmi.dbis.polyphenydb.plan.Contexts;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCostFactory;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.VolcanoPlanner;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AbstractMaterializedViewRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateExpandDistinctAggregatesRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateReduceFunctionsRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateStarTableRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateValuesRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterAggregateTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterJoinRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterProjectTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterTableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.JoinAssociateRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.JoinCommuteRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.JoinPushExpressionsRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.JoinPushThroughJoinRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.MaterializedViewFilterScanRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectFilterTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectMergeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectTableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectWindowTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ReduceExpressionsRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.SortJoinTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.SortProjectTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.SortRemoveConstantKeysRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.SortUnionTransposeRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.TableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ValuesReduceRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.stream.StreamRules;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Bindable;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Typed;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlBinaryOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExecutableStatement;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserImplFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ExtraSqlTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.ChainedSqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlRexConvertletTable;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlToRelConverter;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.StandardConvertletTable;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
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


/**
 * This class is public so that projects that create their own JDBC driver and server can fine-tune preferences. However, this class and its methods are subject to change without notice.
 */
public class PolyphenyDbPrepareImpl implements PolyphenyDbPrepare {

    public static final boolean DEBUG = Util.getBooleanProperty( "polyphenydb.debug" );

    public static final boolean COMMUTE = Util.getBooleanProperty( "polyphenydb.enable.join.commute" );

    /**
     * Whether to enable the collation trait. Some extra optimizations are possible if enabled, but queries should work either way. At some point
     * this will become a preference, or we will run multiple phases: first disabled, then enabled.
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

    public static final List<RelOptRule> ENUMERABLE_RULES =
            ImmutableList.of(
                    EnumerableRules.ENUMERABLE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_RULE,
                    EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                    EnumerableRules.ENUMERABLE_SORT_RULE,
                    EnumerableRules.ENUMERABLE_LIMIT_RULE,
                    EnumerableRules.ENUMERABLE_COLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
                    EnumerableRules.ENUMERABLE_UNION_RULE,
                    EnumerableRules.ENUMERABLE_INTERSECT_RULE,
                    EnumerableRules.ENUMERABLE_MINUS_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
                    EnumerableRules.ENUMERABLE_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_WINDOW_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE );

    private static final List<RelOptRule> DEFAULT_RULES =
            ImmutableList.of(
                    AggregateStarTableRule.INSTANCE,
                    AggregateStarTableRule.INSTANCE2,
                    TableScanRule.INSTANCE,
                    COMMUTE
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

    private static final List<RelOptRule> CONSTANT_REDUCTION_RULES =
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


    public ParseResult parse( Context context, String sql ) {
        return parse_( context, sql, false, false, false );
    }


    public ConvertResult convert( Context context, String sql ) {
        return (ConvertResult) parse_( context, sql, true, false, false );
    }


    public AnalyzeViewResult analyzeView( Context context, String sql, boolean fail ) {
        return (AnalyzeViewResult) parse_( context, sql, true, true, fail );
    }


    /**
     * Shared implementation for {@link #parse}, {@link #convert} and {@link #analyzeView}.
     */
    private ParseResult parse_( Context context, String sql, boolean convert, boolean analyze, boolean fail ) {
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        PolyphenyDbCatalogReader catalogReader =
                new PolyphenyDbCatalogReader(
                        context.getRootSchema(),
                        context.getDefaultSchemaPath(),
                        typeFactory );
        SqlParser parser = createParser( sql );
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseStmt();
        } catch ( SqlParseException e ) {
            throw new RuntimeException( "parse failed", e );
        }
        final SqlValidator validator = createSqlValidator( context, catalogReader );
        SqlNode sqlNode1 = validator.validate( sqlNode );
        if ( convert ) {
            return convert_( context, sql, analyze, fail, catalogReader, validator, sqlNode1 );
        }
        return new ParseResult( this, validator, sql, sqlNode1, validator.getValidatedNodeType( sqlNode1 ) );
    }


    private ParseResult convert_( Context context, String sql, boolean analyze, boolean fail, PolyphenyDbCatalogReader catalogReader, SqlValidator validator, SqlNode sqlNode1 ) {
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final Convention resultConvention =
                enableBindable
                        ? BindableConvention.INSTANCE
                        : EnumerableConvention.INSTANCE;
        final HepPlanner planner = new HepPlanner( new HepProgramBuilder().build() );
        planner.addRelTraitDef( ConventionTraitDef.INSTANCE );

        final SqlToRelConverter.ConfigBuilder configBuilder = SqlToRelConverter.configBuilder().withTrimUnusedFields( true );
        if ( analyze ) {
            configBuilder.withConvertTableAccess( false );
        }

        final PolyphenyDbPreparingStmt preparingStmt = new PolyphenyDbPreparingStmt( this, context, catalogReader, typeFactory, context.getRootSchema(), null, planner, resultConvention, createConvertletTable() );
        final SqlToRelConverter converter = preparingStmt.getSqlToRelConverter( validator, catalogReader, configBuilder.build() );

        final RelRoot root = converter.convertQuery( sqlNode1, false, true );
        if ( analyze ) {
            return analyze_( validator, sql, sqlNode1, root, fail );
        }
        return new ConvertResult( this, validator, sql, sqlNode1, validator.getValidatedNodeType( sqlNode1 ), root );
    }


    private AnalyzeViewResult analyze_( SqlValidator validator, String sql, SqlNode sqlNode, RelRoot root, boolean fail ) {
        final RexBuilder rexBuilder = root.rel.getCluster().getRexBuilder();
        RelNode rel = root.rel;
        final RelNode viewRel = rel;
        Project project;
        if ( rel instanceof Project ) {
            project = (Project) rel;
            rel = project.getInput();
        } else {
            project = null;
        }
        Filter filter;
        if ( rel instanceof Filter ) {
            filter = (Filter) rel;
            rel = filter.getInput();
        } else {
            filter = null;
        }
        TableScan scan;
        if ( rel instanceof TableScan ) {
            scan = (TableScan) rel;
        } else {
            scan = null;
        }
        if ( scan == null ) {
            if ( fail ) {
                throw validator.newValidationError( sqlNode, Static.RESOURCE.modifiableViewMustBeBasedOnSingleTable() );
            }
            return new AnalyzeViewResult( this, validator, sql, sqlNode, validator.getValidatedNodeType( sqlNode ), root, null, null, null, null, false );
        }
        final RelOptTable targetRelTable = scan.getTable();
        final RelDataType targetRowType = targetRelTable.getRowType();
        final Table table = targetRelTable.unwrap( Table.class );
        final List<String> tablePath = targetRelTable.getQualifiedName();
        assert table != null;
        List<Integer> columnMapping;
        final Map<Integer, RexNode> projectMap = new HashMap<>();
        if ( project == null ) {
            columnMapping = ImmutableIntList.range( 0, targetRowType.getFieldCount() );
        } else {
            columnMapping = new ArrayList<>();
            for ( Ord<RexNode> node : Ord.zip( project.getProjects() ) ) {
                if ( node.e instanceof RexInputRef ) {
                    RexInputRef rexInputRef = (RexInputRef) node.e;
                    int index = rexInputRef.getIndex();
                    if ( projectMap.get( index ) != null ) {
                        if ( fail ) {
                            throw validator.newValidationError( sqlNode, Static.RESOURCE.moreThanOneMappedColumn( targetRowType.getFieldList().get( index ).getName(), Util.last( tablePath ) ) );
                        }
                        return new AnalyzeViewResult( this, validator, sql, sqlNode, validator.getValidatedNodeType( sqlNode ), root, null, null, null, null, false );
                    }
                    projectMap.put( index, rexBuilder.makeInputRef( viewRel, node.i ) );
                    columnMapping.add( index );
                } else {
                    columnMapping.add( -1 );
                }
            }
        }
        final RexNode constraint;
        if ( filter != null ) {
            constraint = filter.getCondition();
        } else {
            constraint = rexBuilder.makeLiteral( true );
        }
        final List<RexNode> filters = new ArrayList<>();
        // If we put a constraint in projectMap above, then filters will not be empty despite being a modifiable view.
        final List<RexNode> filters2 = new ArrayList<>();
        boolean retry = false;
        RelOptUtil.inferViewPredicates( projectMap, filters, constraint );
        if ( fail && !filters.isEmpty() ) {
            final Map<Integer, RexNode> projectMap2 = new HashMap<>();
            RelOptUtil.inferViewPredicates( projectMap2, filters2, constraint );
            if ( !filters2.isEmpty() ) {
                throw validator.newValidationError( sqlNode, Static.RESOURCE.modifiableViewMustHaveOnlyEqualityPredicates() );
            }
            retry = true;
        }

        // Check that all columns that are not projected have a constant value
        for ( RelDataTypeField field : targetRowType.getFieldList() ) {
            final int x = columnMapping.indexOf( field.getIndex() );
            if ( x >= 0 ) {
                assert Util.skip( columnMapping, x + 1 ).indexOf( field.getIndex() ) < 0 : "column projected more than once; should have checked above";
                continue; // target column is projected
            }
            if ( projectMap.get( field.getIndex() ) != null ) {
                continue; // constant expression
            }
            if ( field.getType().isNullable() ) {
                continue; // don't need expression for nullable columns; NULL suffices
            }
            if ( fail ) {
                throw validator.newValidationError( sqlNode, Static.RESOURCE.noValueSuppliedForViewColumn( field.getName(), Util.last( tablePath ) ) );
            }
            return new AnalyzeViewResult(
                    this,
                    validator,
                    sql,
                    sqlNode,
                    validator.getValidatedNodeType( sqlNode ),
                    root,
                    null,
                    null,
                    null,
                    null,
                    false );
        }

        final boolean modifiable = filters.isEmpty() || retry && filters2.isEmpty();
        return new AnalyzeViewResult(
                this,
                validator,
                sql,
                sqlNode,
                validator.getValidatedNodeType( sqlNode ),
                root,
                modifiable ? table : null,
                ImmutableList.copyOf( tablePath ),
                constraint,
                ImmutableIntList.copyOf( columnMapping ),
                modifiable );
    }


    @Override
    public void executeDdl( Context context, SqlNode node ) {
        if ( node instanceof SqlExecutableStatement ) {
            SqlExecutableStatement statement = (SqlExecutableStatement) node;
            statement.execute( context, null );
            return;
        }
        throw new UnsupportedOperationException();
    }


    /**
     * Factory method for default SQL parser.
     */
    protected SqlParser createParser( String sql ) {
        return createParser( sql, createParserConfig() );
    }


    /**
     * Factory method for SQL parser with a given configuration.
     */
    protected SqlParser createParser( String sql, SqlParser.ConfigBuilder parserConfig ) {
        return SqlParser.create( sql, parserConfig.build() );
    }


    /**
     * Factory method for SQL parser configuration.
     */
    protected SqlParser.ConfigBuilder createParserConfig() {
        return SqlParser.configBuilder();
    }


    /**
     * Factory method for default convertlet table.
     */
    protected SqlRexConvertletTable createConvertletTable() {
        return StandardConvertletTable.INSTANCE;
    }


    /**
     * Factory method for cluster.
     */
    protected RelOptCluster createCluster( RelOptPlanner planner, RexBuilder rexBuilder ) {
        return RelOptCluster.create( planner, rexBuilder );
    }


    /**
     * Creates a collection of planner factories.
     *
     * The collection must have at least one factory, and each factory must create a planner. If the collection has more than one planner, Polypheny-DB will try each planner in turn.
     *
     * One of the things you can do with this mechanism is to try a simpler, faster, planner with a smaller rule set first, then fall back to a more complex planner for complex and costly queries.
     *
     * The default implementation returns a factory that calls {@link #createPlanner(Context)}.
     */
    protected List<Function1<Context, RelOptPlanner>> createPlannerFactories() {
        return Collections.singletonList( context -> createPlanner( context, null, null ) );
    }


    /**
     * Creates a query planner and initializes it with a default set of rules.
     */
    protected RelOptPlanner createPlanner( Context prepareContext ) {
        return createPlanner( prepareContext, null, null );
    }


    /**
     * Creates a query planner and initializes it with a default set of rules.
     */
    protected RelOptPlanner createPlanner( final Context prepareContext, ch.unibas.dmi.dbis.polyphenydb.plan.Context externalContext, RelOptCostFactory costFactory ) {
        if ( externalContext == null ) {
            externalContext = Contexts.of( prepareContext.config() );
        }
        final VolcanoPlanner planner = new VolcanoPlanner( costFactory, externalContext );
        planner.addRelTraitDef( ConventionTraitDef.INSTANCE );
        if ( ENABLE_COLLATION_TRAIT ) {
            planner.addRelTraitDef( RelCollationTraitDef.INSTANCE );
            planner.registerAbstractRelationalRules();
        }
        RelOptUtil.registerAbstractRels( planner );
        for ( RelOptRule rule : DEFAULT_RULES ) {
            planner.addRule( rule );
        }
        if ( prepareContext.config().materializationsEnabled() ) {
            planner.addRule( MaterializedViewFilterScanRule.INSTANCE );
            planner.addRule( AbstractMaterializedViewRule.INSTANCE_PROJECT_FILTER );
            planner.addRule( AbstractMaterializedViewRule.INSTANCE_FILTER );
            planner.addRule( AbstractMaterializedViewRule.INSTANCE_PROJECT_JOIN );
            planner.addRule( AbstractMaterializedViewRule.INSTANCE_JOIN );
            planner.addRule( AbstractMaterializedViewRule.INSTANCE_PROJECT_AGGREGATE );
            planner.addRule( AbstractMaterializedViewRule.INSTANCE_AGGREGATE );
        }
        if ( enableBindable ) {
            for ( RelOptRule rule : Bindables.RULES ) {
                planner.addRule( rule );
            }
        }
        planner.addRule( Bindables.BINDABLE_TABLE_SCAN_RULE );
        planner.addRule( ProjectTableScanRule.INSTANCE );
        planner.addRule( ProjectTableScanRule.INTERPRETER );

        if ( ENABLE_ENUMERABLE ) {
            for ( RelOptRule rule : ENUMERABLE_RULES ) {
                planner.addRule( rule );
            }
            planner.addRule( EnumerableInterpreterRule.INSTANCE );
        }

        if ( enableBindable && ENABLE_ENUMERABLE ) {
            planner.addRule( EnumerableBindable.EnumerableToBindableConverterRule.INSTANCE );
        }

        if ( ENABLE_STREAM ) {
            for ( RelOptRule rule : StreamRules.RULES ) {
                planner.addRule( rule );
            }
        }

        // Change the below to enable constant-reduction.
        if ( false ) {
            for ( RelOptRule rule : CONSTANT_REDUCTION_RULES ) {
                planner.addRule( rule );
            }
        }

        final SparkHandler spark = prepareContext.spark();
        if ( spark.enabled() ) {
            spark.registerRules(
                    new SparkHandler.RuleSetBuilder() {
                        public void addRule( RelOptRule rule ) {
                            // TODO:
                        }


                        public void removeRule( RelOptRule rule ) {
                            // TODO:
                        }
                    } );
        }

        Hook.PLANNER.run( planner ); // allow test to add or remove rules

        return planner;
    }


    public <T> PolyphenyDbSignature<T> prepareQueryable( Context context, Queryable<T> queryable ) {
        return prepare_( context, Query.of( queryable ), queryable.getElementType(), -1 );
    }


    public <T> PolyphenyDbSignature<T> prepareSql( Context context, Query<T> query, Type elementType, long maxRowCount ) {
        return prepare_( context, query, elementType, maxRowCount );
    }


    <T> PolyphenyDbSignature<T> prepare_( Context context, Query<T> query, Type elementType, long maxRowCount ) {
        if ( SIMPLE_SQLS.contains( query.sql ) ) {
            return simplePrepare( context, query.sql );
        }
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        PolyphenyDbCatalogReader catalogReader =
                new PolyphenyDbCatalogReader(
                        context.getRootSchema(),
                        context.getDefaultSchemaPath(),
                        typeFactory );
        final List<Function1<Context, RelOptPlanner>> plannerFactories = createPlannerFactories();
        if ( plannerFactories.isEmpty() ) {
            throw new AssertionError( "no planner factories" );
        }
        RuntimeException exception = Util.FoundOne.NULL;
        for ( Function1<Context, RelOptPlanner> plannerFactory : plannerFactories ) {
            final RelOptPlanner planner = plannerFactory.apply( context );
            if ( planner == null ) {
                throw new AssertionError( "factory returned null planner" );
            }
            try {
                return prepare2_( context, query, elementType, maxRowCount, catalogReader, planner );
            } catch ( RelOptPlanner.CannotPlanException e ) {
                exception = e;
            }
        }
        throw exception;
    }


    /**
     * Quickly prepares a simple SQL statement, circumventing the usual preparation process.
     */
    private <T> PolyphenyDbSignature<T> simplePrepare( Context context, String sql ) {
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final RelDataType x =
                typeFactory.builder()
                        .add( SqlUtil.deriveAliasFromOrdinal( 0 ), SqlTypeName.INTEGER )
                        .build();
        @SuppressWarnings("unchecked") final List<T> list = (List) ImmutableList.of( 1 );
        final List<String> origin = null;
        final List<List<String>> origins = Collections.nCopies( x.getFieldCount(), origin );
        final List<ColumnMetaData> columns = getColumnMetaDataList( typeFactory, x, x, origins );
        final Meta.CursorFactory cursorFactory = Meta.CursorFactory.deduce( columns, null );
        return new PolyphenyDbSignature<>(
                sql,
                ImmutableList.of(),
                ImmutableMap.of(),
                x,
                columns,
                cursorFactory,
                context.getRootSchema(),
                ImmutableList.of(),
                -1,
                dataContext -> Linq4j.asEnumerable( list ),
                Meta.StatementType.SELECT );
    }


    /**
     * Deduces the broad type of statement. Currently returns SELECT for most statement types, but this may change.
     *
     * @param kind Kind of statement
     */
    private Meta.StatementType getStatementType( SqlKind kind ) {
        switch ( kind ) {
            case INSERT:
            case DELETE:
            case UPDATE:
                return Meta.StatementType.IS_DML;
            default:
                return Meta.StatementType.SELECT;
        }
    }


    /**
     * Deduces the broad type of statement for a prepare result.
     * Currently returns SELECT for most statement types, but this may change.
     *
     * @param preparedResult Prepare result
     */
    private Meta.StatementType getStatementType( Prepare.PreparedResult preparedResult ) {
        if ( preparedResult.isDml() ) {
            return Meta.StatementType.IS_DML;
        } else {
            return Meta.StatementType.SELECT;
        }
    }


    <T> PolyphenyDbSignature<T> prepare2_( Context context, Query<T> query, Type elementType, long maxRowCount, PolyphenyDbCatalogReader catalogReader, RelOptPlanner planner ) {
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final Prefer prefer;
        if ( elementType == Object[].class ) {
            prefer = EnumerableRel.Prefer.ARRAY;
        } else {
            prefer = EnumerableRel.Prefer.CUSTOM;
        }
        final Convention resultConvention =
                enableBindable
                        ? BindableConvention.INSTANCE
                        : EnumerableConvention.INSTANCE;
        final PolyphenyDbPreparingStmt preparingStmt = new PolyphenyDbPreparingStmt( this, context, catalogReader, typeFactory, context.getRootSchema(), prefer, planner, resultConvention, createConvertletTable() );

        final RelDataType x;
        final Prepare.PreparedResult preparedResult;
        final Meta.StatementType statementType;
        if ( query.sql != null ) {
            final PolyphenyDbConnectionConfig config = context.config();
            final SqlParser.ConfigBuilder parserConfig = createParserConfig()
                    .setQuotedCasing( config.quotedCasing() )
                    .setUnquotedCasing( config.unquotedCasing() )
                    .setQuoting( config.quoting() )
                    .setConformance( config.conformance() )
                    .setCaseSensitive( RuntimeConfig.CASE_SENSITIVE.getBoolean() );
            final SqlParserImplFactory parserFactory = config.parserFactory( SqlParserImplFactory.class, null );
            if ( parserFactory != null ) {
                parserConfig.setParserFactory( parserFactory );
            }
            SqlParser parser = createParser( query.sql, parserConfig );
            SqlNode sqlNode;
            try {
                sqlNode = parser.parseStmt();
                statementType = getStatementType( sqlNode.getKind() );
            } catch ( SqlParseException e ) {
                throw new RuntimeException( "parse failed: " + e.getMessage(), e );
            }

            Hook.PARSE_TREE.run( new Object[]{ query.sql, sqlNode } );

            if ( sqlNode.getKind().belongsTo( SqlKind.DDL ) ) {
                executeDdl( context, sqlNode );

                return new PolyphenyDbSignature<>(
                        query.sql,
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        null,
                        ImmutableList.of(),
                        Meta.CursorFactory.OBJECT,
                        null,
                        ImmutableList.of(),
                        -1,
                        null,
                        Meta.StatementType.OTHER_DDL );
            }

            final SqlValidator validator = createSqlValidator( context, catalogReader );
            validator.setIdentifierExpansion( true );
            validator.setDefaultNullCollation( config.defaultNullCollation() );

            preparedResult = preparingStmt.prepareSql( sqlNode, Object.class, validator, true );
            switch ( sqlNode.getKind() ) {
                case INSERT:
                case DELETE:
                case UPDATE:
                case EXPLAIN:
                    // FIXME: getValidatedNodeType is wrong for DML
                    x = RelOptUtil.createDmlRowType( sqlNode.getKind(), typeFactory );
                    break;
                default:
                    x = validator.getValidatedNodeType( sqlNode );
            }
        } else if ( query.queryable != null ) {
            x = context.getTypeFactory().createType( elementType );
            preparedResult = preparingStmt.prepareQueryable( query.queryable, x );
            statementType = getStatementType( preparedResult );
        } else {
            assert query.rel != null;
            x = query.rel.getRowType();
            preparedResult = preparingStmt.prepareRel( query.rel );
            statementType = getStatementType( preparedResult );
        }

        final List<AvaticaParameter> parameters = new ArrayList<>();
        final RelDataType parameterRowType = preparedResult.getParameterRowType();
        for ( RelDataTypeField field : parameterRowType.getFieldList() ) {
            RelDataType type = field.getType();
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

        RelDataType jdbcType = makeStruct( typeFactory, x );
        final List<List<String>> originList = preparedResult.getFieldOrigins();
        final List<ColumnMetaData> columns = getColumnMetaDataList( typeFactory, x, jdbcType, originList );
        Class resultClazz = null;
        if ( preparedResult instanceof Typed ) {
            resultClazz = (Class) ((Typed) preparedResult).getElementType();
        }
        final Meta.CursorFactory cursorFactory =
                preparingStmt.resultConvention == BindableConvention.INSTANCE
                        ? Meta.CursorFactory.ARRAY
                        : Meta.CursorFactory.deduce( columns, resultClazz );
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
                preparedResult instanceof Prepare.PreparedResultImpl
                        ? ((Prepare.PreparedResultImpl) preparedResult).collations
                        : ImmutableList.of(),
                maxRowCount,
                bindable,
                statementType );
    }


    private SqlValidator createSqlValidator( Context context, PolyphenyDbCatalogReader catalogReader ) {
        final SqlOperatorTable opTab0 = context.config().fun( SqlOperatorTable.class, SqlStdOperatorTable.instance() );
        final SqlOperatorTable opTab = ChainedSqlOperatorTable.of( opTab0, catalogReader );
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final SqlConformance conformance = context.config().conformance();
        return new PolyphenyDbSqlValidator( opTab, catalogReader, typeFactory, conformance );
    }


    private List<ColumnMetaData> getColumnMetaDataList( JavaTypeFactory typeFactory, RelDataType x, RelDataType jdbcType, List<List<String>> originList ) {
        final List<ColumnMetaData> columns = new ArrayList<>();
        for ( Ord<RelDataTypeField> pair : Ord.zip( jdbcType.getFieldList() ) ) {
            final RelDataTypeField field = pair.e;
            final RelDataType type = field.getType();
            final RelDataType fieldType = x.isStruct() ? x.getFieldList().get( pair.i ).getType() : type;
            columns.add( metaData( typeFactory, columns.size(), field.getName(), type, fieldType, originList.get( pair.i ) ) );
        }
        return columns;
    }


    private ColumnMetaData metaData( JavaTypeFactory typeFactory, int ordinal, String fieldName, RelDataType type, RelDataType fieldType, List<String> origins ) {
        final ColumnMetaData.AvaticaType avaticaType = avaticaType( typeFactory, type, fieldType );
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


    private ColumnMetaData.AvaticaType avaticaType( JavaTypeFactory typeFactory, RelDataType type, RelDataType fieldType ) {
        final String typeName = getTypeName( type );
        if ( type.getComponentType() != null ) {
            final ColumnMetaData.AvaticaType componentType = avaticaType( typeFactory, type.getComponentType(), null );
            final Type clazz = typeFactory.getJavaClass( type.getComponentType() );
            final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of( clazz );
            assert rep != null;
            return ColumnMetaData.array( componentType, typeName, rep );
        } else {
            int typeOrdinal = getTypeOrdinal( type );
            switch ( typeOrdinal ) {
                case Types.STRUCT:
                    final List<ColumnMetaData> columns = new ArrayList<>();
                    for ( RelDataTypeField field : type.getFieldList() ) {
                        columns.add( metaData( typeFactory, field.getIndex(), field.getName(), field.getType(), null, null ) );
                    }
                    return ColumnMetaData.struct( columns );
                case ExtraSqlTypes.GEOMETRY:
                    typeOrdinal = Types.VARCHAR;
                    // fall through
                default:
                    final Type clazz = typeFactory.getJavaClass( Util.first( fieldType, type ) );
                    final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of( clazz );
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


    private int getTypeOrdinal( RelDataType type ) {
        return type.getSqlTypeName().getJdbcOrdinal();
    }


    private static String getClassName( RelDataType type ) {
        return Object.class.getName(); // POLYPHENYDB-2613
    }


    private static int getScale( RelDataType type ) {
        return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
                ? 0
                : type.getScale();
    }


    private static int getPrecision( RelDataType type ) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
                ? 0
                : type.getPrecision();
    }


    /**
     * Returns the type name in string form. Does not include precision, scale or whether nulls are allowed.
     * Example: "DECIMAL" not "DECIMAL(7, 2)"; "INTEGER" not "JavaType(int)".
     */
    private static String getTypeName( RelDataType type ) {
        final SqlTypeName sqlTypeName = type.getSqlTypeName();
        switch ( sqlTypeName ) {
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
                return type.toString(); // e.g. "INTEGER ARRAY"
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
                return sqlTypeName.getName(); // e.g. "DECIMAL", "INTERVAL_YEAR_MONTH"
        }
    }


    protected void populateMaterializations( Context context, RelOptPlanner planner, Prepare.Materialization materialization ) {
        // REVIEW: initialize queryRel and tableRel inside MaterializationService, not here?
        try {
            final PolyphenyDbSchema schema = materialization.materializedTable.schema;
            PolyphenyDbCatalogReader catalogReader = new PolyphenyDbCatalogReader( schema.root(), materialization.viewSchemaPath, context.getTypeFactory() );
            final PolyphenyDbMaterializer materializer = new PolyphenyDbMaterializer( this, context, catalogReader, schema, planner, createConvertletTable() );
            materializer.populate( materialization );
        } catch ( Exception e ) {
            throw new RuntimeException( "While populating materialization " + materialization.materializedTable.path(), e );
        }
    }


    private static RelDataType makeStruct( RelDataTypeFactory typeFactory, RelDataType type ) {
        if ( type.isStruct() ) {
            return type;
        }
        return typeFactory.builder().add( "$0", type ).build();
    }


    /**
     * Executes a prepare action.
     */
    public <R> R perform( Frameworks.PrepareAction<R> action ) {
        final Context prepareContext = action.getConfig().getPrepareContext();
        final JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
        final PolyphenyDbSchema schema =
                action.getConfig().getDefaultSchema() != null
                        ? PolyphenyDbSchema.from( action.getConfig().getDefaultSchema() )
                        : prepareContext.getRootSchema();
        PolyphenyDbCatalogReader catalogReader = new PolyphenyDbCatalogReader( schema.root(), schema.path( null ), typeFactory );
        final RexBuilder rexBuilder = new RexBuilder( typeFactory );
        final RelOptPlanner planner = createPlanner( prepareContext, action.getConfig().getContext(), action.getConfig().getCostFactory() );
        final RelOptCluster cluster = createCluster( planner, rexBuilder );
        return action.apply( cluster, catalogReader, prepareContext.getRootSchema().plus() );
    }


    /**
     * Holds state for the process of preparing a SQL statement.
     */
    static class PolyphenyDbPreparingStmt extends Prepare implements RelOptTable.ViewExpander {

        protected final RelOptPlanner planner;
        protected final RexBuilder rexBuilder;
        protected final PolyphenyDbPrepareImpl prepare;
        protected final PolyphenyDbSchema schema;
        protected final RelDataTypeFactory typeFactory;
        protected final SqlRexConvertletTable convertletTable;
        private final EnumerableRel.Prefer prefer;
        private final Map<String, Object> internalParameters = new LinkedHashMap<>();
        private int expansionDepth;
        private SqlValidator sqlValidator;


        PolyphenyDbPreparingStmt( PolyphenyDbPrepareImpl prepare, Context context, CatalogReader catalogReader, RelDataTypeFactory typeFactory, PolyphenyDbSchema schema, EnumerableRel.Prefer prefer, RelOptPlanner planner, Convention resultConvention, SqlRexConvertletTable convertletTable ) {
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


        public PreparedResult prepareQueryable( final Queryable queryable, RelDataType resultType ) {
            return prepare_( () -> {
                final RelOptCluster cluster = prepare.createCluster( planner, rexBuilder );
                return new LixToRelTranslator( cluster, PolyphenyDbPreparingStmt.this ).translate( queryable );
            }, resultType );
        }


        public PreparedResult prepareRel( final RelNode rel ) {
            return prepare_( () -> rel, rel.getRowType() );
        }


        private PreparedResult prepare_( Supplier<RelNode> fn, RelDataType resultType ) {
            Class runtimeContextClass = Object.class;
            init( runtimeContextClass );

            final RelNode rel = fn.get();
            final RelDataType rowType = rel.getRowType();
            final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
            final RelCollation collation =
                    rel instanceof Sort
                            ? ((Sort) rel).collation
                            : RelCollations.EMPTY;
            RelRoot root = new RelRoot( rel, resultType, SqlKind.SELECT, fields, collation );

            if ( timingTracer != null ) {
                timingTracer.traceTime( "end sql2rel" );
            }

            final RelDataType jdbcType = makeStruct( rexBuilder.getTypeFactory(), resultType );
            fieldOrigins = Collections.nCopies( jdbcType.getFieldCount(), null );
            parameterRowType = rexBuilder.getTypeFactory().builder().build();

            // Structured type flattening, view expansion, and plugging in physical storage.
            root = root.withRel( flattenTypes( root.rel, true ) );

            // Trim unused fields.
            root = trimUnusedFields( root );

            final List<Materialization> materializations = ImmutableList.of();
            final List<PolyphenyDbSchema.LatticeEntry> lattices = ImmutableList.of();
            root = optimize( root, materializations, lattices );

            if ( timingTracer != null ) {
                timingTracer.traceTime( "end optimization" );
            }

            return implement( root );
        }


        @Override
        protected SqlToRelConverter getSqlToRelConverter( SqlValidator validator, CatalogReader catalogReader, SqlToRelConverter.Config config ) {
            final RelOptCluster cluster = prepare.createCluster( planner, rexBuilder );
            return new SqlToRelConverter( this, validator, catalogReader, cluster, convertletTable, config );
        }


        @Override
        public RelNode flattenTypes( RelNode rootRel, boolean restructure ) {
            final SparkHandler spark = context.spark();
            if ( spark.enabled() ) {
                return spark.flattenTypes( planner, rootRel, restructure );
            }
            return rootRel;
        }


        @Override
        protected RelNode decorrelate( SqlToRelConverter sqlToRelConverter, SqlNode query, RelNode rootRel ) {
            return sqlToRelConverter.decorrelate( query, rootRel );
        }


        @Override
        public RelRoot expandView( RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath ) {
            expansionDepth++;

            SqlParser parser = prepare.createParser( queryString );
            SqlNode sqlNode;
            try {
                sqlNode = parser.parseQuery();
            } catch ( SqlParseException e ) {
                throw new RuntimeException( "parse failed", e );
            }
            // View may have different schema path than current connection.
            final CatalogReader catalogReader = this.catalogReader.withSchemaPath( schemaPath );
            SqlValidator validator = createSqlValidator( catalogReader );
            final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder().withTrimUnusedFields( true ).build();
            SqlToRelConverter sqlToRelConverter = getSqlToRelConverter( validator, catalogReader, config );
            RelRoot root = sqlToRelConverter.convertQuery( sqlNode, true, false );

            --expansionDepth;
            return root;
        }


        protected SqlValidator createSqlValidator( CatalogReader catalogReader ) {
            return prepare.createSqlValidator( context, (PolyphenyDbCatalogReader) catalogReader );
        }


        @Override
        protected SqlValidator getSqlValidator() {
            if ( sqlValidator == null ) {
                sqlValidator = createSqlValidator( catalogReader );
            }
            return sqlValidator;
        }


        @Override
        protected PreparedResult createPreparedExplanation( RelDataType resultType, RelDataType parameterRowType, RelRoot root, SqlExplainFormat format, SqlExplainLevel detailLevel ) {
            return new PolyphenyDbPreparedExplain( resultType, parameterRowType, root, format, detailLevel );
        }


        @Override
        protected PreparedResult implement( RelRoot root ) {
            RelDataType resultType = root.rel.getRowType();
            boolean isDml = root.kind.belongsTo( SqlKind.DML );
            final Bindable bindable;
            if ( resultConvention == BindableConvention.INSTANCE ) {
                bindable = Interpreters.bindable( root.rel );
            } else {
                EnumerableRel enumerable = (EnumerableRel) root.rel;
                if ( !root.isRefTrivial() ) {
                    final List<RexNode> projects = new ArrayList<>();
                    final RexBuilder rexBuilder = enumerable.getCluster().getRexBuilder();
                    for ( int field : Pair.left( root.fields ) ) {
                        projects.add( rexBuilder.makeInputRef( enumerable, field ) );
                    }
                    RexProgram program = RexProgram.create( enumerable.getRowType(), projects, null, root.validatedRowType, rexBuilder );
                    enumerable = EnumerableCalc.create( enumerable, program );
                }

                try {
                    CatalogReader.THREAD_LOCAL.set( catalogReader );
                    final SqlConformance conformance = context.config().conformance();
                    internalParameters.put( "_conformance", conformance );
                    bindable = EnumerableInterpretable.toBindable( internalParameters, context.spark(), enumerable, prefer );
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
                    root.rel,
                    mapTableModOp( isDml, root.kind ),
                    isDml ) {
                public String getCode() {
                    throw new UnsupportedOperationException();
                }


                public Bindable getBindable( Meta.CursorFactory cursorFactory ) {
                    return bindable;
                }


                public Type getElementType() {
                    return ((Typed) bindable).getElementType();
                }
            };
        }


        @Override
        protected List<Materialization> getMaterializations() {
            final List<Prepare.Materialization> materializations =
                    context.config().materializationsEnabled()
                            ? MaterializationService.instance().query( schema )
                            : ImmutableList.of();
            for ( Prepare.Materialization materialization : materializations ) {
                prepare.populateMaterializations( context, planner, materialization );
            }
            return materializations;
        }


        @Override
        protected List<LatticeEntry> getLattices() {
            return Schemas.getLatticeEntries( schema );
        }
    }


    /**
     * An {@code EXPLAIN} statement, prepared and ready to execute.
     */
    private static class PolyphenyDbPreparedExplain extends Prepare.PreparedExplain {

        PolyphenyDbPreparedExplain( RelDataType resultType, RelDataType parameterRowType, RelRoot root, SqlExplainFormat format, SqlExplainLevel detailLevel ) {
            super( resultType, parameterRowType, root, format, detailLevel );
        }


        public Bindable getBindable( final Meta.CursorFactory cursorFactory ) {
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


        public List<RexNode> toRexList( BlockStatement statement ) {
            final List<Expression> simpleList = simpleList( statement );
            final List<RexNode> list = new ArrayList<>();
            for ( Expression expression1 : simpleList ) {
                list.add( toRex( expression1 ) );
            }
            return list;
        }


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


        public RexNode toRex( Expression expression ) {
            switch ( expression.getNodeType() ) {
                case MemberAccess:
                    // Case-sensitive name match because name was previously resolved.
                    return rexBuilder.makeFieldAccess(
                            toRex( ((MemberExpression) expression).expression ),
                            ((MemberExpression) expression).field.getName(),
                            true );
                case GreaterThan:
                    return binary( expression, SqlStdOperatorTable.GREATER_THAN );
                case LessThan:
                    return binary( expression, SqlStdOperatorTable.LESS_THAN );
                case Parameter:
                    return parameter( (ParameterExpression) expression );
                case Call:
                    MethodCallExpression call = (MethodCallExpression) expression;
                    SqlOperator operator = RexToLixTranslator.JAVA_TO_SQL_METHOD_MAP.get( call.method );
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


        private RexNode binary( Expression expression, SqlBinaryOperator op ) {
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


        protected RelDataType type( Expression expression ) {
            final Type type = expression.getType();
            return ((JavaTypeFactory) rexBuilder.getTypeFactory()).createType( type );
        }


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


        public RexNode parameter( ParameterExpression param ) {
            int i = parameterList.indexOf( param );
            if ( i >= 0 ) {
                return values.get( i );
            }
            throw new RuntimeException( "unknown parameter " + param );
        }
    }
}

