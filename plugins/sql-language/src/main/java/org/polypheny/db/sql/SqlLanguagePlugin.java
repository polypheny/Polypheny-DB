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

package org.polypheny.db.sql;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.json.JsonConstructorNullClause;
import org.polypheny.db.algebra.operators.ChainedOperatorTable;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.PolyphenyDbConnectionProperty;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.sql.parser.impl.SqlParserImpl;
import org.polypheny.db.nodes.LangFunctionOperator;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.sql.language.SqlAsOperator;
import org.polypheny.db.sql.language.SqlBinaryOperator;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlFilterOperator;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlGroupedWindowFunction;
import org.polypheny.db.sql.language.SqlInternalOperator;
import org.polypheny.db.sql.language.SqlLateralOperator;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNumericLiteral;
import org.polypheny.db.sql.language.SqlOverOperator;
import org.polypheny.db.sql.language.SqlPostfixOperator;
import org.polypheny.db.sql.language.SqlPrefixOperator;
import org.polypheny.db.sql.language.SqlProcedureCallOperator;
import org.polypheny.db.sql.language.SqlRankFunction;
import org.polypheny.db.sql.language.SqlSetOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlUnnestOperator;
import org.polypheny.db.sql.language.SqlUtil;
import org.polypheny.db.sql.language.SqlValuesOperator;
import org.polypheny.db.sql.language.SqlWithinGroupOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.fun.OracleSqlOperatorTable;
import org.polypheny.db.sql.language.fun.SqlAbstractTimeFunction;
import org.polypheny.db.sql.language.fun.SqlAnyValueAggFunction;
import org.polypheny.db.sql.language.fun.SqlArgumentAssignmentOperator;
import org.polypheny.db.sql.language.fun.SqlArrayQueryConstructor;
import org.polypheny.db.sql.language.fun.SqlArrayValueConstructor;
import org.polypheny.db.sql.language.fun.SqlAvgAggFunction;
import org.polypheny.db.sql.language.fun.SqlBaseContextVariable;
import org.polypheny.db.sql.language.fun.SqlBetweenOperator;
import org.polypheny.db.sql.language.fun.SqlBitOpAggFunction;
import org.polypheny.db.sql.language.fun.SqlCaseOperator;
import org.polypheny.db.sql.language.fun.SqlCastFunction;
import org.polypheny.db.sql.language.fun.SqlCoalesceFunction;
import org.polypheny.db.sql.language.fun.SqlCollectionTableOperator;
import org.polypheny.db.sql.language.fun.SqlColumnListConstructor;
import org.polypheny.db.sql.language.fun.SqlConvertFunction;
import org.polypheny.db.sql.language.fun.SqlCountAggFunction;
import org.polypheny.db.sql.language.fun.SqlCovarAggFunction;
import org.polypheny.db.sql.language.fun.SqlCurrentDateFunction;
import org.polypheny.db.sql.language.fun.SqlCursorConstructor;
import org.polypheny.db.sql.language.fun.SqlDatePartFunction;
import org.polypheny.db.sql.language.fun.SqlDatetimePlusOperator;
import org.polypheny.db.sql.language.fun.SqlDatetimeSubtractionOperator;
import org.polypheny.db.sql.language.fun.SqlDefaultOperator;
import org.polypheny.db.sql.language.fun.SqlDistanceFunction;
import org.polypheny.db.sql.language.fun.SqlDotOperator;
import org.polypheny.db.sql.language.fun.SqlExtendOperator;
import org.polypheny.db.sql.language.fun.SqlExtractFunction;
import org.polypheny.db.sql.language.fun.SqlFirstLastValueAggFunction;
import org.polypheny.db.sql.language.fun.SqlFloorFunction;
import org.polypheny.db.sql.language.fun.SqlGroupIdFunction;
import org.polypheny.db.sql.language.fun.SqlGroupingFunction;
import org.polypheny.db.sql.language.fun.SqlHistogramAggFunction;
import org.polypheny.db.sql.language.fun.SqlInOperator;
import org.polypheny.db.sql.language.fun.SqlItemOperator;
import org.polypheny.db.sql.language.fun.SqlJsonApiCommonSyntaxOperator;
import org.polypheny.db.sql.language.fun.SqlJsonArrayAggAggFunction;
import org.polypheny.db.sql.language.fun.SqlJsonArrayFunction;
import org.polypheny.db.sql.language.fun.SqlJsonExistsFunction;
import org.polypheny.db.sql.language.fun.SqlJsonObjectAggAggFunction;
import org.polypheny.db.sql.language.fun.SqlJsonObjectFunction;
import org.polypheny.db.sql.language.fun.SqlJsonQueryFunction;
import org.polypheny.db.sql.language.fun.SqlJsonValueExpressionOperator;
import org.polypheny.db.sql.language.fun.SqlJsonValueFunction;
import org.polypheny.db.sql.language.fun.SqlLeadLagAggFunction;
import org.polypheny.db.sql.language.fun.SqlLikeOperator;
import org.polypheny.db.sql.language.fun.SqlLiteralChainOperator;
import org.polypheny.db.sql.language.fun.SqlMapQueryConstructor;
import org.polypheny.db.sql.language.fun.SqlMapValueConstructor;
import org.polypheny.db.sql.language.fun.SqlMetaFunction;
import org.polypheny.db.sql.language.fun.SqlMinMaxAggFunction;
import org.polypheny.db.sql.language.fun.SqlMonotonicBinaryOperator;
import org.polypheny.db.sql.language.fun.SqlMonotonicUnaryFunction;
import org.polypheny.db.sql.language.fun.SqlMultisetMemberOfOperator;
import org.polypheny.db.sql.language.fun.SqlMultisetQueryConstructor;
import org.polypheny.db.sql.language.fun.SqlMultisetSetOperator;
import org.polypheny.db.sql.language.fun.SqlMultisetValueConstructor;
import org.polypheny.db.sql.language.fun.SqlNewOperator;
import org.polypheny.db.sql.language.fun.SqlNthValueAggFunction;
import org.polypheny.db.sql.language.fun.SqlNtileAggFunction;
import org.polypheny.db.sql.language.fun.SqlNullifFunction;
import org.polypheny.db.sql.language.fun.SqlOverlapsOperator;
import org.polypheny.db.sql.language.fun.SqlOverlayFunction;
import org.polypheny.db.sql.language.fun.SqlPositionFunction;
import org.polypheny.db.sql.language.fun.SqlQuantifyOperator;
import org.polypheny.db.sql.language.fun.SqlRandFunction;
import org.polypheny.db.sql.language.fun.SqlRandIntegerFunction;
import org.polypheny.db.sql.language.fun.SqlRegrCountAggFunction;
import org.polypheny.db.sql.language.fun.SqlRollupOperator;
import org.polypheny.db.sql.language.fun.SqlRowOperator;
import org.polypheny.db.sql.language.fun.SqlSequenceValueOperator;
import org.polypheny.db.sql.language.fun.SqlSingleValueAggFunction;
import org.polypheny.db.sql.language.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.language.fun.SqlStringContextVariable;
import org.polypheny.db.sql.language.fun.SqlSubstringFunction;
import org.polypheny.db.sql.language.fun.SqlSumAggFunction;
import org.polypheny.db.sql.language.fun.SqlSumEmptyIsZeroAggFunction;
import org.polypheny.db.sql.language.fun.SqlThrowOperator;
import org.polypheny.db.sql.language.fun.SqlTimestampAddFunction;
import org.polypheny.db.sql.language.fun.SqlTimestampDiffFunction;
import org.polypheny.db.sql.language.fun.SqlTranslate3Function;
import org.polypheny.db.sql.language.fun.SqlTrimFunction;
import org.polypheny.db.sql.language.validate.PolyphenyDbSqlValidator;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Optionality;
import org.polypheny.db.util.temporal.TimeUnit;
import org.polypheny.db.webui.crud.LanguageCrud;

@Slf4j
public class SqlLanguagePlugin extends PolyPlugin {

    @Getter
    @VisibleForTesting
    private static boolean isInit = false;


    /*
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public SqlLanguagePlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void start() {
        startup();
    }


    @Override
    public void stop() {
        throw new GenericRuntimeException( "Cannot remove language SQL." );
    }


    public static void startup() {
        // add language to general processing
        QueryLanguage language = new QueryLanguage(
                DataModel.RELATIONAL,
                "sql",
                List.of( "sql" ),
                SqlParserImpl.FACTORY,
                SqlProcessor::new,
                SqlLanguagePlugin::getValidator,
                LanguageManager::toQueryNodes,
                SqlLanguagePlugin::removeLimit );
        LanguageManager.getINSTANCE().addQueryLanguage( language );
        PolyPluginManager.AFTER_INIT.add( () -> {
            // add language to webui
            LanguageCrud.addToResult( language, LanguageCrud::getRelResult );
        } );

        if ( !isInit() ) {
            registerOperators();
        }
    }


    private static QueryContext removeLimit( QueryContext queryContext ) {
        String lowercase = queryContext.getQuery().toLowerCase();
        if ( !lowercase.contains( "limit" ) ) {
            return queryContext;
        }

        // ends with "LIMIT <number>" or "LIMIT <number>;" with optional whitespace, matches <number>
        Pattern pattern = Pattern.compile( "LIMIT\\s+(\\d+)(?:,(\\d+))?\\s*((?:;\\s*\\z|$)|OFFSET\\s*\\d+;$)", Pattern.CASE_INSENSITIVE );
        String limitClause = null;
        Matcher matcher = pattern.matcher( lowercase );
        if ( matcher.find() && matcher.groupCount() > 0 ) {
            limitClause = matcher.group( 1 );
        }
        if ( limitClause == null ) {
            return queryContext;
        }
        try {
            int limit = Integer.parseInt( limitClause.trim() );
            return queryContext.toBuilder().query( queryContext.getQuery() ).batch( limit ).build();
        } catch ( NumberFormatException e ) {
            log.error( "Could not parse limit clause: {}", limitClause );
            return queryContext;
        }
    }


    public static PolyphenyDbSqlValidator getValidator( org.polypheny.db.prepare.Context context, Snapshot snapshot ) {

        final OperatorTable opTab0 = fun( OperatorTable.class, SqlStdOperatorTable.instance() );
        final OperatorTable opTab = ChainedOperatorTable.of( opTab0, snapshot );
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final Conformance conformance = context.config().conformance();
        return new PolyphenyDbSqlValidator( opTab, snapshot, typeFactory, conformance );
    }


    public static <T> T fun( Class<T> operatorTableClass, T defaultOperatorTable ) {
        final String fun = PolyphenyDbConnectionProperty.FUN.wrap( new Properties() ).getString();
        if ( fun == null || fun.isEmpty() || fun.equals( "standard" ) ) {
            return defaultOperatorTable;
        }
        final Collection<OperatorTable> tables = new LinkedHashSet<>();
        for ( String s : fun.split( "," ) ) {
            operatorTable( s, tables );
        }
        tables.add( SqlStdOperatorTable.instance() );
        return operatorTableClass.cast( ChainedOperatorTable.of( tables.toArray( new OperatorTable[0] ) ) );
    }


    public static void operatorTable( String s, Collection<OperatorTable> tables ) {
        switch ( s ) {
            case "standard":
                tables.add( SqlStdOperatorTable.instance() );
                return;
            case "oracle":
                tables.add( OracleSqlOperatorTable.instance() );
                return;
            default:
                throw new IllegalArgumentException( "Unknown operator table: " + s );
        }
    }


    public static void registerOperators() {
        if ( isInit ) {
            throw new GenericRuntimeException( "Sql operators were already registered." );
        }

        register( OperatorName.ORACLE_TRANSLATE3, new SqlTranslate3Function() );

        //-------------------------------------------------------------
        //                   SET OPERATORS
        //-------------------------------------------------------------
        // The set operators can be compared to the arithmetic operators
        // UNION -> +
        // EXCEPT -> -
        // INTERSECT -> *
        // which explains the different precedence values
        register( OperatorName.UNION, new SqlSetOperator( "UNION", Kind.UNION, 14, false ) );

        register( OperatorName.UNION_ALL, new SqlSetOperator( "UNION ALL", Kind.UNION, 14, true ) );

        register( OperatorName.EXCEPT, new SqlSetOperator( "EXCEPT", Kind.EXCEPT, 14, false ) );

        register( OperatorName.EXCEPT_ALL, new SqlSetOperator( "EXCEPT ALL", Kind.EXCEPT, 14, true ) );

        register( OperatorName.INTERSECT, new SqlSetOperator( "INTERSECT", Kind.INTERSECT, 18, false ) );

        register( OperatorName.INTERSECT_ALL, new SqlSetOperator( "INTERSECT ALL", Kind.INTERSECT, 18, true ) );

        /*
         * The {@code MULTISET UNION DISTINCT} operator.
         */
        register( OperatorName.MULTISET_UNION_DISTINCT, new SqlMultisetSetOperator( "MULTISET UNION DISTINCT", 14, false ) );

        /*
         * The {@code MULTISET UNION [ALL]} operator.
         */
        register( OperatorName.MULTISET_UNION, new SqlMultisetSetOperator( "MULTISET UNION ALL", 14, true ) );

        /*
         * The {@code MULTISET EXCEPT DISTINCT} operator.
         */
        register( OperatorName.MULTISET_EXCEPT_DISTINCT, new SqlMultisetSetOperator( "MULTISET EXCEPT DISTINCT", 14, false ) );

        /*
         * The {@code MULTISET EXCEPT [ALL]} operator.
         */
        register( OperatorName.MULTISET_EXCEPT, new SqlMultisetSetOperator( "MULTISET EXCEPT ALL", 14, true ) );

        /*
         * The {@code MULTISET INTERSECT DISTINCT} operator.
         */
        register( OperatorName.MULTISET_INTERSECT_DISTINCT, new SqlMultisetSetOperator( "MULTISET INTERSECT DISTINCT", 18, false ) );

        /*
         * The {@code MULTISET INTERSECT [ALL]} operator.
         */
        register( OperatorName.MULTISET_INTERSECT, new SqlMultisetSetOperator( "MULTISET INTERSECT ALL", 18, true ) );

        //-------------------------------------------------------------
        //                   BINARY OPERATORS
        //-------------------------------------------------------------

        /*
         * Logical <code>AND</code> operator.
         */
        register( OperatorName.AND, new SqlBinaryOperator(
                "AND",
                Kind.AND,
                24,
                true,
                ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
                InferTypes.BOOLEAN,
                OperandTypes.BOOLEAN_BOOLEAN ) );

        /*
         * <code>AS</code> operator associates an expression in the SELECT clause with an alias.
         */
        register( OperatorName.AS, new SqlAsOperator() );

        /*
         * <code>ARGUMENT_ASSIGNMENT</code> operator (<code>=&lt;</code>) assigns an argument to a function call to a particular named parameter.
         */
        register( OperatorName.ARGUMENT_ASSIGNMENT, new SqlArgumentAssignmentOperator() );

        /*
         * <code>DEFAULT</code> operator indicates that an argument to a function call is to take its default value..
         */
        register( OperatorName.DEFAULT, new SqlDefaultOperator() );

        /*
         * <code>FILTER</code> operator filters which rows are included in an aggregate function.
         */
        register( OperatorName.FILTER, new SqlFilterOperator() );

        /*
         * <code>WITHIN_GROUP</code> operator performs aggregations on ordered data input.
         */
        register( OperatorName.WITHIN_GROUP, new SqlWithinGroupOperator() );

        /*
         * {@code CUBE} operator, occurs within {@code GROUP BY} clause or nested within a {@code GROUPING SETS}.
         */
        register( OperatorName.CUBE, new SqlRollupOperator( "CUBE", Kind.CUBE ) );

        /*
         * {@code ROLLUP} operator, occurs within {@code GROUP BY} clause or nested within a {@code GROUPING SETS}.
         */
        register( OperatorName.ROLLUP, new SqlRollupOperator( "ROLLUP", Kind.ROLLUP ) );

        /*
         * {@code GROUPING SETS} operator, occurs within {@code GROUP BY} clause or nested within a {@code GROUPING SETS}.
         */
        register( OperatorName.GROUPING_SETS, new SqlRollupOperator( "GROUPING SETS", Kind.GROUPING_SETS ) );

        /*
         * {@code GROUPING(c1 [, c2, ...])} function.
         *
         * Occurs in similar places to an aggregate function ({@code SELECT}, {@code HAVING} clause, etc. of an aggregate query), but not technically an aggregate function.
         */
        register( OperatorName.GROUPING, new SqlGroupingFunction( "GROUPING" ) );

        /*
         * {@code GROUP_ID()} function. (Oracle-specific.)
         */
        register( OperatorName.GROUP_ID, new SqlGroupIdFunction() );

        /*
         * {@code GROUPING_ID} function is a synonym for {@code GROUPING}.
         *
         * Some history. The {@code GROUPING} function is in the SQL standard, and originally supported only one argument. {@code GROUPING_ID} is not standard (though supported in Oracle and SQL Server)
         * and supports one or more arguments.
         *
         * The SQL standard has changed to allow {@code GROUPING} to have multiple arguments. It is now equivalent to {@code GROUPING_ID}, so we made {@code GROUPING_ID} a synonym for {@code GROUPING}.
         */
        register( OperatorName.GROUPING_ID, new SqlGroupingFunction( "GROUPING_ID" ) );

        /*
         * {@code EXTEND} operator.
         */
        register( OperatorName.EXTEND, new SqlExtendOperator() );

        /*
         * String concatenation operator, '<code>||</code>'.
         */
        register(
                OperatorName.CONCAT,
                new SqlBinaryOperator(
                        "||",
                        Kind.OTHER,
                        60,
                        true,
                        ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE,
                        null,
                        OperandTypes.STRING_SAME_SAME ) );

        /*
         * Arithmetic division operator, '<code>/</code>'.
         */
        register(
                OperatorName.DIVIDE,
                new SqlBinaryOperator(
                        "/",
                        Kind.DIVIDE,
                        60,
                        true,
                        ReturnTypes.QUOTIENT_NULLABLE,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.DIVISION_OPERATOR ) );

        /*
         * Arithmetic remainder operator, '<code>%</code>', an alternative to {@link #MOD} allowed if under certain conformance levels.
         *
         * @see SqlConformance#isPercentRemainderAllowed
         */
        register(
                OperatorName.PERCENT_REMAINDER,
                new SqlBinaryOperator(
                        "%",
                        Kind.MOD,
                        60,
                        true,
                        ReturnTypes.ARG1_NULLABLE,
                        null,
                        OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC ) );

        /*
         * The {@code RAND_INTEGER([seed, ] bound)} function, which yields a random integer, optionally with seed.
         */
        register( OperatorName.RAND_INTEGER, new SqlRandIntegerFunction() );

        /*
         * The {@code RAND([seed])} function, which yields a random double, optionally with seed.
         */
        register( OperatorName.RAND, new SqlRandFunction() );

        /*
         * Internal integer arithmetic division operator, '<code>/INT</code>'. This is only used to adjust scale for numerics. We distinguish it from user-requested division since some personalities want a floating-point computation,
         * whereas for the internal scaling use of division, we always want integer division.
         */
        register(
                OperatorName.DIVIDE_INTEGER,
                new SqlBinaryOperator(
                        "/INT",
                        Kind.DIVIDE,
                        60,
                        true,
                        ReturnTypes.INTEGER_QUOTIENT_NULLABLE,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.DIVISION_OPERATOR ) );

        /*
         * Dot operator, '<code>.</code>', used for referencing fields of records.
         */
        register( OperatorName.DOT, new SqlDotOperator() );

        /*
         * Logical equals operator, '<code>=</code>'.
         */
        register(
                OperatorName.EQUALS,
                new SqlBinaryOperator(
                        "=",
                        Kind.EQUALS,
                        30,
                        true,
                        ReturnTypes.BOOLEAN_NULLABLE,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED ) );

        /*
         * Logical greater-than operator, '<code>&gt;</code>'.
         */
        register(
                OperatorName.GREATER_THAN,
                new SqlBinaryOperator(
                        ">",
                        Kind.GREATER_THAN,
                        30,
                        true,
                        ReturnTypes.BOOLEAN_NULLABLE,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED ) );

        /*
         * <code>IS DISTINCT FROM</code> operator.
         */
        register(
                OperatorName.IS_DISTINCT_FROM,
                new SqlBinaryOperator(
                        "IS DISTINCT FROM",
                        Kind.IS_DISTINCT_FROM,
                        30,
                        true,
                        ReturnTypes.BOOLEAN,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED ) );

        /*
         * <code>IS NOT DISTINCT FROM</code> operator. Is equivalent to <code>NOT(x IS DISTINCT FROM y)</code>
         */
        register(
                OperatorName.IS_NOT_DISTINCT_FROM,
                new SqlBinaryOperator(
                        "IS NOT DISTINCT FROM",
                        Kind.IS_NOT_DISTINCT_FROM,
                        30,
                        true,
                        ReturnTypes.BOOLEAN,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED ) );

        /*
         * The internal <code>$IS_DIFFERENT_FROM</code> operator is the same as the user-level {@link #IS_DISTINCT_FROM} in all respects except that the test for equality on character datatypes treats trailing spaces as significant.
         */
        register(
                OperatorName.IS_DIFFERENT_FROM,
                new SqlBinaryOperator(
                        "$IS_DIFFERENT_FROM",
                        Kind.OTHER,
                        30,
                        true,
                        ReturnTypes.BOOLEAN,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED ) );

        /*
         * Logical greater-than-or-equal operator, '<code>&gt;=</code>'.
         */
        register(
                OperatorName.GREATER_THAN_OR_EQUAL,
                new SqlBinaryOperator(
                        ">=",
                        Kind.GREATER_THAN_OR_EQUAL,
                        30,
                        true,
                        ReturnTypes.BOOLEAN_NULLABLE,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED ) );

        /*
         * <code>IN</code> operator tests for a value's membership in a sub-query or a list of values.
         */
        register( OperatorName.IN, new SqlInOperator( Kind.IN ) );

        /*
         * <code>NOT IN</code> operator tests for a value's membership in a sub-query or a list of values.
         */
        register( OperatorName.NOT_IN, new SqlInOperator( Kind.NOT_IN ) );

        /*
         * The <code>&lt; SOME</code> operator (synonymous with <code>&lt; ANY</code>).
         */
        register( OperatorName.SOME_LT, new SqlQuantifyOperator( Kind.SOME, Kind.LESS_THAN ) );

        register( OperatorName.SOME_LE, new SqlQuantifyOperator( Kind.SOME, Kind.LESS_THAN_OR_EQUAL ) );

        register( OperatorName.SOME_GT, new SqlQuantifyOperator( Kind.SOME, Kind.GREATER_THAN ) );

        register( OperatorName.SOME_GE, new SqlQuantifyOperator( Kind.SOME, Kind.GREATER_THAN_OR_EQUAL ) );

        register( OperatorName.SOME_EQ, new SqlQuantifyOperator( Kind.SOME, Kind.EQUALS ) );

        register( OperatorName.SOME_NE, new SqlQuantifyOperator( Kind.SOME, Kind.NOT_EQUALS ) );

        /*
         * The <code>&lt; ALL</code> operator.
         */
        register( OperatorName.ALL_LT, new SqlQuantifyOperator( Kind.ALL, Kind.LESS_THAN ) );

        register( OperatorName.ALL_LE, new SqlQuantifyOperator( Kind.ALL, Kind.LESS_THAN_OR_EQUAL ) );

        register( OperatorName.ALL_GT, new SqlQuantifyOperator( Kind.ALL, Kind.GREATER_THAN ) );

        register( OperatorName.ALL_GE, new SqlQuantifyOperator( Kind.ALL, Kind.GREATER_THAN_OR_EQUAL ) );

        register( OperatorName.ALL_EQ, new SqlQuantifyOperator( Kind.ALL, Kind.EQUALS ) );

        register( OperatorName.ALL_NE, new SqlQuantifyOperator( Kind.ALL, Kind.NOT_EQUALS ) );

        /*
         * Logical less-than operator, '<code>&lt;</code>'.
         */
        register(
                OperatorName.LESS_THAN,
                new SqlBinaryOperator(
                        "<",
                        Kind.LESS_THAN,
                        30,
                        true,
                        ReturnTypes.BOOLEAN_NULLABLE,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED ) );

        /*
         * Logical less-than-or-equal operator, '<code>&lt;=</code>'.
         */
        register(
                OperatorName.LESS_THAN_OR_EQUAL,
                new SqlBinaryOperator(
                        "<=",
                        Kind.LESS_THAN_OR_EQUAL,
                        30,
                        true,
                        ReturnTypes.BOOLEAN_NULLABLE,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED ) );

        /*
         * Infix arithmetic minus operator, '<code>-</code>'.
         *
         * Its precedence is less than the prefix {@link #UNARY_PLUS +} and {@link #UNARY_MINUS -} operators.
         */
        register(
                OperatorName.MINUS,
                new SqlMonotonicBinaryOperator(
                        "-",
                        Kind.MINUS,
                        40,
                        true,

                        // Same type inference strategy as sum
                        ReturnTypes.NULLABLE_SUM,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.MINUS_OPERATOR ) );

        /*
         * Arithmetic multiplication operator, '<code>*</code>'.
         */
        register(
                OperatorName.MULTIPLY,
                new SqlMonotonicBinaryOperator(
                        "*",
                        Kind.TIMES,
                        60,
                        true,
                        ReturnTypes.PRODUCT_NULLABLE,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.MULTIPLY_OPERATOR ) );

        /*
         * Logical not-equals operator, '<code>&lt;&gt;</code>'.
         */
        register(
                OperatorName.NOT_EQUALS,
                new SqlBinaryOperator(
                        "<>",
                        Kind.NOT_EQUALS,
                        30,
                        true,
                        ReturnTypes.BOOLEAN_NULLABLE,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED ) );

        /*
         * Logical <code>OR</code> operator.
         */
        register(
                OperatorName.OR,
                new SqlBinaryOperator(
                        "OR",
                        Kind.OR,
                        22,
                        true,
                        ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
                        InferTypes.BOOLEAN,
                        OperandTypes.BOOLEAN_BOOLEAN ) );

        /*
         * Infix arithmetic plus operator, '<code>+</code>'.
         */
        register(
                OperatorName.PLUS,
                new SqlMonotonicBinaryOperator(
                        "+",
                        Kind.PLUS,
                        40,
                        true,
                        ReturnTypes.NULLABLE_SUM,
                        InferTypes.FIRST_KNOWN,
                        OperandTypes.PLUS_OPERATOR ) );

        /*
         * Infix datetime plus operator, '<code>DATETIME + INTERVAL</code>'.
         */
        register( OperatorName.DATETIME_PLUS, new SqlDatetimePlusOperator() );

        /*
         * Multiset {@code MEMBER OF}, which returns whether a element belongs to a multiset.
         *
         * For example, the following returns <code>false</code>:
         *
         * <blockquote>
         * <code>'green' MEMBER OF MULTISET ['red','almost green','blue']</code>
         * </blockquote>
         */
        register( OperatorName.MEMBER_OF, new SqlMultisetMemberOfOperator() );

        /*
         * Submultiset. Checks to see if an multiset is a sub-set of another multiset.
         *
         * For example, the following returns <code>false</code>:
         *
         * <blockquote>
         * <code>MULTISET ['green'] SUBMULTISET OF MULTISET['red', 'almost green', 'blue']</code>
         * </blockquote>
         *
         * The following returns <code>true</code>, in part because multisets are order-independent:
         *
         * <blockquote>
         * <code>MULTISET ['blue', 'red'] SUBMULTISET OF MULTISET ['red', 'almost green', 'blue']</code>
         * </blockquote>
         */
        register(
                OperatorName.SUBMULTISET_OF,

                // TODO: check if precedence is correct
                new SqlBinaryOperator(
                        "SUBMULTISET OF",
                        Kind.OTHER,
                        30,
                        true,
                        ReturnTypes.BOOLEAN_NULLABLE,
                        null,
                        OperandTypes.MULTISET_MULTISET ) );

        register(
                OperatorName.NOT_SUBMULTISET_OF,

                // TODO: check if precedence is correct
                new SqlBinaryOperator(
                        "NOT SUBMULTISET OF",
                        Kind.OTHER,
                        30,
                        true,
                        ReturnTypes.BOOLEAN_NULLABLE,
                        null,
                        OperandTypes.MULTISET_MULTISET ) );

        //-------------------------------------------------------------
        //                   POSTFIX OPERATORS
        //-------------------------------------------------------------
        register(
                OperatorName.DESC,
                new SqlPostfixOperator(
                        "DESC",
                        Kind.DESCENDING,
                        20,
                        ReturnTypes.ARG0,
                        InferTypes.RETURN_TYPE,
                        OperandTypes.ANY ) );

        register(
                OperatorName.NULLS_FIRST,
                new SqlPostfixOperator(
                        "NULLS FIRST",
                        Kind.NULLS_FIRST,
                        18,
                        ReturnTypes.ARG0,
                        InferTypes.RETURN_TYPE,
                        OperandTypes.ANY ) );

        register(
                OperatorName.NULLS_LAST,
                new SqlPostfixOperator(
                        "NULLS LAST",
                        Kind.NULLS_LAST,
                        18,
                        ReturnTypes.ARG0,
                        InferTypes.RETURN_TYPE,
                        OperandTypes.ANY ) );

        register(
                OperatorName.IS_NOT_NULL,
                new SqlPostfixOperator(
                        "IS NOT NULL",
                        Kind.IS_NOT_NULL,
                        28,
                        ReturnTypes.BOOLEAN_NOT_NULL,
                        InferTypes.VARCHAR_1024,
                        OperandTypes.ANY ) );

        register(
                OperatorName.IS_NULL,
                new SqlPostfixOperator(
                        "IS NULL",
                        Kind.IS_NULL,
                        28,
                        ReturnTypes.BOOLEAN_NOT_NULL,
                        InferTypes.VARCHAR_1024,
                        OperandTypes.ANY ) );

        register(
                OperatorName.IS_NOT_TRUE,
                new SqlPostfixOperator(
                        "IS NOT TRUE",
                        Kind.IS_NOT_TRUE,
                        28,
                        ReturnTypes.BOOLEAN_NOT_NULL,
                        InferTypes.BOOLEAN,
                        OperandTypes.BOOLEAN ) );

        register(
                OperatorName.IS_TRUE,
                new SqlPostfixOperator(
                        "IS TRUE",
                        Kind.IS_TRUE,
                        28,
                        ReturnTypes.BOOLEAN_NOT_NULL,
                        InferTypes.BOOLEAN,
                        OperandTypes.BOOLEAN ) );

        register(
                OperatorName.IS_NOT_FALSE,
                new SqlPostfixOperator(
                        "IS NOT FALSE",
                        Kind.IS_NOT_FALSE,
                        28,
                        ReturnTypes.BOOLEAN_NOT_NULL,
                        InferTypes.BOOLEAN,
                        OperandTypes.BOOLEAN ) );

        register(
                OperatorName.IS_FALSE,
                new SqlPostfixOperator(
                        "IS FALSE",
                        Kind.IS_FALSE,
                        28,
                        ReturnTypes.BOOLEAN_NOT_NULL,
                        InferTypes.BOOLEAN,
                        OperandTypes.BOOLEAN ) );

        register(
                OperatorName.IS_NOT_UNKNOWN,
                new SqlPostfixOperator(
                        "IS NOT UNKNOWN",
                        Kind.IS_NOT_NULL,
                        28,
                        ReturnTypes.BOOLEAN_NOT_NULL,
                        InferTypes.BOOLEAN,
                        OperandTypes.BOOLEAN ) );

        register(
                OperatorName.IS_UNKNOWN,
                new SqlPostfixOperator(
                        "IS UNKNOWN",
                        Kind.IS_NULL,
                        28,
                        ReturnTypes.BOOLEAN_NOT_NULL,
                        InferTypes.BOOLEAN,
                        OperandTypes.BOOLEAN ) );

        register(
                OperatorName.IS_A_SET,
                new SqlPostfixOperator(
                        "IS A SET",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.MULTISET ) );

        register(
                OperatorName.IS_NOT_A_SET,
                new SqlPostfixOperator(
                        "IS NOT A SET",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.MULTISET ) );

        register(
                OperatorName.IS_EMPTY,
                new SqlPostfixOperator(
                        "IS EMPTY",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.COLLECTION_OR_MAP ) );

        register(
                OperatorName.IS_NOT_EMPTY,
                new SqlPostfixOperator(
                        "IS NOT EMPTY",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.COLLECTION_OR_MAP ) );

        register(
                OperatorName.IS_JSON_VALUE,
                new SqlPostfixOperator(
                        "IS JSON VALUE",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.CHARACTER ) );

        register(
                OperatorName.IS_NOT_JSON_VALUE,
                new SqlPostfixOperator(
                        "IS NOT JSON VALUE",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.CHARACTER ) );

        register(
                OperatorName.IS_JSON_OBJECT,
                new SqlPostfixOperator(
                        "IS JSON OBJECT",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.CHARACTER ) );

        register(
                OperatorName.IS_NOT_JSON_OBJECT,
                new SqlPostfixOperator(
                        "IS NOT JSON OBJECT",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.CHARACTER ) );

        register(
                OperatorName.IS_JSON_ARRAY,
                new SqlPostfixOperator(
                        "IS JSON ARRAY",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.CHARACTER ) );

        register(
                OperatorName.IS_NOT_JSON_ARRAY,
                new SqlPostfixOperator(
                        "IS NOT JSON ARRAY",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.CHARACTER ) );

        register(
                OperatorName.IS_JSON_SCALAR,
                new SqlPostfixOperator(
                        "IS JSON SCALAR",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.CHARACTER ) );

        register(
                OperatorName.IS_NOT_JSON_SCALAR,
                new SqlPostfixOperator(
                        "IS NOT JSON SCALAR",
                        Kind.OTHER,
                        28,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.CHARACTER ) );

        //-------------------------------------------------------------
        //                   PREFIX OPERATORS
        //-------------------------------------------------------------
        register(
                OperatorName.EXISTS,
                new SqlPrefixOperator(
                        "EXISTS",
                        Kind.EXISTS,
                        40,
                        ReturnTypes.BOOLEAN,
                        null,
                        OperandTypes.ANY ) {
                    @Override
                    public boolean argumentMustBeScalar( int ordinal ) {
                        return false;
                    }


                    @Override
                    public boolean validRexOperands( int count, Litmus litmus ) {
                        if ( count != 0 ) {
                            return litmus.fail( "wrong operand count {} for {}", count, this );
                        }
                        return litmus.succeed();
                    }
                } );

        register(
                OperatorName.NOT,
                new SqlPrefixOperator(
                        "NOT",
                        Kind.NOT,
                        26,
                        ReturnTypes.ARG0,
                        InferTypes.BOOLEAN,
                        OperandTypes.BOOLEAN ) );

        /*
         * Prefix arithmetic minus operator, '<code>-</code>'.
         *
         * Its precedence is greater than the infix '{@link #PLUS +}' and '{@link #MINUS -}' operators.
         */
        register(
                OperatorName.UNARY_MINUS,
                new SqlPrefixOperator(
                        "-",
                        Kind.MINUS_PREFIX,
                        80,
                        ReturnTypes.ARG0,
                        InferTypes.RETURN_TYPE,
                        OperandTypes.NUMERIC_OR_INTERVAL ) );

        /*
         * Prefix arithmetic plus operator, '<code>+</code>'.
         *
         * Its precedence is greater than the infix '{@link #PLUS +}' and '{@link #MINUS -}' operators.
         */
        register(
                OperatorName.UNARY_PLUS,
                new SqlPrefixOperator(
                        "+",
                        Kind.PLUS_PREFIX,
                        80,
                        ReturnTypes.ARG0,
                        InferTypes.RETURN_TYPE,
                        OperandTypes.NUMERIC_OR_INTERVAL ) );

        /*
         * Keyword which allows an identifier to be explicitly flagged as a table.
         * For example, <code>select * from (TABLE t)</code> or <code>TABLE t</code>. See also {@link #COLLECTION_TABLE}.
         */
        register(
                OperatorName.EXPLICIT_TABLE,
                new SqlPrefixOperator(
                        "TABLE",
                        Kind.EXPLICIT_TABLE,
                        2,
                        null,
                        null,
                        null ) );

        /*
         * {@code FINAL} function to be used within {@code MATCH_RECOGNIZE}.
         */
        register(
                OperatorName.FINAL,
                new SqlPrefixOperator(
                        "FINAL",
                        Kind.FINAL,
                        80,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.ANY ) );

        /*
         * {@code RUNNING} function to be used within {@code MATCH_RECOGNIZE}.
         */
        register(
                OperatorName.RUNNING,
                new SqlPrefixOperator(
                        "RUNNING",
                        Kind.RUNNING,
                        80,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.ANY ) );

        //-------------------------------------------------------------
        // AGGREGATE OPERATORS
        //-------------------------------------------------------------
        /*
         * <code>SUM</code> aggregate function.
         */
        register( OperatorName.SUM, new SqlSumAggFunction( null ) );

        /*
         * <code>COUNT</code> aggregate function.
         */
        register( OperatorName.COUNT, new SqlCountAggFunction( "COUNT" ) );

        /*
         * <code>APPROX_COUNT_DISTINCT</code> aggregate function.
         */
        register( OperatorName.APPROX_COUNT_DISTINCT, new SqlCountAggFunction( "APPROX_COUNT_DISTINCT" ) );

        /*
         * <code>MIN</code> aggregate function.
         */
        register( OperatorName.MIN, new SqlMinMaxAggFunction( Kind.MIN ) );

        /*
         * <code>MAX</code> aggregate function.
         */
        register( OperatorName.MAX, new SqlMinMaxAggFunction( Kind.MAX ) );

        /*
         * <code>LAST_VALUE</code> aggregate function.
         */
        register( OperatorName.LAST_VALUE, new SqlFirstLastValueAggFunction( Kind.LAST_VALUE ) );

        /*
         * <code>ANY_VALUE</code> aggregate function.
         */
        register( OperatorName.ANY_VALUE, new SqlAnyValueAggFunction( Kind.ANY_VALUE ) );

        /*
         * <code>FIRST_VALUE</code> aggregate function.
         */
        register( OperatorName.FIRST_VALUE, new SqlFirstLastValueAggFunction( Kind.FIRST_VALUE ) );

        /*
         * <code>NTH_VALUE</code> aggregate function.
         */
        register( OperatorName.NTH_VALUE, new SqlNthValueAggFunction( Kind.NTH_VALUE ) );

        /*
         * <code>LEAD</code> aggregate function.
         */
        register( OperatorName.LEAD, new SqlLeadLagAggFunction( Kind.LEAD ) );

        /*
         * <code>LAG</code> aggregate function.
         */
        register( OperatorName.LAG, new SqlLeadLagAggFunction( Kind.LAG ) );

        /*
         * <code>NTILE</code> aggregate function.
         */
        register( OperatorName.NTILE, new SqlNtileAggFunction() );

        /*
         * <code>SINGLE_VALUE</code> aggregate function.
         */
        register( OperatorName.SINGLE_VALUE, new SqlSingleValueAggFunction( null ) );

        /*
         * <code>AVG</code> aggregate function.
         */
        register( OperatorName.AVG, new SqlAvgAggFunction( Kind.AVG ) );

        /*
         * <code>STDDEV_POP</code> aggregate function.
         */
        register( OperatorName.STDDEV_POP, new SqlAvgAggFunction( Kind.STDDEV_POP ) );

        /*
         * <code>REGR_COUNT</code> aggregate function.
         */
        register( OperatorName.REGR_COUNT, new SqlRegrCountAggFunction( Kind.REGR_COUNT ) );

        /*
         * <code>REGR_SXX</code> aggregate function.
         */
        register( OperatorName.REGR_SXX, new SqlCovarAggFunction( Kind.REGR_SXX ) );

        /*
         * <code>REGR_SYY</code> aggregate function.
         */
        register( OperatorName.REGR_SYY, new SqlCovarAggFunction( Kind.REGR_SYY ) );

        /*
         * <code>COVAR_POP</code> aggregate function.
         */
        register( OperatorName.COVAR_POP, new SqlCovarAggFunction( Kind.COVAR_POP ) );

        /*
         * <code>COVAR_SAMP</code> aggregate function.
         */
        register( OperatorName.COVAR_SAMP, new SqlCovarAggFunction( Kind.COVAR_SAMP ) );

        /*
         * <code>STDDEV_SAMP</code> aggregate function.
         */
        register( OperatorName.STDDEV_SAMP, new SqlAvgAggFunction( Kind.STDDEV_SAMP ) );

        /*
         * <code>STDDEV</code> aggregate function.
         */
        register( OperatorName.STDDEV, new SqlAvgAggFunction( "STDDEV", Kind.STDDEV_SAMP ) );

        /*
         * <code>VAR_POP</code> aggregate function.
         */
        register( OperatorName.VAR_POP, new SqlAvgAggFunction( Kind.VAR_POP ) );

        /*
         * <code>VAR_SAMP</code> aggregate function.
         */
        register( OperatorName.VAR_SAMP, new SqlAvgAggFunction( Kind.VAR_SAMP ) );

        /*
         * <code>VARIANCE</code> aggregate function.
         */
        register( OperatorName.VARIANCE, new SqlAvgAggFunction( "VARIANCE", Kind.VAR_SAMP ) );

        /*
         * <code>BIT_AND</code> aggregate function.
         */
        register( OperatorName.BIT_AND, new SqlBitOpAggFunction( Kind.BIT_AND ) );

        /*
         * <code>BIT_OR</code> aggregate function.
         */
        register( OperatorName.BIT_OR, new SqlBitOpAggFunction( Kind.BIT_OR ) );

        //-------------------------------------------------------------
        // WINDOW Aggregate Functions
        //-------------------------------------------------------------
        /*
         * <code>HISTOGRAM</code> aggregate function support. Used by window aggregate versions of MIN/MAX
         */
        register( OperatorName.HISTOGRAM_AGG, new SqlHistogramAggFunction( null ) );

        /*
         * <code>HISTOGRAM_MIN</code> window aggregate function.
         */
        register(
                OperatorName.HISTOGRAM_MIN,
                new SqlFunction(
                        "$HISTOGRAM_MIN",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.NUMERIC_OR_STRING,
                        FunctionCategory.NUMERIC ) );

        /*
         * <code>HISTOGRAM_MAX</code> window aggregate function.
         */
        register(
                OperatorName.HISTOGRAM_MAX,
                new SqlFunction(
                        "$HISTOGRAM_MAX",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.NUMERIC_OR_STRING,
                        FunctionCategory.NUMERIC ) );

        /*
         * <code>HISTOGRAM_FIRST_VALUE</code> window aggregate function.
         */
        register(
                OperatorName.HISTOGRAM_FIRST_VALUE,
                new SqlFunction(
                        "$HISTOGRAM_FIRST_VALUE",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.NUMERIC_OR_STRING,
                        FunctionCategory.NUMERIC ) );

        /*
         * <code>HISTOGRAM_LAST_VALUE</code> window aggregate function.
         */
        register(
                OperatorName.HISTOGRAM_LAST_VALUE,
                new SqlFunction(
                        "$HISTOGRAM_LAST_VALUE",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.NUMERIC_OR_STRING,
                        FunctionCategory.NUMERIC ) );

        /*
         * <code>SUM0</code> aggregate function.
         */
        register( OperatorName.SUM0, new SqlSumEmptyIsZeroAggFunction() );

        //-------------------------------------------------------------
        // WINDOW Rank Functions
        //-------------------------------------------------------------
        /*
         * <code>CUME_DIST</code> window function.
         */
        register( OperatorName.CUME_DIST, new SqlRankFunction( Kind.CUME_DIST, ReturnTypes.FRACTIONAL_RANK, true ) );

        /*
         * <code>DENSE_RANK</code> window function.
         */
        register( OperatorName.DENSE_RANK, new SqlRankFunction( Kind.DENSE_RANK, ReturnTypes.RANK, true ) );

        /*
         * <code>PERCENT_RANK</code> window function.
         */
        register( OperatorName.PERCENT_RANK, new SqlRankFunction( Kind.PERCENT_RANK, ReturnTypes.FRACTIONAL_RANK, true ) );

        /*
         * <code>RANK</code> window function.
         */
        register( OperatorName.RANK, new SqlRankFunction( Kind.RANK, ReturnTypes.RANK, true ) );

        /*
         * <code>ROW_NUMBER</code> window function.
         */
        register( OperatorName.ROW_NUMBER, new SqlRankFunction( Kind.ROW_NUMBER, ReturnTypes.RANK, false ) );

        //-------------------------------------------------------------
        //                   SPECIAL OPERATORS
        //-------------------------------------------------------------
        register( OperatorName.ROW, new SqlRowOperator( "ROW" ) );

        /*
         * A special operator for the subtraction of two DATETIMEs. The format of DATETIME subtraction is:
         *
         * <blockquote><code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")" &lt;interval qualifier&gt;</code></blockquote>
         *
         * This operator is special since it needs to hold the additional interval qualifier specification.
         */
        register( OperatorName.MINUS_DATE, new SqlDatetimeSubtractionOperator() );

        /*
         * The MULTISET Value Constructor. e.g. "<code>MULTISET[1,2,3]</code>".
         */
        register( OperatorName.MULTISET_VALUE, new SqlMultisetValueConstructor() );

        /*
         * The MULTISET Query Constructor. e.g. "<code>SELECT dname, MULTISET(SELECT FROM emp WHERE deptno", dept.deptno) FROM dept</code>".
         */
        register( OperatorName.MULTISET_QUERY, new SqlMultisetQueryConstructor() );

        /*
         * The ARRAY Query Constructor. e.g. "<code>SELECT dname, ARRAY(SELECT FROM emp WHERE deptno", dept.deptno) FROM dept</code>".
         */
        register( OperatorName.ARRAY_QUERY, new SqlArrayQueryConstructor() );

        /*
         * The MAP Query Constructor. e.g. "<code>MAP(SELECT empno, deptno FROM emp)</code>".
         */
        register( OperatorName.MAP_QUERY, new SqlMapQueryConstructor() );

        /*
         * The CURSOR constructor. e.g. "<code>SELECT * FROM TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), 'name'))</code>".
         */
        register( OperatorName.CURSOR, new SqlCursorConstructor() );

        /*
         * The COLUMN_LIST constructor. e.g. the ROW() call in "<code>SELECT * FROM TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), ROW(name, empno)))</code>".
         */
        register( OperatorName.COLUMN_LIST, new SqlColumnListConstructor() );

        /*
         * The <code>UNNEST</code> operator.
         */
        register( OperatorName.UNNEST, new SqlUnnestOperator( false ) );

        /*
         * The <code>UNNEST WITH ORDINALITY</code> operator.
         */
        register( OperatorName.UNNEST_WITH_ORDINALITY, new SqlUnnestOperator( true ) );

        /*
         * The <code>LATERAL</code> operator.
         */
        register( OperatorName.LATERAL, new SqlLateralOperator( Kind.LATERAL ) );

        /*
         * The "table function derived table" operator, which a table-valued function into a relation, e.g. "<code>SELECT * FROM TABLE(ramp(5))</code>".
         *
         * This operator has function syntax (with one argument), whereas {@link #EXPLICIT_TABLE} is a prefix operator.
         */
        register( OperatorName.COLLECTION_TABLE, new SqlCollectionTableOperator( "TABLE", Modality.RELATION ) );

        register( OperatorName.OVERLAPS, new SqlOverlapsOperator( Kind.OVERLAPS ) );

        register( OperatorName.CONTAINS, new SqlOverlapsOperator( Kind.CONTAINS ) );

        register( OperatorName.PRECEDES, new SqlOverlapsOperator( Kind.PRECEDES ) );

        register( OperatorName.IMMEDIATELY_PRECEDES, new SqlOverlapsOperator( Kind.IMMEDIATELY_PRECEDES ) );

        register( OperatorName.SUCCEEDS, new SqlOverlapsOperator( Kind.SUCCEEDS ) );

        register( OperatorName.IMMEDIATELY_SUCCEEDS, new SqlOverlapsOperator( Kind.IMMEDIATELY_SUCCEEDS ) );

        register( OperatorName.PERIOD_EQUALS, new SqlOverlapsOperator( Kind.PERIOD_EQUALS ) );

        register( OperatorName.VALUES, new SqlValuesOperator() );

        register( OperatorName.LITERAL_CHAIN, new SqlLiteralChainOperator() );

        register( OperatorName.THROW, new SqlThrowOperator() );

        register( OperatorName.JSON_VALUE_EXPRESSION, new SqlJsonValueExpressionOperator( "JSON_VALUE_EXPRESSION", false ) );

        register( OperatorName.JSON_VALUE_EXPRESSION_EXCLUDED, new SqlJsonValueExpressionOperator( "JSON_VALUE_EXPRESSION_EXCLUDE", false ) );

        register( OperatorName.JSON_STRUCTURED_VALUE_EXPRESSION, new SqlJsonValueExpressionOperator( "JSON_STRUCTURED_VALUE_EXPRESSION", true ) );

        register( OperatorName.JSON_API_COMMON_SYNTAX, new SqlJsonApiCommonSyntaxOperator() );

        register( OperatorName.JSON_EXISTS, new SqlJsonExistsFunction() );

        register( OperatorName.JSON_VALUE, new SqlJsonValueFunction( "JSON_VALUE", false ) );

        register( OperatorName.JSON_VALUE_ANY, new SqlJsonValueFunction( "JSON_VALUE_ANY", true ) );

        register( OperatorName.JSON_QUERY, new SqlJsonQueryFunction() );

        register( OperatorName.JSON_OBJECT, new SqlJsonObjectFunction() );

        register( OperatorName.JSON_OBJECTAGG, new SqlJsonObjectAggAggFunction( "JSON_OBJECTAGG", JsonConstructorNullClause.NULL_ON_NULL ) );

        register( OperatorName.JSON_ARRAY, new SqlJsonArrayFunction() );

        register( OperatorName.JSON_ARRAYAGG, new SqlJsonArrayAggAggFunction( "JSON_ARRAYAGG", JsonConstructorNullClause.NULL_ON_NULL ) );

        register( OperatorName.BETWEEN, new SqlBetweenOperator( SqlBetweenOperator.Flag.ASYMMETRIC, false ) );

        register( OperatorName.SYMMETRIC_BETWEEN, new SqlBetweenOperator( SqlBetweenOperator.Flag.SYMMETRIC, false ) );

        register( OperatorName.NOT_BETWEEN, new SqlBetweenOperator( SqlBetweenOperator.Flag.ASYMMETRIC, true ) );

        register( OperatorName.SYMMETRIC_NOT_BETWEEN, new SqlBetweenOperator( SqlBetweenOperator.Flag.SYMMETRIC, true ) );

        register( OperatorName.NOT_LIKE, new SqlLikeOperator( "NOT LIKE", Kind.LIKE, true ) );

        register( OperatorName.LIKE, new SqlLikeOperator( "LIKE", Kind.LIKE, false ) );

        register( OperatorName.NOT_SIMILAR_TO, new SqlLikeOperator( "NOT SIMILAR TO", Kind.SIMILAR, true ) );

        register( OperatorName.SIMILAR_TO, new SqlLikeOperator( "SIMILAR TO", Kind.SIMILAR, false ) );

        /*
         * Internal operator used to represent the ESCAPE clause of a LIKE or SIMILAR TO expression.
         */
        register( OperatorName.ESCAPE, new SqlSpecialOperator( "ESCAPE", Kind.ESCAPE, 0 ) );

        register( OperatorName.CASE, SqlCaseOperator.INSTANCE );

        register( OperatorName.PROCEDURE_CALL, new SqlProcedureCallOperator() );

        register( OperatorName.NEW, new SqlNewOperator() );

        /*
         * The <code>OVER</code> operator, which applies an aggregate functions to a {@link SqlWindow window}.
         *
         * Operands are as follows:
         *
         * <ol>
         * <li>name of window function ({@link SqlCall})</li>
         * <li>window name ({@link org.polypheny.db.sql.SqlLiteral}) or window in-line specification (@link SqlWindowOperator})</li>
         * </ol>
         */
        register( OperatorName.OVER, new SqlOverOperator() );

        /*
         * An <code>REINTERPRET</code> operator is internal to the planner. When the physical storage of two types is the same, this operator may be used to reinterpret values of one type as the other. This operator is similar to a cast,
         * except that it does not alter the data value. Like a regular cast it accepts one operand and stores the target type as the return type. It performs an overflow check if it has <i>any</i> second operand, whether true or not.
         */
        register(
                OperatorName.REINTERPRET,
                new SqlSpecialOperator( "Reinterpret", Kind.REINTERPRET ) {
                    @Override
                    public OperandCountRange getOperandCountRange() {
                        return PolyOperandCountRanges.between( 1, 2 );
                    }
                } );

        //-------------------------------------------------------------
        //                   FUNCTIONS
        //-------------------------------------------------------------

        /*
         * distance function: <code>DISTANCE(column, ARRAY[], METRIC, WEIGHTS)</code>.
         */
        register( OperatorName.DISTANCE, new SqlDistanceFunction() );

        /*
         * Get metadata of multimedia files
         */
        register( OperatorName.META, new SqlMetaFunction() );

        /*
         * The character substring function: <code>SUBSTRING(string FROM start [FOR length])</code>.
         *
         * If the length parameter is a constant, the length of the result is the minimum of the length of the input and that length. Otherwise it is the length of the input.
         */
        register( OperatorName.SUBSTRING, new SqlSubstringFunction() );

        /*
         * The {@code REPLACE(string, search, replace)} function. Not standard SQL, but in Oracle and Postgres.
         */
        register(
                OperatorName.REPLACE,
                new SqlFunction(
                        "REPLACE",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0_NULLABLE_VARYING,
                        null,
                        OperandTypes.STRING_STRING_STRING,
                        FunctionCategory.STRING ) );

        register( OperatorName.CONVERT, new SqlConvertFunction( "CONVERT" ) );

        /*
         * The <code>TRANSLATE(<i>char_value</i> USING <i>translation_name</i>)</code> function alters the character set of a string value from one base character set to another.
         *
         * It is defined in the SQL standard. See also non-standard {@link OracleSqlOperatorTable#TRANSLATE3}.
         */
        register( OperatorName.TRANSLATE, new SqlConvertFunction( "TRANSLATE" ) );

        register( OperatorName.OVERLAY, new SqlOverlayFunction() );

        /*
         * The "TRIM" function.
         */
        register( OperatorName.TRIM, SqlTrimFunction.INSTANCE );

        register( OperatorName.POSITION, new SqlPositionFunction() );

        register(
                OperatorName.CHAR_LENGTH,
                new SqlFunction(
                        "CHAR_LENGTH",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.INTEGER_NULLABLE,
                        null,
                        OperandTypes.CHARACTER,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.CHARACTER_LENGTH,
                new SqlFunction(
                        "CHARACTER_LENGTH",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.INTEGER_NULLABLE,
                        null,
                        OperandTypes.CHARACTER,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.UPPER,
                new SqlFunction(
                        "UPPER",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.CHARACTER,
                        FunctionCategory.STRING ) );

        register(
                OperatorName.LOWER,
                new SqlFunction(
                        "LOWER",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.CHARACTER,
                        FunctionCategory.STRING ) );

        register(
                OperatorName.INITCAP,
                new SqlFunction(
                        "INITCAP",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.CHARACTER,
                        FunctionCategory.STRING ) );

        /*
         * Uses SqlOperatorTable.useDouble for its return type since we don't know what the result type will be by just looking at the operand types. For example POW(int, int) can return a non integer if the second operand is negative.
         */
        register(
                OperatorName.POWER,
                new SqlFunction(
                        "POWER",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC_NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.SQRT,
                new SqlFunction(
                        "SQRT",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        /*
         * Arithmetic remainder function {@code MOD}.
         *
         * @see #PERCENT_REMAINDER
         */
        register(
                OperatorName.MOD,
                // Return type is same as divisor (2nd operand)
                // SQL2003 Part2 Section 6.27, Syntax Rules 9
                new SqlFunction(
                        "MOD",
                        Kind.MOD,
                        ReturnTypes.ARG1_NULLABLE,
                        null,
                        OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.LN,
                new SqlFunction(
                        "LN",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.LOG10,
                new SqlFunction(
                        "LOG10",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.ABS,
                new SqlFunction(
                        "ABS",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0,
                        null,
                        OperandTypes.NUMERIC_OR_INTERVAL,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.ACOS,
                new SqlFunction(
                        "ACOS",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.ASIN,
                new SqlFunction(
                        "ASIN",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.ATAN,
                new SqlFunction(
                        "ATAN",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.ATAN2,
                new SqlFunction(
                        "ATAN2",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC_NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.COS,
                new SqlFunction(
                        "COS",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.COT,
                new SqlFunction(
                        "COT",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.DEGREES,
                new SqlFunction(
                        "DEGREES",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.EXP,
                new SqlFunction(
                        "EXP",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.RADIANS,
                new SqlFunction(
                        "RADIANS",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.ROUND,
                new SqlFunction(
                        "ROUND",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.NUMERIC_OPTIONAL_INTEGER,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.SIGN,
                new SqlFunction(
                        "SIGN",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.SIN,
                new SqlFunction(
                        "SIN",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.TAN,
                new SqlFunction(
                        "TAN",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.DOUBLE_NULLABLE,
                        null,
                        OperandTypes.NUMERIC,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.TRUNCATE,
                new SqlFunction(
                        "TRUNCATE",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.NUMERIC_OPTIONAL_INTEGER,
                        FunctionCategory.NUMERIC ) );

        register(
                OperatorName.PI,
                new SqlBaseContextVariable(
                        "PI",
                        ReturnTypes.DOUBLE,
                        FunctionCategory.NUMERIC ) );

        /*
         * {@code FIRST} function to be used within {@code MATCH_RECOGNIZE}.
         */
        register(
                OperatorName.FIRST,
                new SqlFunction(
                        "FIRST",
                        Kind.FIRST,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.ANY_NUMERIC,
                        FunctionCategory.MATCH_RECOGNIZE ) );

        /*
         * {@code LAST} function to be used within {@code MATCH_RECOGNIZE}.
         */
        register(
                OperatorName.LAST,
                new SqlFunction(
                        "LAST",
                        Kind.LAST,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.ANY_NUMERIC,
                        FunctionCategory.MATCH_RECOGNIZE ) );

        /*
         * {@code PREV} function to be used within {@code MATCH_RECOGNIZE}.
         */
        register(
                OperatorName.PREV,
                new SqlFunction(
                        "PREV",
                        Kind.PREV,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.ANY_NUMERIC,
                        FunctionCategory.MATCH_RECOGNIZE ) );

        /*
         * {@code NEXT} function to be used within {@code MATCH_RECOGNIZE}.
         */
        register(
                OperatorName.NEXT,
                new SqlFunction(
                        "NEXT",
                        Kind.NEXT,
                        ReturnTypes.ARG0_NULLABLE,
                        null,
                        OperandTypes.ANY_NUMERIC,
                        FunctionCategory.MATCH_RECOGNIZE ) );

        /*
         * {@code CLASSIFIER} function to be used within {@code MATCH_RECOGNIZE}.
         */
        register(
                OperatorName.CLASSIFIER,
                new SqlFunction(
                        "CLASSIFIER",
                        Kind.CLASSIFIER,
                        ReturnTypes.VARCHAR_2000,
                        null,
                        OperandTypes.NILADIC,
                        FunctionCategory.MATCH_RECOGNIZE ) );

        /*
         * {@code MATCH_NUMBER} function to be used within {@code MATCH_RECOGNIZE}.
         */
        register(
                OperatorName.MATCH_NUMBER,
                new SqlFunction(
                        "MATCH_NUMBER ",
                        Kind.MATCH_NUMBER,
                        ReturnTypes.BIGINT_NULLABLE,
                        null,
                        OperandTypes.NILADIC,
                        FunctionCategory.MATCH_RECOGNIZE ) );

        register( OperatorName.NULLIF, new SqlNullifFunction() );

        /*
         * The COALESCE builtin function.
         */
        register( OperatorName.COALESCE, new SqlCoalesceFunction() );

        /*
         * The <code>FLOOR</code> function.
         */
        register( OperatorName.FLOOR, new SqlFloorFunction( Kind.FLOOR ) );

        /*
         * The <code>CEIL</code> function.
         */
        register( OperatorName.CEIL, new SqlFloorFunction( Kind.CEIL ) );

        /*
         * The <code>USER</code> function.
         */
        register( OperatorName.USER, new SqlStringContextVariable( "USER" ) );

        /*
         * The <code>CURRENT_USER</code> function.
         */
        register( OperatorName.CURRENT_USER, new SqlStringContextVariable( "CURRENT_USER" ) );

        /*
         * The <code>SESSION_USER</code> function.
         */
        register( OperatorName.SESSION_USER, new SqlStringContextVariable( "SESSION_USER" ) );

        /*
         * The <code>SYSTEM_USER</code> function.
         */
        register( OperatorName.SYSTEM_USER, new SqlStringContextVariable( "SYSTEM_USER" ) );

        /*
         * The <code>CURRENT_PATH</code> function.
         */
        register( OperatorName.CURRENT_PATH, new SqlStringContextVariable( "CURRENT_PATH" ) );

        /*
         * The <code>CURRENT_ROLE</code> function.
         */
        register( OperatorName.CURRENT_ROLE, new SqlStringContextVariable( "CURRENT_ROLE" ) );

        /*
         * The <code>CURRENT_CATALOG</code> function.
         */
        register( OperatorName.CURRENT_CATALOG, new SqlStringContextVariable( "CURRENT_CATALOG" ) );

        /*
         * The <code>CURRENT_SCHEMA</code> function.
         */
        register( OperatorName.CURRENT_SCHEMA, new SqlStringContextVariable( "CURRENT_SCHEMA" ) );

        /*
         * The <code>LOCALTIME [(<i>precision</i>)]</code> function.
         */
        register( OperatorName.LOCALTIME, new SqlAbstractTimeFunction( "LOCALTIME", PolyType.TIME ) );

        /*
         * The <code>LOCALTIMESTAMP [(<i>precision</i>)]</code> function.
         */
        register( OperatorName.LOCALTIMESTAMP, new SqlAbstractTimeFunction( "LOCALTIMESTAMP", PolyType.TIMESTAMP ) );

        /*
         * The <code>CURRENT_TIME [(<i>precision</i>)]</code> function.
         */
        register( OperatorName.CURRENT_TIME, new SqlAbstractTimeFunction( "CURRENT_TIME", PolyType.TIME ) );

        /*
         * The <code>CURRENT_TIMESTAMP [(<i>precision</i>)]</code> function.
         */
        register( OperatorName.CURRENT_TIMESTAMP, new SqlAbstractTimeFunction( "CURRENT_TIMESTAMP", PolyType.TIMESTAMP ) );

        /*
         * The <code>CURRENT_DATE</code> function.
         */
        register( OperatorName.CURRENT_DATE, new SqlCurrentDateFunction() );

        /*
         * The <code>TIMESTAMPADD</code> function.
         */
        register( OperatorName.TIMESTAMP_ADD, new SqlTimestampAddFunction() );

        /*
         * The <code>TIMESTAMPDIFF</code> function.
         */
        register( OperatorName.TIMESTAMP_DIFF, new SqlTimestampDiffFunction() );

        /*
         * Use of the <code>IN_FENNEL</code> operator forces the argument to be evaluated in Fennel. Otherwise acts as identity function.
         */
        register(
                OperatorName.IN_FENNEL,
                new SqlMonotonicUnaryFunction(
                        "IN_FENNEL",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.ARG0,
                        null,
                        OperandTypes.ANY,
                        FunctionCategory.SYSTEM ) );

        /*
         * The SQL <code>CAST</code> operator.
         *
         * The SQL syntax is
         *
         * <blockquote><code>CAST(<i>expression</i> AS <i>type</i>)</code></blockquote>
         *
         * When the CAST operator is applies as a {@link SqlCall}, it has two arguments: the expression and the type. The type must not include a constraint, so <code>CAST(x AS INTEGER NOT NULL)</code>,
         * for instance, is invalid.
         *
         * When the CAST operator is applied as a <code>RexCall</code>, the target type is simply stored as the return type, not an explicit operand. For example, the expression <code>CAST(1 + 2 AS DOUBLE)</code> will
         * become a call to <code>CAST</code> with the expression <code>1 + 2</code> as its only operand.
         *
         * The <code>RexCall</code> form can also have a type which contains a <code>NOT NULL</code> constraint. When this expression is implemented, if the value is NULL, an exception will be thrown.
         */
        register( OperatorName.CAST, new SqlCastFunction() );

        /*
         * The SQL <code>EXTRACT</code> operator. Extracts a specified field value from a DATETIME or an INTERVAL. E.g.<br>
         * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code>23</code>
         */
        register( OperatorName.EXTRACT, new SqlExtractFunction() );

        /*
         * The SQL <code>YEAR</code> operator. Returns the Year from a DATETIME  E.g.<br>
         * <code>YEAR(date '2008-9-23')</code> returns <code>2008</code>
         */
        register( OperatorName.YEAR, new SqlDatePartFunction( "YEAR", TimeUnit.YEAR ) );

        /*
         * The SQL <code>QUARTER</code> operator. Returns the Quarter from a DATETIME  E.g.<br>
         * <code>QUARTER(date '2008-9-23')</code> returns <code>3</code>
         */
        register( OperatorName.QUARTER, new SqlDatePartFunction( "QUARTER", TimeUnit.QUARTER ) );

        /*
         * The SQL <code>MONTH</code> operator. Returns the Month from a DATETIME  E.g.<br>
         * <code>MONTH(date '2008-9-23')</code> returns <code>9</code>
         */
        register( OperatorName.MONTH, new SqlDatePartFunction( "MONTH", TimeUnit.MONTH ) );

        /*
         * The SQL <code>WEEK</code> operator. Returns the Week from a DATETIME  E.g.<br>
         * <code>WEEK(date '2008-9-23')</code> returns <code>39</code>
         */
        register( OperatorName.WEEK, new SqlDatePartFunction( "WEEK", TimeUnit.WEEK ) );

        /*
         * The SQL <code>DAYOFYEAR</code> operator. Returns the DOY from a DATETIME  E.g.<br>
         * <code>DAYOFYEAR(date '2008-9-23')</code> returns <code>267</code>
         */
        register( OperatorName.DAYOFYEAR, new SqlDatePartFunction( "DAYOFYEAR", TimeUnit.DOY ) );

        /*
         * The SQL <code>DAYOFMONTH</code> operator. Returns the Day from a DATETIME  E.g.<br>
         * <code>DAYOFMONTH(date '2008-9-23')</code> returns <code>23</code>
         */
        register( OperatorName.DAYOFMONTH, new SqlDatePartFunction( "DAYOFMONTH", TimeUnit.DAY ) );

        /*
         * The SQL <code>DAYOFWEEK</code> operator. Returns the DOW from a DATETIME  E.g.<br>
         * <code>DAYOFWEEK(date '2008-9-23')</code> returns <code>2</code>
         */
        register( OperatorName.DAYOFWEEK, new SqlDatePartFunction( "DAYOFWEEK", TimeUnit.DOW ) );

        /*
         * The SQL <code>HOUR</code> operator. Returns the Hour from a DATETIME  E.g.<br>
         * <code>HOUR(timestamp '2008-9-23 01:23:45')</code> returns <code>1</code>
         */
        register( OperatorName.HOUR, new SqlDatePartFunction( "HOUR", TimeUnit.HOUR ) );

        /*
         * The SQL <code>MINUTE</code> operator. Returns the Minute from a DATETIME  E.g.<br>
         * <code>MINUTE(timestamp '2008-9-23 01:23:45')</code> returns <code>23</code>
         */
        register( OperatorName.MINUTE, new SqlDatePartFunction( "MINUTE", TimeUnit.MINUTE ) );

        /*
         * The SQL <code>SECOND</code> operator. Returns the Second from a DATETIME  E.g.<br>
         * <code>SECOND(timestamp '2008-9-23 01:23:45')</code> returns <code>45</code>
         */
        register( OperatorName.SECOND, new SqlDatePartFunction( "SECOND", TimeUnit.SECOND ) );

        /*
         * The ELEMENT operator, used to convert a multiset with only one item to a "regular" type. Example ... log(ELEMENT(MULTISET[1])) ...
         */
        register(
                OperatorName.ELEMENT,
                new SqlFunction(
                        "ELEMENT",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.MULTISET_ELEMENT_NULLABLE,
                        null,
                        OperandTypes.COLLECTION,
                        FunctionCategory.SYSTEM ) );

        /*
         * The item operator {@code [ ... ]}, used to access a given element of an array or map. For example, {@code myArray[3]} or {@code "myMap['foo']"}.
         *
         * The SQL standard calls the ARRAY variant a &lt;array element reference&gt;. Index is 1-based. The standard says to raise "data exception - array element error"
         * but we currently return null.
         *
         * MAP is not standard SQL.
         */
        register( OperatorName.ITEM, new SqlItemOperator() );

        /*
         * The ARRAY Value Constructor. e.g. "<code>ARRAY[1, 2, 3]</code>".
         */
        register( OperatorName.ARRAY_VALUE_CONSTRUCTOR, new SqlArrayValueConstructor() );

        /*
         * The MAP Value Constructor, e.g. "<code>MAP['washington', 1, 'obama', 44]</code>".
         */
        register( OperatorName.MAP_VALUE_CONSTRUCTOR, new SqlMapValueConstructor() );

        /*
         * The internal "$SLICE" operator takes a multiset of records and returns a multiset of the first column of those records.
         *
         * It is introduced when multisets of scalar types are created, in order to keep types consistent. For example, <code>MULTISET [5]</code> has type <code>INTEGER MULTISET</code> but is translated to an expression of type
         * <code>RECORD(INTEGER EXPR$0) MULTISET</code> because in our internal representation of multisets, every element must be a record. Applying the "$SLICE" operator to this result converts the type back to an
         * <code>INTEGER MULTISET</code> multiset value.
         *
         * <code>$SLICE</code> is often translated away when the multiset type is converted back to scalar values.
         */
        register(
                OperatorName.SLICE,
                new SqlInternalOperator(
                        "$SLICE",
                        Kind.OTHER,
                        0,
                        false,
                        ReturnTypes.MULTISET_PROJECT0,
                        null,
                        OperandTypes.RECORD_COLLECTION ) {
                } );

        /*
         * The internal "$ELEMENT_SLICE" operator returns the first field of the only element of a multiset.
         *
         * It is introduced when multisets of scalar types are created, in order to keep types consistent. For example, <code>ELEMENT(MULTISET [5])</code> is translated to <code>$ELEMENT_SLICE(MULTISET (VALUES ROW (5 EXPR$0))</code>
         * It is translated away when the multiset type is converted back to scalar values.
         *
         * NOTE: jhyde, 2006/1/9: Usages of this operator are commented out, but I'm not deleting the operator, because some multiset tests are disabled, and we may need this operator to get them working!
         */
        register(
                OperatorName.ELEMENT_SLICE,
                new SqlInternalOperator(
                        "$ELEMENT_SLICE",
                        Kind.OTHER,
                        0,
                        false,
                        ReturnTypes.MULTISET_RECORD,
                        null,
                        OperandTypes.MULTISET ) {
                    @Override
                    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
                        SqlUtil.unparseFunctionSyntax( this, writer, call );
                    }
                } );

        /*
         * The internal "$SCALAR_QUERY" operator returns a scalar value from a record type. It assumes the record type only has one field, and returns that field as the output.
         */
        register(
                OperatorName.SCALAR_QUERY,
                new SqlInternalOperator(
                        "$SCALAR_QUERY",
                        Kind.SCALAR_QUERY,
                        0,
                        false,
                        ReturnTypes.RECORD_TO_SCALAR,
                        null,
                        OperandTypes.RECORD_TO_SCALAR ) {
                    @Override
                    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
                        final SqlWriter.Frame frame = writer.startList( " (", ") " );
                        ((SqlNode) call.operand( 0 )).unparse( writer, 0, 0 );
                        writer.endList( frame );
                    }


                    @Override
                    public boolean argumentMustBeScalar( int ordinal ) {
                        // Obvious, really.
                        return false;
                    }
                } );

        /*
         * The internal {@code $STRUCT_ACCESS} operator is used to access a field of a record.
         *
         * In contrast with {@link #DOT} operator, it never appears in an {@link SqlNode} tree and allows to access fields by position and not by name.
         */
        register( OperatorName.STRUCT_ACCESS, new SqlInternalOperator( "$STRUCT_ACCESS", Kind.OTHER ) );

        /*
         * The CARDINALITY operator, used to retrieve the number of elements in a MULTISET, ARRAY or MAP.
         */
        register(
                OperatorName.CARDINALITY,
                new SqlFunction(
                        "CARDINALITY",
                        Kind.OTHER_FUNCTION,
                        ReturnTypes.INTEGER_NULLABLE,
                        null,
                        OperandTypes.COLLECTION_OR_MAP,
                        FunctionCategory.SYSTEM ) );

        /*
         * The COLLECT operator. Multiset aggregator function.
         */
        register(
                OperatorName.COLLECT,
                new SqlAggFunction(
                        "COLLECT",
                        null,
                        Kind.COLLECT,
                        ReturnTypes.TO_MULTISET,
                        null,
                        OperandTypes.ANY,
                        FunctionCategory.SYSTEM,
                        false,
                        false,
                        Optionality.OPTIONAL ) {
                } );

        /*
         * The FUSION operator. Multiset aggregator function.
         */
        register(
                OperatorName.FUSION,
                new SqlAggFunction(
                        "FUSION",
                        null,
                        Kind.FUSION,
                        ReturnTypes.ARG0,
                        null,
                        OperandTypes.MULTISET,
                        FunctionCategory.SYSTEM,
                        false,
                        false,
                        Optionality.FORBIDDEN ) {
                } );

        /*
         * The sequence next value function: <code>NEXT VALUE FOR sequence</code>
         */
        register( OperatorName.NEXT_VALUE, new SqlSequenceValueOperator( Kind.NEXT_VALUE ) );

        /*
         * The sequence current value function: <code>CURRENT VALUE FOR sequence</code>
         */
        register( OperatorName.CURRENT_VALUE, new SqlSequenceValueOperator( Kind.CURRENT_VALUE ) );

        /*
         * The <code>TABLESAMPLE</code> operator.
         *
         * Examples:
         *
         * <ul>
         * <li><code>&lt;query&gt; TABLESAMPLE SUBSTITUTE('sampleName')</code> (non-standard)</li>
         * <li><code>&lt;query&gt; TABLESAMPLE BERNOULLI(&lt;percent&gt;) [REPEATABLE(&lt;seed&gt;)]</code> (standard, but not implemented for FTRS yet)</li>
         * <li><code>&lt;query&gt; TABLESAMPLE SYSTEM(&lt;percent&gt;) [REPEATABLE(&lt;seed&gt;)]</code> (standard, but not implemented for FTRS yet)</li>
         * </ul>
         *
         * Operand #0 is a query or table; Operand #1 is a {@link SqlSampleSpec} wrapped in a {@link SqlLiteral}.
         */
        register(
                OperatorName.TABLESAMPLE,
                new SqlSpecialOperator(
                        "TABLESAMPLE",
                        Kind.TABLESAMPLE,
                        20,
                        true,
                        ReturnTypes.ARG0,
                        null,
                        OperandTypes.VARIADIC ) {
                    @Override
                    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
                        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, 0 );
                        writer.keyword( "TABLESAMPLE" );
                        ((SqlNode) call.operand( 1 )).unparse( writer, 0, rightPrec );
                    }
                } );

        /*
         * The {@code TUMBLE} group function.
         */
        register(
                OperatorName.TUMBLE,
                new SqlGroupedWindowFunction(
                        Kind.TUMBLE.name(),
                        Kind.TUMBLE,
                        null,
                        ReturnTypes.ARG0,
                        null,
                        OperandTypes.or( OperandTypes.DATETIME_INTERVAL, OperandTypes.DATETIME_INTERVAL_TIME ),
                        FunctionCategory.SYSTEM ) {
                    @Override
                    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
                        return ImmutableList.of(
                                OperatorRegistry.get( OperatorName.TUMBLE_START, SqlGroupedWindowFunction.class ),
                                OperatorRegistry.get( OperatorName.TUMBLE_END, SqlGroupedWindowFunction.class ) );
                    }
                } );

        /*
         * The {@code TUMBLE_START} auxiliary function of the {@code TUMBLE} group function.
         */
        register( OperatorName.TUMBLE_START, OperatorRegistry.get( OperatorName.TUMBLE, SqlGroupedWindowFunction.class ).auxiliary( Kind.TUMBLE_START ) );

        /*
         * The {@code TUMBLE_END} auxiliary function of the {@code TUMBLE} group function.
         */
        register( OperatorName.TUMBLE_END, OperatorRegistry.get( OperatorName.TUMBLE, SqlGroupedWindowFunction.class ).auxiliary( Kind.TUMBLE_END ) );

        /*
         * The {@code HOP} group function.
         */
        register(
                OperatorName.HOP,
                new SqlGroupedWindowFunction(
                        Kind.HOP.name(),
                        Kind.HOP,
                        null,
                        ReturnTypes.ARG0,
                        null,
                        OperandTypes.or( OperandTypes.DATETIME_INTERVAL_INTERVAL, OperandTypes.DATETIME_INTERVAL_INTERVAL_TIME ),
                        FunctionCategory.SYSTEM ) {
                    @Override
                    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
                        return ImmutableList.of(
                                OperatorRegistry.get( OperatorName.HOP_START, SqlGroupedWindowFunction.class ),
                                OperatorRegistry.get( OperatorName.HOP_END, SqlGroupedWindowFunction.class )
                        );
                    }
                } );

        /*
         * The {@code HOP_START} auxiliary function of the {@code HOP} group function.
         */
        register( OperatorName.HOP_START, OperatorRegistry.get( OperatorName.HOP, SqlGroupedWindowFunction.class ).auxiliary( Kind.HOP_START ) );

        /*
         * The {@code HOP_END} auxiliary function of the {@code HOP} group function.
         */
        register( OperatorName.HOP_END, OperatorRegistry.get( OperatorName.HOP, SqlGroupedWindowFunction.class ).auxiliary( Kind.HOP_END ) );

        /*
         * The {@code SESSION} group function.
         */
        register(
                OperatorName.SESSION,
                new SqlGroupedWindowFunction(
                        Kind.SESSION.name(),
                        Kind.SESSION,
                        null,
                        ReturnTypes.ARG0,
                        null,
                        OperandTypes.or( OperandTypes.DATETIME_INTERVAL, OperandTypes.DATETIME_INTERVAL_TIME ),
                        FunctionCategory.SYSTEM ) {
                    @Override
                    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
                        return ImmutableList.of(
                                OperatorRegistry.get( OperatorName.SESSION_START, SqlGroupedWindowFunction.class ),
                                OperatorRegistry.get( OperatorName.SESSION_END, SqlGroupedWindowFunction.class )
                        );
                    }
                } );

        /*
         * The {@code SESSION_START} auxiliary function of the {@code SESSION} group function.
         */
        register( OperatorName.SESSION_START, OperatorRegistry.get( OperatorName.SESSION, SqlGroupedWindowFunction.class ).auxiliary( Kind.SESSION_START ) );

        /*
         * The {@code SESSION_END} auxiliary function of the {@code SESSION} group function.
         */
        register( OperatorName.SESSION_END, OperatorRegistry.get( OperatorName.SESSION, SqlGroupedWindowFunction.class ).auxiliary( Kind.SESSION_END ) );

        /*
         * {@code |} operator to create alternate patterns within {@code MATCH_RECOGNIZE}.
         *
         * If {@code p1} and {@code p2} are patterns then {@code p1 | p2} is a pattern that matches {@code p1} or {@code p2}.
         */
        register(
                OperatorName.PATTERN_ALTER,
                new SqlBinaryOperator(
                        "|",
                        Kind.PATTERN_ALTER,
                        70,
                        true,
                        null,
                        null,
                        null ) );

        /*
         * Operator to concatenate patterns within {@code MATCH_RECOGNIZE}.
         *
         * If {@code p1} and {@code p2} are patterns then {@code p1 p2} is a pattern that matches {@code p1} followed by {@code p2}.
         */
        register(
                OperatorName.PATTERN_CONCAT,
                new SqlBinaryOperator(
                        "",
                        Kind.PATTERN_CONCAT,
                        80,
                        true,
                        null,
                        null,
                        null ) );

        /*
         * Operator to quantify patterns within {@code MATCH_RECOGNIZE}.
         *
         * If {@code p} is a pattern then {@code p{3, 5}} is a pattern that matches between 3 and 5 occurrences of {@code p}.
         */
        register(
                OperatorName.PATTERN_QUANTIFIER,
                new SqlSpecialOperator( "PATTERN_QUANTIFIER", Kind.PATTERN_QUANTIFIER, 90 ) {
                    @Override
                    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
                        ((SqlNode) call.operand( 0 )).unparse( writer, this.getLeftPrec(), this.getRightPrec() );
                        int startNum = ((SqlNumericLiteral) call.operand( 1 )).intValue( true );
                        SqlNumericLiteral endRepNum = call.operand( 2 );
                        boolean isReluctant = ((SqlLiteral) call.operand( 3 )).booleanValue();
                        int endNum = endRepNum.intValue( true );
                        if ( startNum == endNum ) {
                            writer.keyword( "{ " + startNum + " }" );
                        } else {
                            if ( endNum == -1 ) {
                                if ( startNum == 0 ) {
                                    writer.keyword( "*" );
                                } else if ( startNum == 1 ) {
                                    writer.keyword( "+" );
                                } else {
                                    writer.keyword( "{ " + startNum + ", }" );
                                }
                            } else {
                                if ( startNum == 0 && endNum == 1 ) {
                                    writer.keyword( "?" );
                                } else if ( startNum == -1 ) {
                                    writer.keyword( "{ , " + endNum + " }" );
                                } else {
                                    writer.keyword( "{ " + startNum + ", " + endNum + " }" );
                                }
                            }
                            if ( isReluctant ) {
                                writer.keyword( "?" );
                            }
                        }
                    }
                } );

        /*
         * {@code PERMUTE} operator to combine patterns within {@code MATCH_RECOGNIZE}.
         *
         * If {@code p1} and {@code p2} are patterns then {@code PERMUTE (p1, p2)} is a pattern that matches all permutations of {@code p1} and {@code p2}.
         */
        register(
                OperatorName.PATTERN_PERMUTE,
                new SqlSpecialOperator( "PATTERN_PERMUTE", Kind.PATTERN_PERMUTE, 100 ) {
                    @Override
                    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
                        writer.keyword( "PERMUTE" );
                        SqlWriter.Frame frame = writer.startList( "(", ")" );
                        for ( int i = 0; i < call.getOperandList().size(); i++ ) {
                            SqlNode pattern = (SqlNode) call.getOperandList().get( i );
                            pattern.unparse( writer, 0, 0 );
                            if ( i != call.getOperandList().size() - 1 ) {
                                writer.print( "," );
                            }
                        }
                        writer.endList( frame );
                    }
                } );

        /*
         * {@code EXCLUDE} operator within {@code MATCH_RECOGNIZE}.
         *
         * If {@code p} is a pattern then {@code {- p -} }} is a pattern that excludes {@code p} from the output.
         */
        register(
                OperatorName.PATTERN_EXCLUDE,
                new SqlSpecialOperator( "PATTERN_EXCLUDE", Kind.PATTERN_EXCLUDED, 100 ) {
                    @Override
                    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
                        SqlWriter.Frame frame = writer.startList( "{-", "-}" );
                        SqlNode node = (SqlNode) call.getOperandList().get( 0 );
                        node.unparse( writer, 0, 0 );
                        writer.endList( frame );
                    }
                } );

        /*
         * Operator for array elements in different models.
         */
        register(
                OperatorName.CROSS_MODEL_ITEM,
                new LangFunctionOperator( OperatorName.CROSS_MODEL_ITEM.name(), Kind.CROSS_MODEL_ITEM ) );

        /*
         * Operator which transforms a value to JSON.
         */
        register( OperatorName.TO_JSON, new LangFunctionOperator( OperatorName.TO_JSON.name(), Kind.OTHER ) );

        isInit = true;
    }


    private static void register( OperatorName key, Operator operator ) {
        OperatorRegistry.register( key, operator );
    }

}
