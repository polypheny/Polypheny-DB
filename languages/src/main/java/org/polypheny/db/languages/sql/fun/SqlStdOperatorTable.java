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

package org.polypheny.db.languages.sql.fun;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.avatica.util.TimeUnit;
import org.polypheny.db.core.Conformance;
import org.polypheny.db.core.FunctionCategory;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Literal;
import org.polypheny.db.core.Modality;
import org.polypheny.db.core.OperatorTable;
import org.polypheny.db.core.StdOperatorRegistry;
import org.polypheny.db.core.json.JsonConstructorNullClause;
import org.polypheny.db.languages.sql.SqlAggFunction;
import org.polypheny.db.languages.sql.SqlAsOperator;
import org.polypheny.db.languages.sql.SqlBasicCall;
import org.polypheny.db.languages.sql.SqlBinaryOperator;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlFilterOperator;
import org.polypheny.db.languages.sql.SqlFunction;
import org.polypheny.db.languages.sql.SqlGroupedWindowFunction;
import org.polypheny.db.languages.sql.SqlInternalOperator;
import org.polypheny.db.languages.sql.SqlLateralOperator;
import org.polypheny.db.languages.sql.SqlLiteral;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlNumericLiteral;
import org.polypheny.db.languages.sql.SqlOperator;
import org.polypheny.db.languages.sql.SqlOverOperator;
import org.polypheny.db.languages.sql.SqlPostfixOperator;
import org.polypheny.db.languages.sql.SqlPrefixOperator;
import org.polypheny.db.languages.sql.SqlProcedureCallOperator;
import org.polypheny.db.languages.sql.SqlRankFunction;
import org.polypheny.db.languages.sql.SqlSampleSpec;
import org.polypheny.db.languages.sql.SqlSetOperator;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.SqlUnnestOperator;
import org.polypheny.db.languages.sql.SqlUtil;
import org.polypheny.db.languages.sql.SqlValuesOperator;
import org.polypheny.db.languages.sql.SqlWindow;
import org.polypheny.db.languages.sql.SqlWithinGroupOperator;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.languages.sql.util.ReflectiveSqlOperatorTable;
import org.polypheny.db.languages.sql2rel.AuxiliaryConverter;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Optionality;
import org.polypheny.db.util.Pair;


/**
 * Implementation of {@link OperatorTable} containing the standard operators and functions.
 */
public class SqlStdOperatorTable extends ReflectiveSqlOperatorTable {

    /**
     * The standard operator table.
     */
    private static SqlStdOperatorTable instance;


    static {
        StdOperatorRegistry.register( "DEFAULT", new SqlDefaultOperator() );

        /**
         * Logical greater-than operator, '<code>&gt;</code>'.
         */
        StdOperatorRegistry.register( "GREATER_THAN", new SqlBinaryOperator(
                ">",
                Kind.GREATER_THAN,
                30,
                true,
                ReturnTypes.BOOLEAN_NULLABLE,
                InferTypes.FIRST_KNOWN,
                OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED ) );

    }


    //-------------------------------------------------------------
    //                   SET OPERATORS
    //-------------------------------------------------------------
    // The set operators can be compared to the arithmetic operators
    // UNION -> +
    // EXCEPT -> -
    // INTERSECT -> *
    // which explains the different precedence values
    public static final SqlSetOperator UNION = new SqlSetOperator( "UNION", Kind.UNION, 14, false );

    public static final SqlSetOperator UNION_ALL = new SqlSetOperator( "UNION ALL", Kind.UNION, 14, true );

    public static final SqlSetOperator EXCEPT = new SqlSetOperator( "EXCEPT", Kind.EXCEPT, 14, false );

    public static final SqlSetOperator EXCEPT_ALL = new SqlSetOperator( "EXCEPT ALL", Kind.EXCEPT, 14, true );

    public static final SqlSetOperator INTERSECT = new SqlSetOperator( "INTERSECT", Kind.INTERSECT, 18, false );

    public static final SqlSetOperator INTERSECT_ALL = new SqlSetOperator( "INTERSECT ALL", Kind.INTERSECT, 18, true );

    /**
     * The {@code MULTISET UNION DISTINCT} operator.
     */
    public static final SqlMultisetSetOperator MULTISET_UNION_DISTINCT = new SqlMultisetSetOperator( "MULTISET UNION DISTINCT", 14, false );

    /**
     * The {@code MULTISET UNION [ALL]} operator.
     */
    public static final SqlMultisetSetOperator MULTISET_UNION = new SqlMultisetSetOperator( "MULTISET UNION ALL", 14, true );

    /**
     * The {@code MULTISET EXCEPT DISTINCT} operator.
     */
    public static final SqlMultisetSetOperator MULTISET_EXCEPT_DISTINCT = new SqlMultisetSetOperator( "MULTISET EXCEPT DISTINCT", 14, false );

    /**
     * The {@code MULTISET EXCEPT [ALL]} operator.
     */
    public static final SqlMultisetSetOperator MULTISET_EXCEPT = new SqlMultisetSetOperator( "MULTISET EXCEPT ALL", 14, true );

    /**
     * The {@code MULTISET INTERSECT DISTINCT} operator.
     */
    public static final SqlMultisetSetOperator MULTISET_INTERSECT_DISTINCT = new SqlMultisetSetOperator( "MULTISET INTERSECT DISTINCT", 18, false );

    /**
     * The {@code MULTISET INTERSECT [ALL]} operator.
     */
    public static final SqlMultisetSetOperator MULTISET_INTERSECT = new SqlMultisetSetOperator( "MULTISET INTERSECT ALL", 18, true );

    //-------------------------------------------------------------
    //                   BINARY OPERATORS
    //-------------------------------------------------------------

    /**
     * Logical <code>AND</code> operator.
     */
    public static final SqlBinaryOperator AND =
            new SqlBinaryOperator(
                    "AND",
                    Kind.AND,
                    24,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN_BOOLEAN );

    /**
     * <code>AS</code> operator associates an expression in the SELECT clause with an alias.
     */
    public static final SqlAsOperator AS = new SqlAsOperator();

    /**
     * <code>ARGUMENT_ASSIGNMENT</code> operator (<code>=&lt;</code>) assigns an argument to a function call to a particular named parameter.
     */
    public static final SqlSpecialOperator ARGUMENT_ASSIGNMENT = new SqlArgumentAssignmentOperator();

    /**
     * <code>DEFAULT</code> operator indicates that an argument to a function call is to take its default value..
     */
    public static final SqlSpecialOperator DEFAULT = new SqlDefaultOperator();

    /**
     * <code>FILTER</code> operator filters which rows are included in an aggregate function.
     */
    public static final SqlFilterOperator FILTER = new SqlFilterOperator();

    /**
     * <code>WITHIN_GROUP</code> operator performs aggregations on ordered data input.
     */
    public static final SqlWithinGroupOperator WITHIN_GROUP = new SqlWithinGroupOperator();

    /**
     * {@code CUBE} operator, occurs within {@code GROUP BY} clause or nested within a {@code GROUPING SETS}.
     */
    public static final SqlInternalOperator CUBE = new SqlRollupOperator( "CUBE", Kind.CUBE );

    /**
     * {@code ROLLUP} operator, occurs within {@code GROUP BY} clause or nested within a {@code GROUPING SETS}.
     */
    public static final SqlInternalOperator ROLLUP = new SqlRollupOperator( "ROLLUP", Kind.ROLLUP );

    /**
     * {@code GROUPING SETS} operator, occurs within {@code GROUP BY} clause or nested within a {@code GROUPING SETS}.
     */
    public static final SqlInternalOperator GROUPING_SETS = new SqlRollupOperator( "GROUPING SETS", Kind.GROUPING_SETS );

    /**
     * {@code GROUPING(c1 [, c2, ...])} function.
     *
     * Occurs in similar places to an aggregate function ({@code SELECT}, {@code HAVING} clause, etc. of an aggregate query), but not technically an aggregate function.
     */
    public static final SqlGroupingFunction GROUPING = new SqlGroupingFunction( "GROUPING" );

    /**
     * {@code GROUP_ID()} function. (Oracle-specific.)
     */
    public static final SqlGroupIdFunction GROUP_ID = new SqlGroupIdFunction();

    /**
     * {@code GROUPING_ID} function is a synonym for {@code GROUPING}.
     *
     * Some history. The {@code GROUPING} function is in the SQL standard, and originally supported only one argument. {@code GROUPING_ID} is not standard (though supported in Oracle and SQL Server)
     * and supports one or more arguments.
     *
     * The SQL standard has changed to allow {@code GROUPING} to have multiple arguments. It is now equivalent to {@code GROUPING_ID}, so we made {@code GROUPING_ID} a synonym for {@code GROUPING}.
     */
    public static final SqlGroupingFunction GROUPING_ID = new SqlGroupingFunction( "GROUPING_ID" );

    /**
     * {@code EXTEND} operator.
     */
    public static final SqlInternalOperator EXTEND = new SqlExtendOperator();

    /**
     * String concatenation operator, '<code>||</code>'.
     */
    public static final SqlBinaryOperator CONCAT =
            new SqlBinaryOperator(
                    "||",
                    Kind.OTHER,
                    60,
                    true,
                    ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE,
                    null,
                    OperandTypes.STRING_SAME_SAME );

    /**
     * Arithmetic division operator, '<code>/</code>'.
     */
    public static final SqlBinaryOperator DIVIDE =
            new SqlBinaryOperator(
                    "/",
                    Kind.DIVIDE,
                    60,
                    true,
                    ReturnTypes.QUOTIENT_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.DIVISION_OPERATOR );

    /**
     * Arithmetic remainder operator, '<code>%</code>', an alternative to {@link #MOD} allowed if under certain conformance levels.
     *
     * @see Conformance#isPercentRemainderAllowed
     */
    public static final SqlBinaryOperator PERCENT_REMAINDER =
            new SqlBinaryOperator(
                    "%",
                    Kind.MOD,
                    60,
                    true,
                    ReturnTypes.ARG1_NULLABLE,
                    null,
                    OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC );

    /**
     * The {@code RAND_INTEGER([seed, ] bound)} function, which yields a random integer, optionally with seed.
     */
    public static final SqlRandIntegerFunction RAND_INTEGER = new SqlRandIntegerFunction();

    /**
     * The {@code RAND([seed])} function, which yields a random double, optionally with seed.
     */
    public static final SqlRandFunction RAND = new SqlRandFunction();

    /**
     * Internal integer arithmetic division operator, '<code>/INT</code>'. This is only used to adjust scale for numerics. We distinguish it from user-requested division since some personalities want a floating-point computation,
     * whereas for the internal scaling use of division, we always want integer division.
     */
    public static final SqlBinaryOperator DIVIDE_INTEGER =
            new SqlBinaryOperator(
                    "/INT",
                    Kind.DIVIDE,
                    60,
                    true,
                    ReturnTypes.INTEGER_QUOTIENT_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.DIVISION_OPERATOR );

    /**
     * Dot operator, '<code>.</code>', used for referencing fields of records.
     */
    public static final SqlOperator DOT = new SqlDotOperator();

    /**
     * Logical equals operator, '<code>=</code>'.
     */
    public static final SqlBinaryOperator EQUALS =
            new SqlBinaryOperator(
                    "=",
                    Kind.EQUALS,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED );

    /**
     * Logical greater-than operator, '<code>&gt;</code>'.
     */
    public static final SqlBinaryOperator GREATER_THAN =
            new SqlBinaryOperator(
                    ">",
                    Kind.GREATER_THAN,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED );

    /**
     * <code>IS DISTINCT FROM</code> operator.
     */
    public static final SqlBinaryOperator IS_DISTINCT_FROM =
            new SqlBinaryOperator(
                    "IS DISTINCT FROM",
                    Kind.IS_DISTINCT_FROM,
                    30,
                    true,
                    ReturnTypes.BOOLEAN,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED );

    /**
     * <code>IS NOT DISTINCT FROM</code> operator. Is equivalent to <code>NOT(x IS DISTINCT FROM y)</code>
     */
    public static final SqlBinaryOperator IS_NOT_DISTINCT_FROM =
            new SqlBinaryOperator(
                    "IS NOT DISTINCT FROM",
                    Kind.IS_NOT_DISTINCT_FROM,
                    30,
                    true,
                    ReturnTypes.BOOLEAN,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED );

    /**
     * The internal <code>$IS_DIFFERENT_FROM</code> operator is the same as the user-level {@link #IS_DISTINCT_FROM} in all respects except that the test for equality on character datatypes treats trailing spaces as significant.
     */
    public static final SqlBinaryOperator IS_DIFFERENT_FROM =
            new SqlBinaryOperator(
                    "$IS_DIFFERENT_FROM",
                    Kind.OTHER,
                    30,
                    true,
                    ReturnTypes.BOOLEAN,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED );

    /**
     * Logical greater-than-or-equal operator, '<code>&gt;=</code>'.
     */
    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL =
            new SqlBinaryOperator(
                    ">=",
                    Kind.GREATER_THAN_OR_EQUAL,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED );

    /**
     * <code>IN</code> operator tests for a value's membership in a sub-query or a list of values.
     */
    public static final SqlBinaryOperator IN = new SqlInOperator( Kind.IN );

    /**
     * <code>NOT IN</code> operator tests for a value's membership in a sub-query or a list of values.
     */
    public static final SqlBinaryOperator NOT_IN = new SqlInOperator( Kind.NOT_IN );

    /**
     * The <code>&lt; SOME</code> operator (synonymous with <code>&lt; ANY</code>).
     */
    public static final SqlQuantifyOperator SOME_LT = new SqlQuantifyOperator( Kind.SOME, Kind.LESS_THAN );

    public static final SqlQuantifyOperator SOME_LE = new SqlQuantifyOperator( Kind.SOME, Kind.LESS_THAN_OR_EQUAL );

    public static final SqlQuantifyOperator SOME_GT = new SqlQuantifyOperator( Kind.SOME, Kind.GREATER_THAN );

    public static final SqlQuantifyOperator SOME_GE = new SqlQuantifyOperator( Kind.SOME, Kind.GREATER_THAN_OR_EQUAL );

    public static final SqlQuantifyOperator SOME_EQ = new SqlQuantifyOperator( Kind.SOME, Kind.EQUALS );

    public static final SqlQuantifyOperator SOME_NE = new SqlQuantifyOperator( Kind.SOME, Kind.NOT_EQUALS );

    /**
     * The <code>&lt; ALL</code> operator.
     */
    public static final SqlQuantifyOperator ALL_LT = new SqlQuantifyOperator( Kind.ALL, Kind.LESS_THAN );

    public static final SqlQuantifyOperator ALL_LE = new SqlQuantifyOperator( Kind.ALL, Kind.LESS_THAN_OR_EQUAL );

    public static final SqlQuantifyOperator ALL_GT = new SqlQuantifyOperator( Kind.ALL, Kind.GREATER_THAN );

    public static final SqlQuantifyOperator ALL_GE = new SqlQuantifyOperator( Kind.ALL, Kind.GREATER_THAN_OR_EQUAL );

    public static final SqlQuantifyOperator ALL_EQ = new SqlQuantifyOperator( Kind.ALL, Kind.EQUALS );

    public static final SqlQuantifyOperator ALL_NE = new SqlQuantifyOperator( Kind.ALL, Kind.NOT_EQUALS );

    /**
     * Logical less-than operator, '<code>&lt;</code>'.
     */
    public static final SqlBinaryOperator LESS_THAN =
            new SqlBinaryOperator(
                    "<",
                    Kind.LESS_THAN,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED );

    /**
     * Logical less-than-or-equal operator, '<code>&lt;=</code>'.
     */
    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL =
            new SqlBinaryOperator(
                    "<=",
                    Kind.LESS_THAN_OR_EQUAL,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED );

    /**
     * Infix arithmetic minus operator, '<code>-</code>'.
     *
     * Its precedence is less than the prefix {@link #UNARY_PLUS +} and {@link #UNARY_MINUS -} operators.
     */
    public static final SqlBinaryOperator MINUS =
            new SqlMonotonicBinaryOperator(
                    "-",
                    Kind.MINUS,
                    40,
                    true,

                    // Same type inference strategy as sum
                    ReturnTypes.NULLABLE_SUM,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.MINUS_OPERATOR );

    /**
     * Arithmetic multiplication operator, '<code>*</code>'.
     */
    public static final SqlBinaryOperator MULTIPLY =
            new SqlMonotonicBinaryOperator(
                    "*",
                    Kind.TIMES,
                    60,
                    true,
                    ReturnTypes.PRODUCT_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.MULTIPLY_OPERATOR );

    /**
     * Logical not-equals operator, '<code>&lt;&gt;</code>'.
     */
    public static final SqlBinaryOperator NOT_EQUALS =
            new SqlBinaryOperator(
                    "<>",
                    Kind.NOT_EQUALS,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED );

    /**
     * Logical <code>OR</code> operator.
     */
    public static final SqlBinaryOperator OR =
            new SqlBinaryOperator(
                    "OR",
                    Kind.OR,
                    22,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN_BOOLEAN );

    /**
     * Infix arithmetic plus operator, '<code>+</code>'.
     */
    public static final SqlBinaryOperator PLUS =
            new SqlMonotonicBinaryOperator(
                    "+",
                    Kind.PLUS,
                    40,
                    true,
                    ReturnTypes.NULLABLE_SUM,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.PLUS_OPERATOR );

    /**
     * Infix datetime plus operator, '<code>DATETIME + INTERVAL</code>'.
     */
    public static final SqlSpecialOperator DATETIME_PLUS = new SqlDatetimePlusOperator();

    /**
     * Multiset {@code MEMBER OF}, which returns whether a element belongs to a multiset.
     *
     * For example, the following returns <code>false</code>:
     *
     * <blockquote>
     * <code>'green' MEMBER OF MULTISET ['red','almost green','blue']</code>
     * </blockquote>
     */
    public static final SqlBinaryOperator MEMBER_OF = new SqlMultisetMemberOfOperator();

    /**
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
    public static final SqlBinaryOperator SUBMULTISET_OF =

            // TODO: check if precedence is correct
            new SqlBinaryOperator(
                    "SUBMULTISET OF",
                    Kind.OTHER,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    null,
                    OperandTypes.MULTISET_MULTISET );

    public static final SqlBinaryOperator NOT_SUBMULTISET_OF =

            // TODO: check if precedence is correct
            new SqlBinaryOperator(
                    "NOT SUBMULTISET OF",
                    Kind.OTHER,
                    30,
                    true,
                    ReturnTypes.BOOLEAN_NULLABLE,
                    null,
                    OperandTypes.MULTISET_MULTISET );

    //-------------------------------------------------------------
    //                   POSTFIX OPERATORS
    //-------------------------------------------------------------
    public static final SqlPostfixOperator DESC =
            new SqlPostfixOperator(
                    "DESC",
                    Kind.DESCENDING,
                    20,
                    ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.ANY );

    public static final SqlPostfixOperator NULLS_FIRST =
            new SqlPostfixOperator(
                    "NULLS FIRST",
                    Kind.NULLS_FIRST,
                    18,
                    ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.ANY );

    public static final SqlPostfixOperator NULLS_LAST =
            new SqlPostfixOperator(
                    "NULLS LAST",
                    Kind.NULLS_LAST,
                    18,
                    ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.ANY );

    public static final SqlPostfixOperator IS_NOT_NULL =
            new SqlPostfixOperator(
                    "IS NOT NULL",
                    Kind.IS_NOT_NULL,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.VARCHAR_1024,
                    OperandTypes.ANY );

    public static final SqlPostfixOperator IS_NULL =
            new SqlPostfixOperator(
                    "IS NULL",
                    Kind.IS_NULL,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.VARCHAR_1024,
                    OperandTypes.ANY );

    public static final SqlPostfixOperator IS_NOT_TRUE =
            new SqlPostfixOperator(
                    "IS NOT TRUE",
                    Kind.IS_NOT_TRUE,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN );

    public static final SqlPostfixOperator IS_TRUE =
            new SqlPostfixOperator(
                    "IS TRUE",
                    Kind.IS_TRUE,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN );

    public static final SqlPostfixOperator IS_NOT_FALSE =
            new SqlPostfixOperator(
                    "IS NOT FALSE",
                    Kind.IS_NOT_FALSE,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN );

    public static final SqlPostfixOperator IS_FALSE =
            new SqlPostfixOperator(
                    "IS FALSE",
                    Kind.IS_FALSE,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN );

    public static final SqlPostfixOperator IS_NOT_UNKNOWN =
            new SqlPostfixOperator(
                    "IS NOT UNKNOWN",
                    Kind.IS_NOT_NULL,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN );

    public static final SqlPostfixOperator IS_UNKNOWN =
            new SqlPostfixOperator(
                    "IS UNKNOWN",
                    Kind.IS_NULL,
                    28,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN );

    public static final SqlPostfixOperator IS_A_SET =
            new SqlPostfixOperator(
                    "IS A SET",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.MULTISET );

    public static final SqlPostfixOperator IS_NOT_A_SET =
            new SqlPostfixOperator(
                    "IS NOT A SET",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.MULTISET );

    public static final SqlPostfixOperator IS_EMPTY =
            new SqlPostfixOperator(
                    "IS EMPTY",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.COLLECTION_OR_MAP );

    public static final SqlPostfixOperator IS_NOT_EMPTY =
            new SqlPostfixOperator(
                    "IS NOT EMPTY",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.COLLECTION_OR_MAP );

    public static final SqlPostfixOperator IS_JSON_VALUE =
            new SqlPostfixOperator(
                    "IS JSON VALUE",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.CHARACTER );

    public static final SqlPostfixOperator IS_NOT_JSON_VALUE =
            new SqlPostfixOperator(
                    "IS NOT JSON VALUE",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.CHARACTER );

    public static final SqlPostfixOperator IS_JSON_OBJECT =
            new SqlPostfixOperator(
                    "IS JSON OBJECT",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.CHARACTER );

    public static final SqlPostfixOperator IS_NOT_JSON_OBJECT =
            new SqlPostfixOperator(
                    "IS NOT JSON OBJECT",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.CHARACTER );

    public static final SqlPostfixOperator IS_JSON_ARRAY =
            new SqlPostfixOperator(
                    "IS JSON ARRAY",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.CHARACTER );

    public static final SqlPostfixOperator IS_NOT_JSON_ARRAY =
            new SqlPostfixOperator(
                    "IS NOT JSON ARRAY",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.CHARACTER );

    public static final SqlPostfixOperator IS_JSON_SCALAR =
            new SqlPostfixOperator(
                    "IS JSON SCALAR",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.CHARACTER );

    public static final SqlPostfixOperator IS_NOT_JSON_SCALAR =
            new SqlPostfixOperator(
                    "IS NOT JSON SCALAR",
                    Kind.OTHER,
                    28,
                    ReturnTypes.BOOLEAN,
                    null,
                    OperandTypes.CHARACTER );


    //-------------------------------------------------------------
    //                   PREFIX OPERATORS
    //-------------------------------------------------------------
    public static final SqlPrefixOperator EXISTS =
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
            };

    public static final SqlPrefixOperator NOT =
            new SqlPrefixOperator(
                    "NOT",
                    Kind.NOT,
                    26,
                    ReturnTypes.ARG0,
                    InferTypes.BOOLEAN,
                    OperandTypes.BOOLEAN );

    /**
     * Prefix arithmetic minus operator, '<code>-</code>'.
     *
     * Its precedence is greater than the infix '{@link #PLUS +}' and '{@link #MINUS -}' operators.
     */
    public static final SqlPrefixOperator UNARY_MINUS =
            new SqlPrefixOperator(
                    "-",
                    Kind.MINUS_PREFIX,
                    80,
                    ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.NUMERIC_OR_INTERVAL );

    /**
     * Prefix arithmetic plus operator, '<code>+</code>'.
     *
     * Its precedence is greater than the infix '{@link #PLUS +}' and '{@link #MINUS -}' operators.
     */
    public static final SqlPrefixOperator UNARY_PLUS =
            new SqlPrefixOperator(
                    "+",
                    Kind.PLUS_PREFIX,
                    80,
                    ReturnTypes.ARG0,
                    InferTypes.RETURN_TYPE,
                    OperandTypes.NUMERIC_OR_INTERVAL );

    /**
     * Keyword which allows an identifier to be explicitly flagged as a table.
     * For example, <code>select * from (TABLE t)</code> or <code>TABLE t</code>. See also {@link #COLLECTION_TABLE}.
     */
    public static final SqlPrefixOperator EXPLICIT_TABLE =
            new SqlPrefixOperator(
                    "TABLE",
                    Kind.EXPLICIT_TABLE,
                    2,
                    null,
                    null,
                    null );

    /**
     * {@code FINAL} function to be used within {@code MATCH_RECOGNIZE}.
     */
    public static final SqlPrefixOperator FINAL =
            new SqlPrefixOperator(
                    "FINAL",
                    Kind.FINAL,
                    80,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.ANY );

    /**
     * {@code RUNNING} function to be used within {@code MATCH_RECOGNIZE}.
     */
    public static final SqlPrefixOperator RUNNING =
            new SqlPrefixOperator(
                    "RUNNING",
                    Kind.RUNNING,
                    80,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.ANY );

    //-------------------------------------------------------------
    // AGGREGATE OPERATORS
    //-------------------------------------------------------------
    /**
     * <code>SUM</code> aggregate function.
     */
    public static final SqlAggFunction SUM = new SqlSumAggFunction( null );

    /**
     * <code>COUNT</code> aggregate function.
     */
    public static final SqlAggFunction COUNT = new SqlCountAggFunction( "COUNT" );

    /**
     * <code>APPROX_COUNT_DISTINCT</code> aggregate function.
     */
    public static final SqlAggFunction APPROX_COUNT_DISTINCT = new SqlCountAggFunction( "APPROX_COUNT_DISTINCT" );

    /**
     * <code>MIN</code> aggregate function.
     */
    public static final SqlAggFunction MIN = new SqlMinMaxAggFunction( Kind.MIN );

    /**
     * <code>MAX</code> aggregate function.
     */
    public static final SqlAggFunction MAX = new SqlMinMaxAggFunction( Kind.MAX );

    /**
     * <code>LAST_VALUE</code> aggregate function.
     */
    public static final SqlAggFunction LAST_VALUE = new SqlFirstLastValueAggFunction( Kind.LAST_VALUE );

    /**
     * <code>ANY_VALUE</code> aggregate function.
     */
    public static final SqlAggFunction ANY_VALUE = new SqlAnyValueAggFunction( Kind.ANY_VALUE );

    /**
     * <code>FIRST_VALUE</code> aggregate function.
     */
    public static final SqlAggFunction FIRST_VALUE = new SqlFirstLastValueAggFunction( Kind.FIRST_VALUE );

    /**
     * <code>NTH_VALUE</code> aggregate function.
     */
    public static final SqlAggFunction NTH_VALUE = new SqlNthValueAggFunction( Kind.NTH_VALUE );

    /**
     * <code>LEAD</code> aggregate function.
     */
    public static final SqlAggFunction LEAD = new SqlLeadLagAggFunction( Kind.LEAD );

    /**
     * <code>LAG</code> aggregate function.
     */
    public static final SqlAggFunction LAG = new SqlLeadLagAggFunction( Kind.LAG );

    /**
     * <code>NTILE</code> aggregate function.
     */
    public static final SqlAggFunction NTILE = new SqlNtileAggFunction();

    /**
     * <code>SINGLE_VALUE</code> aggregate function.
     */
    public static final SqlAggFunction SINGLE_VALUE = new SqlSingleValueAggFunction( null );

    /**
     * <code>AVG</code> aggregate function.
     */
    public static final SqlAggFunction AVG = new SqlAvgAggFunction( Kind.AVG );

    /**
     * <code>STDDEV_POP</code> aggregate function.
     */
    public static final SqlAggFunction STDDEV_POP = new SqlAvgAggFunction( Kind.STDDEV_POP );

    /**
     * <code>REGR_COUNT</code> aggregate function.
     */
    public static final SqlAggFunction REGR_COUNT = new SqlRegrCountAggFunction( Kind.REGR_COUNT );

    /**
     * <code>REGR_SXX</code> aggregate function.
     */
    public static final SqlAggFunction REGR_SXX = new SqlCovarAggFunction( Kind.REGR_SXX );

    /**
     * <code>REGR_SYY</code> aggregate function.
     */
    public static final SqlAggFunction REGR_SYY = new SqlCovarAggFunction( Kind.REGR_SYY );

    /**
     * <code>COVAR_POP</code> aggregate function.
     */
    public static final SqlAggFunction COVAR_POP = new SqlCovarAggFunction( Kind.COVAR_POP );

    /**
     * <code>COVAR_SAMP</code> aggregate function.
     */
    public static final SqlAggFunction COVAR_SAMP = new SqlCovarAggFunction( Kind.COVAR_SAMP );

    /**
     * <code>STDDEV_SAMP</code> aggregate function.
     */
    public static final SqlAggFunction STDDEV_SAMP = new SqlAvgAggFunction( Kind.STDDEV_SAMP );

    /**
     * <code>STDDEV</code> aggregate function.
     */
    public static final SqlAggFunction STDDEV = new SqlAvgAggFunction( "STDDEV", Kind.STDDEV_SAMP );

    /**
     * <code>VAR_POP</code> aggregate function.
     */
    public static final SqlAggFunction VAR_POP = new SqlAvgAggFunction( Kind.VAR_POP );

    /**
     * <code>VAR_SAMP</code> aggregate function.
     */
    public static final SqlAggFunction VAR_SAMP = new SqlAvgAggFunction( Kind.VAR_SAMP );

    /**
     * <code>VARIANCE</code> aggregate function.
     */
    public static final SqlAggFunction VARIANCE = new SqlAvgAggFunction( "VARIANCE", Kind.VAR_SAMP );

    /**
     * <code>BIT_AND</code> aggregate function.
     */
    public static final SqlAggFunction BIT_AND = new SqlBitOpAggFunction( Kind.BIT_AND );

    /**
     * <code>BIT_OR</code> aggregate function.
     */
    public static final SqlAggFunction BIT_OR = new SqlBitOpAggFunction( Kind.BIT_OR );

    //-------------------------------------------------------------
    // WINDOW Aggregate Functions
    //-------------------------------------------------------------
    /**
     * <code>HISTOGRAM</code> aggregate function support. Used by window aggregate versions of MIN/MAX
     */
    public static final SqlAggFunction HISTOGRAM_AGG = new SqlHistogramAggFunction( null );

    /**
     * <code>HISTOGRAM_MIN</code> window aggregate function.
     */
    public static final SqlFunction HISTOGRAM_MIN =
            new SqlFunction(
                    "$HISTOGRAM_MIN",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_OR_STRING,
                    FunctionCategory.NUMERIC );

    /**
     * <code>HISTOGRAM_MAX</code> window aggregate function.
     */
    public static final SqlFunction HISTOGRAM_MAX =
            new SqlFunction(
                    "$HISTOGRAM_MAX",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_OR_STRING,
                    FunctionCategory.NUMERIC );

    /**
     * <code>HISTOGRAM_FIRST_VALUE</code> window aggregate function.
     */
    public static final SqlFunction HISTOGRAM_FIRST_VALUE =
            new SqlFunction(
                    "$HISTOGRAM_FIRST_VALUE",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_OR_STRING,
                    FunctionCategory.NUMERIC );

    /**
     * <code>HISTOGRAM_LAST_VALUE</code> window aggregate function.
     */
    public static final SqlFunction HISTOGRAM_LAST_VALUE =
            new SqlFunction(
                    "$HISTOGRAM_LAST_VALUE",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_OR_STRING,
                    FunctionCategory.NUMERIC );

    /**
     * <code>SUM0</code> aggregate function.
     */
    public static final SqlAggFunction SUM0 = new SqlSumEmptyIsZeroAggFunction();

    //-------------------------------------------------------------
    // WINDOW Rank Functions
    //-------------------------------------------------------------
    /**
     * <code>CUME_DIST</code> window function.
     */
    public static final SqlRankFunction CUME_DIST = new SqlRankFunction( Kind.CUME_DIST, ReturnTypes.FRACTIONAL_RANK, true );

    /**
     * <code>DENSE_RANK</code> window function.
     */
    public static final SqlRankFunction DENSE_RANK = new SqlRankFunction( Kind.DENSE_RANK, ReturnTypes.RANK, true );

    /**
     * <code>PERCENT_RANK</code> window function.
     */
    public static final SqlRankFunction PERCENT_RANK = new SqlRankFunction( Kind.PERCENT_RANK, ReturnTypes.FRACTIONAL_RANK, true );

    /**
     * <code>RANK</code> window function.
     */
    public static final SqlRankFunction RANK = new SqlRankFunction( Kind.RANK, ReturnTypes.RANK, true );

    /**
     * <code>ROW_NUMBER</code> window function.
     */
    public static final SqlRankFunction ROW_NUMBER = new SqlRankFunction( Kind.ROW_NUMBER, ReturnTypes.RANK, false );


    //-------------------------------------------------------------
    //                   SPECIAL OPERATORS
    //-------------------------------------------------------------
    public static final SqlRowOperator ROW = new SqlRowOperator( "ROW" );

    /**
     * A special operator for the subtraction of two DATETIMEs. The format of DATETIME subtraction is:
     *
     * <blockquote><code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")" &lt;interval qualifier&gt;</code></blockquote>
     *
     * This operator is special since it needs to hold the additional interval qualifier specification.
     */
    public static final SqlDatetimeSubtractionOperator MINUS_DATE = new SqlDatetimeSubtractionOperator();

    /**
     * The MULTISET Value Constructor. e.g. "<code>MULTISET[1,2,3]</code>".
     */
    public static final SqlMultisetValueConstructor MULTISET_VALUE = new SqlMultisetValueConstructor();

    /**
     * The MULTISET Query Constructor. e.g. "<code>SELECT dname, MULTISET(SELECT FROM emp WHERE deptno = dept.deptno) FROM dept</code>".
     */
    public static final SqlMultisetQueryConstructor MULTISET_QUERY = new SqlMultisetQueryConstructor();

    /**
     * The ARRAY Query Constructor. e.g. "<code>SELECT dname, ARRAY(SELECT FROM emp WHERE deptno = dept.deptno) FROM dept</code>".
     */
    public static final SqlMultisetQueryConstructor ARRAY_QUERY = new SqlArrayQueryConstructor();

    /**
     * The MAP Query Constructor. e.g. "<code>MAP(SELECT empno, deptno FROM emp)</code>".
     */
    public static final SqlMultisetQueryConstructor MAP_QUERY = new SqlMapQueryConstructor();

    /**
     * The CURSOR constructor. e.g. "<code>SELECT * FROM TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), 'name'))</code>".
     */
    public static final SqlCursorConstructor CURSOR = new SqlCursorConstructor();

    /**
     * The COLUMN_LIST constructor. e.g. the ROW() call in "<code>SELECT * FROM TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), ROW(name, empno)))</code>".
     */
    public static final SqlColumnListConstructor COLUMN_LIST = new SqlColumnListConstructor();

    /**
     * The <code>UNNEST</code> operator.
     */
    public static final SqlUnnestOperator UNNEST = new SqlUnnestOperator( false );

    /**
     * The <code>UNNEST WITH ORDINALITY</code> operator.
     */
    public static final SqlUnnestOperator UNNEST_WITH_ORDINALITY = new SqlUnnestOperator( true );

    /**
     * The <code>LATERAL</code> operator.
     */
    public static final SqlSpecialOperator LATERAL = new SqlLateralOperator( Kind.LATERAL );

    /**
     * The "table function derived table" operator, which a table-valued function into a relation, e.g. "<code>SELECT * FROM TABLE(ramp(5))</code>".
     *
     * This operator has function syntax (with one argument), whereas {@link #EXPLICIT_TABLE} is a prefix operator.
     */
    public static final SqlSpecialOperator COLLECTION_TABLE = new SqlCollectionTableOperator( "TABLE", Modality.RELATION );

    public static final SqlOverlapsOperator OVERLAPS = new SqlOverlapsOperator( Kind.OVERLAPS );

    public static final SqlOverlapsOperator CONTAINS = new SqlOverlapsOperator( Kind.CONTAINS );

    public static final SqlOverlapsOperator PRECEDES = new SqlOverlapsOperator( Kind.PRECEDES );

    public static final SqlOverlapsOperator IMMEDIATELY_PRECEDES = new SqlOverlapsOperator( Kind.IMMEDIATELY_PRECEDES );

    public static final SqlOverlapsOperator SUCCEEDS = new SqlOverlapsOperator( Kind.SUCCEEDS );

    public static final SqlOverlapsOperator IMMEDIATELY_SUCCEEDS = new SqlOverlapsOperator( Kind.IMMEDIATELY_SUCCEEDS );

    public static final SqlOverlapsOperator PERIOD_EQUALS = new SqlOverlapsOperator( Kind.PERIOD_EQUALS );

    public static final SqlSpecialOperator VALUES = new SqlValuesOperator();

    public static final SqlLiteralChainOperator LITERAL_CHAIN = new SqlLiteralChainOperator();

    public static final SqlThrowOperator THROW = new SqlThrowOperator();

    public static final SqlJsonValueExpressionOperator JSON_VALUE_EXPRESSION = new SqlJsonValueExpressionOperator( "JSON_VALUE_EXPRESSION", false );

    public static final SqlJsonValueExpressionOperator JSON_VALUE_EXPRESSION_EXCLUDED = new SqlJsonValueExpressionOperator( "JSON_VALUE_EXPRESSION_EXCLUDE", false );

    public static final SqlJsonValueExpressionOperator JSON_STRUCTURED_VALUE_EXPRESSION = new SqlJsonValueExpressionOperator( "JSON_STRUCTURED_VALUE_EXPRESSION", true );

    public static final SqlJsonApiCommonSyntaxOperator JSON_API_COMMON_SYNTAX = new SqlJsonApiCommonSyntaxOperator();

    public static final SqlFunction JSON_EXISTS = new SqlJsonExistsFunction();

    public static final SqlFunction JSON_VALUE = new SqlJsonValueFunction( "JSON_VALUE", false );

    public static final SqlFunction JSON_VALUE_ANY = new SqlJsonValueFunction( "JSON_VALUE_ANY", true );

    public static final SqlFunction JSON_QUERY = new SqlJsonQueryFunction();

    public static final SqlFunction JSON_OBJECT = new SqlJsonObjectFunction();

    public static final SqlJsonObjectAggAggFunction JSON_OBJECTAGG = new SqlJsonObjectAggAggFunction( "JSON_OBJECTAGG", JsonConstructorNullClause.NULL_ON_NULL );

    public static final SqlFunction JSON_ARRAY = new SqlJsonArrayFunction();

    public static final SqlJsonArrayAggAggFunction JSON_ARRAYAGG = new SqlJsonArrayAggAggFunction( "JSON_ARRAYAGG", JsonConstructorNullClause.NULL_ON_NULL );

    public static final SqlBetweenOperator BETWEEN = new SqlBetweenOperator( SqlBetweenOperator.Flag.ASYMMETRIC, false );

    public static final SqlBetweenOperator SYMMETRIC_BETWEEN = new SqlBetweenOperator( SqlBetweenOperator.Flag.SYMMETRIC, false );

    public static final SqlBetweenOperator NOT_BETWEEN = new SqlBetweenOperator( SqlBetweenOperator.Flag.ASYMMETRIC, true );

    public static final SqlBetweenOperator SYMMETRIC_NOT_BETWEEN = new SqlBetweenOperator( SqlBetweenOperator.Flag.SYMMETRIC, true );

    public static final SqlSpecialOperator NOT_LIKE = new SqlLikeOperator( "NOT LIKE", Kind.LIKE, true );

    public static final SqlSpecialOperator LIKE = new SqlLikeOperator( "LIKE", Kind.LIKE, false );

    public static final SqlSpecialOperator NOT_SIMILAR_TO = new SqlLikeOperator( "NOT SIMILAR TO", Kind.SIMILAR, true );

    public static final SqlSpecialOperator SIMILAR_TO = new SqlLikeOperator( "SIMILAR TO", Kind.SIMILAR, false );


    /**
     * Internal operator used to represent the ESCAPE clause of a LIKE or SIMILAR TO expression.
     */
    public static final SqlSpecialOperator ESCAPE = new SqlSpecialOperator( "ESCAPE", Kind.ESCAPE, 0 );

    public static final SqlCaseOperator CASE = SqlCaseOperator.INSTANCE;

    public static final SqlOperator PROCEDURE_CALL = new SqlProcedureCallOperator();

    public static final SqlOperator NEW = new SqlNewOperator();

    /**
     * The <code>OVER</code> operator, which applies an aggregate functions to a {@link SqlWindow window}.
     *
     * Operands are as follows:
     *
     * <ol>
     * <li>name of window function ({@link SqlCall})</li>
     * <li>window name ({@link Literal}) or window in-line specification (@link SqlWindowOperator})</li>
     * </ol>
     */
    public static final SqlBinaryOperator OVER = new SqlOverOperator();

    /**
     * An <code>REINTERPRET</code> operator is internal to the planner. When the physical storage of two types is the same, this operator may be used to reinterpret values of one type as the other. This operator is similar to a cast,
     * except that it does not alter the data value. Like a regular cast it accepts one operand and stores the target type as the return type. It performs an overflow check if it has <i>any</i> second operand, whether true or not.
     */
    public static final SqlSpecialOperator REINTERPRET =
            new SqlSpecialOperator( "Reinterpret", Kind.REINTERPRET ) {
                @Override
                public OperandCountRange getOperandCountRange() {
                    return PolyOperandCountRanges.between( 1, 2 );
                }
            };

    //-------------------------------------------------------------
    //                   FUNCTIONS
    //-------------------------------------------------------------

    /**
     * distance function: <code>DISTANCE(column, ARRAY[], METRIC, WEIGHTS)</code>.
     */
    public static final SqlFunction DISTANCE = new SqlDistanceFunction();

    /**
     * Get metadata of multimedia files
     */
    public static final SqlFunction META = new SqlMetaFunction();

    /**
     * The character substring function: <code>SUBSTRING(string FROM start [FOR length])</code>.
     *
     * If the length parameter is a constant, the length of the result is the minimum of the length of the input and that length. Otherwise it is the length of the input.
     */
    public static final SqlFunction SUBSTRING = new SqlSubstringFunction();

    /**
     * The {@code REPLACE(string, search, replace)} function. Not standard SQL, but in Oracle and Postgres.
     */
    public static final SqlFunction REPLACE =
            new SqlFunction(
                    "REPLACE",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE_VARYING,
                    null,
                    OperandTypes.STRING_STRING_STRING,
                    FunctionCategory.STRING );

    public static final SqlFunction CONVERT = new SqlConvertFunction( "CONVERT" );

    /**
     * The <code>TRANSLATE(<i>char_value</i> USING <i>translation_name</i>)</code> function alters the character set of a string value from one base character set to another.
     *
     * It is defined in the SQL standard. See also non-standard {@link OracleSqlOperatorTable#TRANSLATE3}.
     */
    public static final SqlFunction TRANSLATE = new SqlConvertFunction( "TRANSLATE" );

    public static final SqlFunction OVERLAY = new SqlOverlayFunction();

    /**
     * The "TRIM" function.
     */
    public static final SqlFunction TRIM = SqlTrimFunction.INSTANCE;

    public static final SqlFunction POSITION = new SqlPositionFunction();

    public static final SqlFunction CHAR_LENGTH =
            new SqlFunction(
                    "CHAR_LENGTH",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.CHARACTER,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction CHARACTER_LENGTH =
            new SqlFunction(
                    "CHARACTER_LENGTH",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.CHARACTER,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction UPPER =
            new SqlFunction(
                    "UPPER",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.CHARACTER,
                    FunctionCategory.STRING );

    public static final SqlFunction LOWER =
            new SqlFunction(
                    "LOWER",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.CHARACTER,
                    FunctionCategory.STRING );

    public static final SqlFunction INITCAP =
            new SqlFunction(
                    "INITCAP",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.CHARACTER,
                    FunctionCategory.STRING );

    /**
     * Uses SqlOperatorTable.useDouble for its return type since we don't know what the result type will be by just looking at the operand types. For example POW(int, int) can return a non integer if the second operand is negative.
     */
    public static final SqlFunction POWER =
            new SqlFunction(
                    "POWER",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction SQRT =
            new SqlFunction(
                    "SQRT",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    /**
     * Arithmetic remainder function {@code MOD}.
     *
     * @see #PERCENT_REMAINDER
     */
    public static final SqlFunction MOD =
            // Return type is same as divisor (2nd operand)
            // SQL2003 Part2 Section 6.27, Syntax Rules 9
            new SqlFunction(
                    "MOD",
                    Kind.MOD,
                    ReturnTypes.ARG1_NULLABLE,
                    null,
                    OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction LN =
            new SqlFunction(
                    "LN",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction LOG10 =
            new SqlFunction(
                    "LOG10",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction ABS =
            new SqlFunction(
                    "ABS",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.NUMERIC_OR_INTERVAL,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction ACOS =
            new SqlFunction(
                    "ACOS",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction ASIN =
            new SqlFunction(
                    "ASIN",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction ATAN =
            new SqlFunction(
                    "ATAN",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction ATAN2 =
            new SqlFunction(
                    "ATAN2",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction COS =
            new SqlFunction(
                    "COS",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction COT =
            new SqlFunction(
                    "COT",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction DEGREES =
            new SqlFunction(
                    "DEGREES",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction EXP =
            new SqlFunction(
                    "EXP",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction RADIANS =
            new SqlFunction(
                    "RADIANS",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction ROUND =
            new SqlFunction(
                    "ROUND",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_OPTIONAL_INTEGER,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction SIGN =
            new SqlFunction(
                    "SIGN",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction SIN =
            new SqlFunction(
                    "SIN",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );


    public static final SqlFunction TAN =
            new SqlFunction(
                    "TAN",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.DOUBLE_NULLABLE,
                    null,
                    OperandTypes.NUMERIC,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction TRUNCATE =
            new SqlFunction(
                    "TRUNCATE",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.NUMERIC_OPTIONAL_INTEGER,
                    FunctionCategory.NUMERIC );

    public static final SqlFunction PI =
            new SqlBaseContextVariable(
                    "PI",
                    ReturnTypes.DOUBLE,
                    FunctionCategory.NUMERIC );

    /**
     * {@code FIRST} function to be used within {@code MATCH_RECOGNIZE}.
     */
    public static final SqlFunction FIRST =
            new SqlFunction(
                    "FIRST",
                    Kind.FIRST,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.ANY_NUMERIC,
                    FunctionCategory.MATCH_RECOGNIZE );

    /**
     * {@code LAST} function to be used within {@code MATCH_RECOGNIZE}.
     */
    public static final SqlFunction LAST =
            new SqlFunction(
                    "LAST",
                    Kind.LAST,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.ANY_NUMERIC,
                    FunctionCategory.MATCH_RECOGNIZE );

    /**
     * {@code PREV} function to be used within {@code MATCH_RECOGNIZE}.
     */
    public static final SqlFunction PREV =
            new SqlFunction(
                    "PREV",
                    Kind.PREV,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.ANY_NUMERIC,
                    FunctionCategory.MATCH_RECOGNIZE );

    /**
     * {@code NEXT} function to be used within {@code MATCH_RECOGNIZE}.
     */
    public static final SqlFunction NEXT =
            new SqlFunction(
                    "NEXT",
                    Kind.NEXT,
                    ReturnTypes.ARG0_NULLABLE,
                    null,
                    OperandTypes.ANY_NUMERIC,
                    FunctionCategory.MATCH_RECOGNIZE );

    /**
     * {@code CLASSIFIER} function to be used within {@code MATCH_RECOGNIZE}.
     */
    public static final SqlFunction CLASSIFIER =
            new SqlFunction(
                    "CLASSIFIER",
                    Kind.CLASSIFIER,
                    ReturnTypes.VARCHAR_2000,
                    null,
                    OperandTypes.NILADIC,
                    FunctionCategory.MATCH_RECOGNIZE );

    /**
     * {@code MATCH_NUMBER} function to be used within {@code MATCH_RECOGNIZE}.
     */
    public static final SqlFunction MATCH_NUMBER =
            new SqlFunction(
                    "MATCH_NUMBER ",
                    Kind.MATCH_NUMBER,
                    ReturnTypes.BIGINT_NULLABLE,
                    null,
                    OperandTypes.NILADIC,
                    FunctionCategory.MATCH_RECOGNIZE );

    public static final SqlFunction NULLIF = new SqlNullifFunction();

    /**
     * The COALESCE builtin function.
     */
    public static final SqlFunction COALESCE = new SqlCoalesceFunction();

    /**
     * The <code>FLOOR</code> function.
     */
    public static final SqlFunction FLOOR = new SqlFloorFunction( Kind.FLOOR );

    /**
     * The <code>CEIL</code> function.
     */
    public static final SqlFunction CEIL = new SqlFloorFunction( Kind.CEIL );

    /**
     * The <code>USER</code> function.
     */
    public static final SqlFunction USER = new SqlStringContextVariable( "USER" );

    /**
     * The <code>CURRENT_USER</code> function.
     */
    public static final SqlFunction CURRENT_USER = new SqlStringContextVariable( "CURRENT_USER" );

    /**
     * The <code>SESSION_USER</code> function.
     */
    public static final SqlFunction SESSION_USER = new SqlStringContextVariable( "SESSION_USER" );

    /**
     * The <code>SYSTEM_USER</code> function.
     */
    public static final SqlFunction SYSTEM_USER = new SqlStringContextVariable( "SYSTEM_USER" );

    /**
     * The <code>CURRENT_PATH</code> function.
     */
    public static final SqlFunction CURRENT_PATH = new SqlStringContextVariable( "CURRENT_PATH" );

    /**
     * The <code>CURRENT_ROLE</code> function.
     */
    public static final SqlFunction CURRENT_ROLE = new SqlStringContextVariable( "CURRENT_ROLE" );

    /**
     * The <code>CURRENT_CATALOG</code> function.
     */
    public static final SqlFunction CURRENT_CATALOG = new SqlStringContextVariable( "CURRENT_CATALOG" );

    /**
     * The <code>CURRENT_SCHEMA</code> function.
     */
    public static final SqlFunction CURRENT_SCHEMA = new SqlStringContextVariable( "CURRENT_SCHEMA" );

    /**
     * The <code>LOCALTIME [(<i>precision</i>)]</code> function.
     */
    public static final SqlFunction LOCALTIME = new SqlAbstractTimeFunction( "LOCALTIME", PolyType.TIME );

    /**
     * The <code>LOCALTIMESTAMP [(<i>precision</i>)]</code> function.
     */
    public static final SqlFunction LOCALTIMESTAMP = new SqlAbstractTimeFunction( "LOCALTIMESTAMP", PolyType.TIMESTAMP );

    /**
     * The <code>CURRENT_TIME [(<i>precision</i>)]</code> function.
     */
    public static final SqlFunction CURRENT_TIME = new SqlAbstractTimeFunction( "CURRENT_TIME", PolyType.TIME );

    /**
     * The <code>CURRENT_TIMESTAMP [(<i>precision</i>)]</code> function.
     */
    public static final SqlFunction CURRENT_TIMESTAMP = new SqlAbstractTimeFunction( "CURRENT_TIMESTAMP", PolyType.TIMESTAMP );

    /**
     * The <code>CURRENT_DATE</code> function.
     */
    public static final SqlFunction CURRENT_DATE = new SqlCurrentDateFunction();

    /**
     * The <code>TIMESTAMPADD</code> function.
     */
    public static final SqlFunction TIMESTAMP_ADD = new SqlTimestampAddFunction();

    /**
     * The <code>TIMESTAMPDIFF</code> function.
     */
    public static final SqlFunction TIMESTAMP_DIFF = new SqlTimestampDiffFunction();

    /**
     * Use of the <code>IN_FENNEL</code> operator forces the argument to be evaluated in Fennel. Otherwise acts as identity function.
     */
    public static final SqlFunction IN_FENNEL =
            new SqlMonotonicUnaryFunction(
                    "IN_FENNEL",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.ARG0,
                    null,
                    OperandTypes.ANY,
                    FunctionCategory.SYSTEM );

    /**
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
    public static final SqlFunction CAST = new SqlCastFunction();

    /**
     * The SQL <code>EXTRACT</code> operator. Extracts a specified field value from a DATETIME or an INTERVAL. E.g.<br>
     * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code>23</code>
     */
    public static final SqlFunction EXTRACT = new SqlExtractFunction();

    /**
     * The SQL <code>YEAR</code> operator. Returns the Year from a DATETIME  E.g.<br>
     * <code>YEAR(date '2008-9-23')</code> returns <code>2008</code>
     */
    public static final SqlDatePartFunction YEAR = new SqlDatePartFunction( "YEAR", TimeUnit.YEAR );

    /**
     * The SQL <code>QUARTER</code> operator. Returns the Quarter from a DATETIME  E.g.<br>
     * <code>QUARTER(date '2008-9-23')</code> returns <code>3</code>
     */
    public static final SqlDatePartFunction QUARTER = new SqlDatePartFunction( "QUARTER", TimeUnit.QUARTER );

    /**
     * The SQL <code>MONTH</code> operator. Returns the Month from a DATETIME  E.g.<br>
     * <code>MONTH(date '2008-9-23')</code> returns <code>9</code>
     */
    public static final SqlDatePartFunction MONTH = new SqlDatePartFunction( "MONTH", TimeUnit.MONTH );

    /**
     * The SQL <code>WEEK</code> operator. Returns the Week from a DATETIME  E.g.<br>
     * <code>WEEK(date '2008-9-23')</code> returns <code>39</code>
     */
    public static final SqlDatePartFunction WEEK = new SqlDatePartFunction( "WEEK", TimeUnit.WEEK );

    /**
     * The SQL <code>DAYOFYEAR</code> operator. Returns the DOY from a DATETIME  E.g.<br>
     * <code>DAYOFYEAR(date '2008-9-23')</code> returns <code>267</code>
     */
    public static final SqlDatePartFunction DAYOFYEAR = new SqlDatePartFunction( "DAYOFYEAR", TimeUnit.DOY );

    /**
     * The SQL <code>DAYOFMONTH</code> operator. Returns the Day from a DATETIME  E.g.<br>
     * <code>DAYOFMONTH(date '2008-9-23')</code> returns <code>23</code>
     */
    public static final SqlDatePartFunction DAYOFMONTH = new SqlDatePartFunction( "DAYOFMONTH", TimeUnit.DAY );

    /**
     * The SQL <code>DAYOFWEEK</code> operator. Returns the DOW from a DATETIME  E.g.<br>
     * <code>DAYOFWEEK(date '2008-9-23')</code> returns <code>2</code>
     */
    public static final SqlDatePartFunction DAYOFWEEK = new SqlDatePartFunction( "DAYOFWEEK", TimeUnit.DOW );

    /**
     * The SQL <code>HOUR</code> operator. Returns the Hour from a DATETIME  E.g.<br>
     * <code>HOUR(timestamp '2008-9-23 01:23:45')</code> returns <code>1</code>
     */
    public static final SqlDatePartFunction HOUR = new SqlDatePartFunction( "HOUR", TimeUnit.HOUR );

    /**
     * The SQL <code>MINUTE</code> operator. Returns the Minute from a DATETIME  E.g.<br>
     * <code>MINUTE(timestamp '2008-9-23 01:23:45')</code> returns <code>23</code>
     */
    public static final SqlDatePartFunction MINUTE = new SqlDatePartFunction( "MINUTE", TimeUnit.MINUTE );

    /**
     * The SQL <code>SECOND</code> operator. Returns the Second from a DATETIME  E.g.<br>
     * <code>SECOND(timestamp '2008-9-23 01:23:45')</code> returns <code>45</code>
     */
    public static final SqlDatePartFunction SECOND = new SqlDatePartFunction( "SECOND", TimeUnit.SECOND );

    /**
     * The ELEMENT operator, used to convert a multiset with only one item to a "regular" type. Example ... log(ELEMENT(MULTISET[1])) ...
     */
    public static final SqlFunction ELEMENT =
            new SqlFunction(
                    "ELEMENT",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.MULTISET_ELEMENT_NULLABLE,
                    null,
                    OperandTypes.COLLECTION,
                    FunctionCategory.SYSTEM );

    /**
     * The item operator {@code [ ... ]}, used to access a given element of an array or map. For example, {@code myArray[3]} or {@code "myMap['foo']"}.
     *
     * The SQL standard calls the ARRAY variant a &lt;array element reference&gt;. Index is 1-based. The standard says to raise "data exception - array element error"
     * but we currently return null.
     *
     * MAP is not standard SQL.
     */
    public static final SqlOperator ITEM = new SqlItemOperator();

    /**
     * The ARRAY Value Constructor. e.g. "<code>ARRAY[1, 2, 3]</code>".
     */
    public static final SqlArrayValueConstructor ARRAY_VALUE_CONSTRUCTOR = new SqlArrayValueConstructor();

    /**
     * The MAP Value Constructor, e.g. "<code>MAP['washington', 1, 'obama', 44]</code>".
     */
    public static final SqlMapValueConstructor MAP_VALUE_CONSTRUCTOR = new SqlMapValueConstructor();

    /**
     * The internal "$SLICE" operator takes a multiset of records and returns a multiset of the first column of those records.
     *
     * It is introduced when multisets of scalar types are created, in order to keep types consistent. For example, <code>MULTISET [5]</code> has type <code>INTEGER MULTISET</code> but is translated to an expression of type
     * <code>RECORD(INTEGER EXPR$0) MULTISET</code> because in our internal representation of multisets, every element must be a record. Applying the "$SLICE" operator to this result converts the type back to an
     * <code>INTEGER MULTISET</code> multiset value.
     *
     * <code>$SLICE</code> is often translated away when the multiset type is converted back to scalar values.
     */
    public static final SqlInternalOperator SLICE =
            new SqlInternalOperator(
                    "$SLICE",
                    Kind.OTHER,
                    0,
                    false,
                    ReturnTypes.MULTISET_PROJECT0,
                    null,
                    OperandTypes.RECORD_COLLECTION ) {
            };

    /**
     * The internal "$ELEMENT_SLICE" operator returns the first field of the only element of a multiset.
     *
     * It is introduced when multisets of scalar types are created, in order to keep types consistent. For example, <code>ELEMENT(MULTISET [5])</code> is translated to <code>$ELEMENT_SLICE(MULTISET (VALUES ROW (5 EXPR$0))</code>
     * It is translated away when the multiset type is converted back to scalar values.
     *
     * NOTE: jhyde, 2006/1/9: Usages of this operator are commented out, but I'm not deleting the operator, because some multiset tests are disabled, and we may need this operator to get them working!
     */
    public static final SqlInternalOperator ELEMENT_SLICE =
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
            };

    /**
     * The internal "$SCALAR_QUERY" operator returns a scalar value from a record type. It assumes the record type only has one field, and returns that field as the output.
     */
    public static final SqlInternalOperator SCALAR_QUERY =
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
                    final SqlWriter.Frame frame = writer.startList( "(", ")" );
                    call.operand( 0 ).unparse( writer, 0, 0 );
                    writer.endList( frame );
                }


                @Override
                public boolean argumentMustBeScalar( int ordinal ) {
                    // Obvious, really.
                    return false;
                }
            };

    /**
     * The internal {@code $STRUCT_ACCESS} operator is used to access a field of a record.
     *
     * In contrast with {@link #DOT} operator, it never appears in an {@link SqlNode} tree and allows to access fields by position and not by name.
     */
    public static final SqlInternalOperator STRUCT_ACCESS = new SqlInternalOperator( "$STRUCT_ACCESS", Kind.OTHER );

    /**
     * The CARDINALITY operator, used to retrieve the number of elements in a MULTISET, ARRAY or MAP.
     */
    public static final SqlFunction CARDINALITY =
            new SqlFunction(
                    "CARDINALITY",
                    Kind.OTHER_FUNCTION,
                    ReturnTypes.INTEGER_NULLABLE,
                    null,
                    OperandTypes.COLLECTION_OR_MAP,
                    FunctionCategory.SYSTEM );

    /**
     * The COLLECT operator. Multiset aggregator function.
     */
    public static final SqlAggFunction COLLECT =
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
            };

    /**
     * The FUSION operator. Multiset aggregator function.
     */
    public static final SqlAggFunction FUSION =
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
            };

    /**
     * The sequence next value function: <code>NEXT VALUE FOR sequence</code>
     */
    public static final SqlOperator NEXT_VALUE = new SqlSequenceValueOperator( Kind.NEXT_VALUE );

    /**
     * The sequence current value function: <code>CURRENT VALUE FOR sequence</code>
     */
    public static final SqlOperator CURRENT_VALUE = new SqlSequenceValueOperator( Kind.CURRENT_VALUE );

    /**
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
    public static final SqlSpecialOperator TABLESAMPLE =
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
                    call.operand( 0 ).unparse( writer, leftPrec, 0 );
                    writer.keyword( "TABLESAMPLE" );
                    call.operand( 1 ).unparse( writer, 0, rightPrec );
                }
            };

    /**
     * The {@code TUMBLE} group function.
     */
    public static final SqlGroupedWindowFunction TUMBLE =
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
                    return ImmutableList.of( TUMBLE_START, TUMBLE_END );
                }
            };

    /**
     * The {@code TUMBLE_START} auxiliary function of the {@code TUMBLE} group function.
     */
    public static final SqlGroupedWindowFunction TUMBLE_START = TUMBLE.auxiliary( Kind.TUMBLE_START );

    /**
     * The {@code TUMBLE_END} auxiliary function of the {@code TUMBLE} group function.
     */
    public static final SqlGroupedWindowFunction TUMBLE_END = TUMBLE.auxiliary( Kind.TUMBLE_END );

    /**
     * The {@code HOP} group function.
     */
    public static final SqlGroupedWindowFunction HOP =
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
                    return ImmutableList.of( HOP_START, HOP_END );
                }
            };

    /**
     * The {@code HOP_START} auxiliary function of the {@code HOP} group function.
     */
    public static final SqlGroupedWindowFunction HOP_START = HOP.auxiliary( Kind.HOP_START );

    /**
     * The {@code HOP_END} auxiliary function of the {@code HOP} group function.
     */
    public static final SqlGroupedWindowFunction HOP_END = HOP.auxiliary( Kind.HOP_END );

    /**
     * The {@code SESSION} group function.
     */
    public static final SqlGroupedWindowFunction SESSION =
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
                    return ImmutableList.of( SESSION_START, SESSION_END );
                }
            };

    /**
     * The {@code SESSION_START} auxiliary function of the {@code SESSION} group function.
     */
    public static final SqlGroupedWindowFunction SESSION_START = SESSION.auxiliary( Kind.SESSION_START );

    /**
     * The {@code SESSION_END} auxiliary function of the {@code SESSION} group function.
     */
    public static final SqlGroupedWindowFunction SESSION_END = SESSION.auxiliary( Kind.SESSION_END );

    /**
     * {@code |} operator to create alternate patterns within {@code MATCH_RECOGNIZE}.
     *
     * If {@code p1} and {@code p2} are patterns then {@code p1 | p2} is a pattern that matches {@code p1} or {@code p2}.
     */
    public static final SqlBinaryOperator PATTERN_ALTER =
            new SqlBinaryOperator(
                    "|",
                    Kind.PATTERN_ALTER,
                    70,
                    true,
                    null,
                    null,
                    null );

    /**
     * Operator to concatenate patterns within {@code MATCH_RECOGNIZE}.
     *
     * If {@code p1} and {@code p2} are patterns then {@code p1 p2} is a pattern that matches {@code p1} followed by {@code p2}.
     */
    public static final SqlBinaryOperator PATTERN_CONCAT =
            new SqlBinaryOperator(
                    "",
                    Kind.PATTERN_CONCAT,
                    80,
                    true,
                    null,
                    null,
                    null );

    /**
     * Operator to quantify patterns within {@code MATCH_RECOGNIZE}.
     *
     * If {@code p} is a pattern then {@code p{3, 5}} is a pattern that matches between 3 and 5 occurrences of {@code p}.
     */
    public static final SqlSpecialOperator PATTERN_QUANTIFIER =
            new SqlSpecialOperator( "PATTERN_QUANTIFIER", Kind.PATTERN_QUANTIFIER, 90 ) {
                @Override
                public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
                    call.operand( 0 ).unparse( writer, this.getLeftPrec(), this.getRightPrec() );
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
            };

    /**
     * {@code PERMUTE} operator to combine patterns within {@code MATCH_RECOGNIZE}.
     *
     * If {@code p1} and {@code p2} are patterns then {@code PERMUTE (p1, p2)} is a pattern that matches all permutations of {@code p1} and {@code p2}.
     */
    public static final SqlSpecialOperator PATTERN_PERMUTE =
            new SqlSpecialOperator( "PATTERN_PERMUTE", Kind.PATTERN_PERMUTE, 100 ) {
                @Override
                public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
                    writer.keyword( "PERMUTE" );
                    SqlWriter.Frame frame = writer.startList( "(", ")" );
                    for ( int i = 0; i < call.getOperandList().size(); i++ ) {
                        SqlNode pattern = call.getOperandList().get( i );
                        pattern.unparse( writer, 0, 0 );
                        if ( i != call.getOperandList().size() - 1 ) {
                            writer.print( "," );
                        }
                    }
                    writer.endList( frame );
                }
            };

    /**
     * {@code EXCLUDE} operator within {@code MATCH_RECOGNIZE}.
     *
     * If {@code p} is a pattern then {@code {- p -} }} is a pattern that excludes {@code p} from the output.
     */
    public static final SqlSpecialOperator PATTERN_EXCLUDE =
            new SqlSpecialOperator( "PATTERN_EXCLUDE", Kind.PATTERN_EXCLUDED, 100 ) {
                @Override
                public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
                    SqlWriter.Frame frame = writer.startList( "{-", "-}" );
                    SqlNode node = call.getOperandList().get( 0 );
                    node.unparse( writer, 0, 0 );
                    writer.endList( frame );
                }
            };


    /**
     * Returns the standard operator table, creating it if necessary.
     */
    public static synchronized SqlStdOperatorTable instance() {
        if ( instance == null ) {
            // Creates and initializes the standard operator table.
            // Uses two-phase construction, because we can't initialize the table until the constructor of the sub-class has completed.
            instance = new SqlStdOperatorTable();
            instance.init();
        }
        return instance;
    }


    /**
     * Returns the group function for which a given kind is an auxiliary function, or null if it is not an auxiliary function.
     */
    public static SqlGroupedWindowFunction auxiliaryToGroup( Kind kind ) {
        switch ( kind ) {
            case TUMBLE_START:
            case TUMBLE_END:
                return TUMBLE;
            case HOP_START:
            case HOP_END:
                return HOP;
            case SESSION_START:
            case SESSION_END:
                return SESSION;
            default:
                return null;
        }
    }


    /**
     * Converts a call to a grouped auxiliary function to a call to the grouped window function. For other calls returns null.
     *
     * For example, converts {@code TUMBLE_START(rowtime, INTERVAL '1' HOUR))} to {@code TUMBLE(rowtime, INTERVAL '1' HOUR))}.
     */
    public static SqlCall convertAuxiliaryToGroupCall( SqlCall call ) {
        final SqlOperator op = (SqlOperator) call.getOperator();
        if ( op instanceof SqlGroupedWindowFunction && op.isGroupAuxiliary() ) {
            return copy( call, ((SqlGroupedWindowFunction) op).groupFunction );
        }
        return null;
    }


    /**
     * Converts a call to a grouped window function to a call to its auxiliary window function(s). For other calls returns null.
     *
     * For example, converts {@code TUMBLE_START(rowtime, INTERVAL '1' HOUR))} to {@code TUMBLE(rowtime, INTERVAL '1' HOUR))}.
     */
    public static List<Pair<SqlNode, AuxiliaryConverter>> convertGroupToAuxiliaryCalls( SqlCall call ) {
        final SqlOperator op = (SqlOperator) call.getOperator();
        if ( op instanceof SqlGroupedWindowFunction && op.isGroup() ) {
            ImmutableList.Builder<Pair<SqlNode, AuxiliaryConverter>> builder = ImmutableList.builder();
            for ( final SqlGroupedWindowFunction f : ((SqlGroupedWindowFunction) op).getAuxiliaryFunctions() ) {
                builder.add( Pair.of( copy( call, f ), new AuxiliaryConverter.Impl( f ) ) );
            }
            return builder.build();
        }
        return ImmutableList.of();
    }


    /**
     * Creates a copy of a call with a new operator.
     */
    private static SqlCall copy( SqlCall call, SqlOperator operator ) {
        final List<Node> list = call.getOperandList();
        return new SqlBasicCall( operator, list.toArray( SqlNode.EMPTY_ARRAY ), call.getPos() );
    }


    /**
     * Returns the operator for {@code SOME comparisonKind}.
     */
    public static SqlQuantifyOperator some( Kind comparisonKind ) {
        switch ( comparisonKind ) {
            case EQUALS:
                return SOME_EQ;
            case NOT_EQUALS:
                return SOME_NE;
            case LESS_THAN:
                return SOME_LT;
            case LESS_THAN_OR_EQUAL:
                return SOME_LE;
            case GREATER_THAN:
                return SOME_GT;
            case GREATER_THAN_OR_EQUAL:
                return SOME_GE;
            default:
                throw new AssertionError( comparisonKind );
        }
    }


    /**
     * Returns the operator for {@code ALL comparisonKind}.
     */
    public static SqlQuantifyOperator all( Kind comparisonKind ) {
        switch ( comparisonKind ) {
            case EQUALS:
                return ALL_EQ;
            case NOT_EQUALS:
                return ALL_NE;
            case LESS_THAN:
                return ALL_LT;
            case LESS_THAN_OR_EQUAL:
                return ALL_LE;
            case GREATER_THAN:
                return ALL_GT;
            case GREATER_THAN_OR_EQUAL:
                return ALL_GE;
            default:
                throw new AssertionError( comparisonKind );
        }
    }

}
