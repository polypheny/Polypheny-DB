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

package org.polypheny.db.algebra.operators;

import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.fun.TrimFunction;
import org.polypheny.db.nodes.BinaryOperator;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Function;
import org.polypheny.db.nodes.LangFunctionOperator;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.util.Conformance;

@Getter
public enum OperatorName {

    ORACLE_TRANSLATE3( Function.class ),

    //-------------------------------------------------------------
    //                   SET OPERATORS
    //-------------------------------------------------------------
    // The set operators can be compared to the arithmetic operators
    // UNION -> +
    // EXCEPT -> -
    // INTERSECT -> *
    // which explains the different precedence values
    UNION( Operator.class ),

    UNION_ALL( Operator.class ),

    EXCEPT( Operator.class ),

    EXCEPT_ALL( Operator.class ),

    INTERSECT( Operator.class ),

    INTERSECT_ALL( Operator.class ),

    /**
     * The {@code MULTISET UNION DISTINCT} operator.
     */
    MULTISET_UNION_DISTINCT( Operator.class ),

    /**
     * The {@code MULTISET UNION [ALL]} operator.
     */
    MULTISET_UNION( Operator.class ),

    /**
     * The {@code MULTISET EXCEPT DISTINCT} operator.
     */
    MULTISET_EXCEPT_DISTINCT( Operator.class ),

    /**
     * The {@code MULTISET EXCEPT [ALL]} operator.
     */
    MULTISET_EXCEPT( Operator.class ),

    /**
     * The {@code MULTISET INTERSECT DISTINCT} operator.
     */
    MULTISET_INTERSECT_DISTINCT( Operator.class ),

    /**
     * The {@code MULTISET INTERSECT [ALL]} operator.
     */
    MULTISET_INTERSECT( Operator.class ),

    //-------------------------------------------------------------
    //                   BINARY OPERATORS
    //-------------------------------------------------------------

    /**
     * Logical <code>AND</code> operator.
     */
    AND( BinaryOperator.class ),

    /**
     * <code>AS</code> operator associates an expression in the SELECT clause with an alias.
     */
    AS( Operator.class ),

    /**
     * <code>ARGUMENT_ASSIGNMENT</code> operator (<code>=&lt;</code>) assigns an argument to a function call to a particular named parameter.
     */
    ARGUMENT_ASSIGNMENT( Operator.class ),

    /**
     * <code>DEFAULT</code> operator indicates that an argument to a function call is to take its default value..
     */
    DEFAULT( Operator.class ),

    /**
     * <code>FILTER</code> operator filters which rows are included in an aggregate function.
     */
    FILTER( Operator.class ),

    /**
     * <code>WITHIN_GROUP</code> operator performs aggregations on ordered data input.
     */
    WITHIN_GROUP( Operator.class ),

    /**
     * {@code CUBE} operator, occurs within {@code GROUP BY} clause or nested within a {@code GROUPING SETS}.
     */
    CUBE( Operator.class ),

    /**
     * {@code ROLLUP} operator, occurs within {@code GROUP BY} clause or nested within a {@code GROUPING SETS}.
     */
    ROLLUP( Operator.class ),

    /**
     * {@code GROUPING SETS} operator, occurs within {@code GROUP BY} clause or nested within a {@code GROUPING SETS}.
     */
    GROUPING_SETS( Operator.class ),

    /**
     * {@code GROUPING(c1 [, c2, ...])} function.
     * <p>
     * Occurs in similar places to an aggregate function ({@code SELECT}, {@code HAVING} clause, etc. of an aggregate query), but not technically an aggregate function.
     */
    GROUPING( Function.class ),

    /**
     * {@code GROUP_ID()} function. (Oracle-specific.)
     */
    GROUP_ID( Function.class ),

    /**
     * {@code GROUPING_ID} function is a synonym for {@code GROUPING}.
     * <p>
     * Some history. The {@code GROUPING} function is in the SQL standard, and originally supported only one argument. {@code GROUPING_ID} is not standard (though supported in Oracle and SQL Server)
     * and supports one or more arguments.
     * <p>
     * The SQL standard has changed to allow {@code GROUPING} to have multiple arguments. It is now equivalent to {@code GROUPING_ID}, so we made {@code GROUPING_ID} a synonym for {@code GROUPING}.
     */
    GROUPING_ID( Function.class ),

    /**
     * {@code EXTEND} operator.
     */
    EXTEND( Operator.class ),

    /**
     * String concatenation operator, '<code>||</code>'.
     */
    CONCAT( BinaryOperator.class ),

    /**
     * Arithmetic division operator, '<code>/</code>'.
     */
    DIVIDE( BinaryOperator.class ),

    /**
     * Arithmetic remainder operator, '<code>%</code>', an alternative to {@link #MOD} allowed if under certain conformance levels.
     *
     * @see Conformance#isPercentRemainderAllowed
     */
    PERCENT_REMAINDER( BinaryOperator.class ),

    /**
     * The {@code RAND_INTEGER([seed, ] bound)} function, which yields a random integer, optionally with seed.
     */
    RAND_INTEGER( Function.class ),

    /**
     * The {@code RAND([seed])} function, which yields a random double, optionally with seed.
     */
    RAND( Function.class ),

    /**
     * Internal integer arithmetic division operator, '<code>/INT</code>'. This is only used to adjust scale for numerics. We distinguish it from user-requested division since some personalities want a floating-point computation,
     * whereas for the internal scaling use of division, we always want integer division.
     */
    DIVIDE_INTEGER( BinaryOperator.class ),

    /**
     * Dot operator, '<code>.</code>', used for referencing fields of records.
     */
    DOT( Operator.class ),

    /**
     * Logical equals operator, '<code>=</code>'.
     */
    EQUALS( BinaryOperator.class ),

    /**
     * Logical greater-than operator, '<code>&gt;</code>'.
     */
    GREATER_THAN( BinaryOperator.class ),

    /**
     * <code>IS DISTINCT FROM</code> operator.
     */
    IS_DISTINCT_FROM( BinaryOperator.class ),

    /**
     * <code>IS NOT DISTINCT FROM</code> operator. Is equivalent to <code>NOT(x IS DISTINCT FROM y)</code>
     */
    IS_NOT_DISTINCT_FROM( BinaryOperator.class ),

    /**
     * The internal <code>$IS_DIFFERENT_FROM</code> operator is the same as the user-level {@link #IS_DISTINCT_FROM} in all respects except that the test for equality on character datatypes treats trailing spaces as significant.
     */
    IS_DIFFERENT_FROM( BinaryOperator.class ),

    /**
     * Logical greater-than-or-equal operator, '<code>&gt;=</code>'.
     */
    GREATER_THAN_OR_EQUAL( BinaryOperator.class ),

    /**
     * <code>IN</code> operator tests for a value's membership in a sub-query or a list of values.
     */
    IN( Operator.class ),

    /**
     * <code>NOT IN</code> operator tests for a value's membership in a sub-query or a list of values.
     */
    NOT_IN( Operator.class ),

    /**
     * The <code>&lt; SOME</code> operator (synonymous with <code>&lt; ANY</code>).
     */
    SOME_LT( Operator.class ),

    SOME_LE( Operator.class ),

    SOME_GT( Operator.class ),

    SOME_GE( Operator.class ),

    SOME_EQ( Operator.class ),

    SOME_NE( Operator.class ),

    /**
     * The <code>&lt; ALL</code> operator.
     */
    ALL_LT( Operator.class ),

    ALL_LE( Operator.class ),

    ALL_GT( Operator.class ),

    ALL_GE( Operator.class ),

    ALL_EQ( Operator.class ),

    ALL_NE( Operator.class ),

    /**
     * Logical less-than operator, '<code>&lt;</code>'.
     */
    LESS_THAN( BinaryOperator.class ),

    /**
     * Logical less-than-or-equal operator, '<code>&lt;=</code>'.
     */
    LESS_THAN_OR_EQUAL( BinaryOperator.class ),

    /**
     * Infix arithmetic minus operator, '<code>-</code>'.
     * <p>
     * Its precedence is less than the prefix {@link #UNARY_PLUS +} and {@link #UNARY_MINUS -} operators.
     */
    MINUS( Operator.class ),

    /**
     * Arithmetic multiplication operator, '<code>*</code>'.
     */
    MULTIPLY( Operator.class ),

    /**
     * Logical not-equals operator, '<code>&lt;&gt;</code>'.
     */
    NOT_EQUALS( BinaryOperator.class ),

    /**
     * Logical <code>OR</code> operator.
     */
    OR( BinaryOperator.class ),

    /**
     * Infix arithmetic plus operator, '<code>+</code>'.
     */
    PLUS( Operator.class ),

    /**
     * Infix datetime plus operator, '<code>DATETIME + INTERVAL</code>'.
     */
    DATETIME_PLUS( Operator.class ),

    /**
     * Multiset {@code MEMBER OF}, which returns whether a element belongs to a multiset.
     * <p>
     * For example, the following returns <code>false</code>:
     *
     * <blockquote>
     * <code>'green' MEMBER OF MULTISET ['red','almost green','blue']</code>
     * </blockquote>
     */
    MEMBER_OF( Operator.class ),

    /**
     * Submultiset. Checks to see if an multiset is a sub-set of another multiset.
     * <p>
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
    SUBMULTISET_OF( BinaryOperator.class ),

    NOT_SUBMULTISET_OF( BinaryOperator.class ),

    //-------------------------------------------------------------
    //                   POSTFIX OPERATORS
    //-------------------------------------------------------------
    DESC( Operator.class ),

    NULLS_FIRST( Operator.class ),

    NULLS_LAST( Operator.class ),

    IS_NOT_NULL( Operator.class ),

    IS_NULL( Operator.class ),

    IS_NOT_TRUE( Operator.class ),

    IS_TRUE( Operator.class ),

    IS_NOT_FALSE( Operator.class ),

    IS_FALSE( Operator.class ),

    IS_NOT_UNKNOWN( Operator.class ),

    IS_UNKNOWN( Operator.class ),

    IS_A_SET( Operator.class ),

    IS_NOT_A_SET( Operator.class ),

    IS_EMPTY( Operator.class ),

    IS_NOT_EMPTY( Operator.class ),

    IS_JSON_VALUE( Operator.class ),

    IS_NOT_JSON_VALUE( Operator.class ),

    IS_JSON_OBJECT( Operator.class ),

    IS_NOT_JSON_OBJECT( Operator.class ),

    IS_JSON_ARRAY( Operator.class ),

    IS_NOT_JSON_ARRAY( Operator.class ),

    IS_JSON_SCALAR( Operator.class ),

    IS_NOT_JSON_SCALAR( Operator.class ),


    //-------------------------------------------------------------
    //                   PREFIX OPERATORS
    //-------------------------------------------------------------
    EXISTS( Operator.class ),

    NOT( Operator.class ),

    /**
     * Prefix arithmetic minus operator, '<code>-</code>'.
     * <p>
     * Its precedence is greater than the infix '{@link #PLUS +}' and '{@link #MINUS -}' operators.
     */
    UNARY_MINUS( Operator.class ),

    /**
     * Prefix arithmetic plus operator, '<code>+</code>'.
     * <p>
     * Its precedence is greater than the infix '{@link #PLUS +}' and '{@link #MINUS -}' operators.
     */
    UNARY_PLUS( Operator.class ),

    /**
     * Keyword which allows an identifier to be explicitly flagged as a table.
     * For example, <code>select * from (TABLE t)</code> or <code>TABLE t</code>. See also {@link #COLLECTION_TABLE}.
     */
    EXPLICIT_TABLE( Operator.class ),

    /**
     * {@code FINAL} function to be used within {@code MATCH_RECOGNIZE}.
     */
    FINAL( Operator.class ),

    /**
     * {@code RUNNING} function to be used within {@code MATCH_RECOGNIZE}.
     */
    RUNNING( Operator.class ),

    //-------------------------------------------------------------
    // AGGREGATE OPERATORS
    //-------------------------------------------------------------
    /**
     * <code>SUM</code> aggregate function.
     */
    SUM( AggFunction.class ),

    /**
     * <code>COUNT</code> aggregate function.
     */
    COUNT( AggFunction.class ),

    /**
     * <code>APPROX_COUNT_DISTINCT</code> aggregate function.
     */
    APPROX_COUNT_DISTINCT( AggFunction.class ),

    /**
     * <code>MIN</code> aggregate function.
     */
    MIN( AggFunction.class ),

    BOOL_OR( AggFunction.class ),
    /**
     * <code>MAX</code> aggregate function.
     */
    MAX( AggFunction.class ),

    /**
     * <code>LAST_VALUE</code> aggregate function.
     */
    LAST_VALUE( AggFunction.class ),

    /**
     * <code>ANY_VALUE</code> aggregate function.
     */
    ANY_VALUE( AggFunction.class ),

    /**
     * <code>FIRST_VALUE</code> aggregate function.
     */
    FIRST_VALUE( AggFunction.class ),

    /**
     * <code>NTH_VALUE</code> aggregate function.
     */
    NTH_VALUE( AggFunction.class ),

    /**
     * <code>LEAD</code> aggregate function.
     */
    LEAD( AggFunction.class ),

    /**
     * <code>LAG</code> aggregate function.
     */
    LAG( AggFunction.class ),

    /**
     * <code>NTILE</code> aggregate function.
     */
    NTILE( AggFunction.class ),

    /**
     * <code>SINGLE_VALUE</code> aggregate function.
     */
    SINGLE_VALUE( AggFunction.class ),

    /**
     * <code>AVG</code> aggregate function.
     */
    AVG( AggFunction.class ),

    /**
     * <code>STDDEV_POP</code> aggregate function.
     */
    STDDEV_POP( AggFunction.class ),

    /**
     * <code>REGR_COUNT</code> aggregate function.
     */
    REGR_COUNT( AggFunction.class ),

    /**
     * <code>REGR_SXX</code> aggregate function.
     */
    REGR_SXX( AggFunction.class ),

    /**
     * <code>REGR_SYY</code> aggregate function.
     */
    REGR_SYY( AggFunction.class ),

    /**
     * <code>COVAR_POP</code> aggregate function.
     */
    COVAR_POP( AggFunction.class ),

    /**
     * <code>COVAR_SAMP</code> aggregate function.
     */
    COVAR_SAMP( AggFunction.class ),

    /**
     * <code>STDDEV_SAMP</code> aggregate function.
     */
    STDDEV_SAMP( AggFunction.class ),

    /**
     * <code>STDDEV</code> aggregate function.
     */
    STDDEV( AggFunction.class ),

    /**
     * <code>VAR_POP</code> aggregate function.
     */
    VAR_POP( AggFunction.class ),

    /**
     * <code>VAR_SAMP</code> aggregate function.
     */
    VAR_SAMP( AggFunction.class ),

    /**
     * <code>VARIANCE</code> aggregate function.
     */
    VARIANCE( AggFunction.class ),

    /**
     * <code>BIT_AND</code> aggregate function.
     */
    BIT_AND( AggFunction.class ),

    /**
     * <code>BIT_OR</code> aggregate function.
     */
    BIT_OR( AggFunction.class ),

    //-------------------------------------------------------------
    // WINDOW Aggregate Functions
    //-------------------------------------------------------------
    /**
     * <code>HISTOGRAM</code> aggregate function support. Used by window aggregate versions of MIN/MAX
     */
    HISTOGRAM_AGG( AggFunction.class ),

    /**
     * <code>HISTOGRAM_MIN</code> window aggregate function.
     */
    HISTOGRAM_MIN( Function.class ),

    /**
     * <code>HISTOGRAM_MAX</code> window aggregate function.
     */
    HISTOGRAM_MAX( Function.class ),

    /**
     * <code>HISTOGRAM_FIRST_VALUE</code> window aggregate function.
     */
    HISTOGRAM_FIRST_VALUE( Function.class ),

    /**
     * <code>HISTOGRAM_LAST_VALUE</code> window aggregate function.
     */
    HISTOGRAM_LAST_VALUE( Function.class ),

    /**
     * <code>SUM0</code> aggregate function.
     */
    SUM0( AggFunction.class ),

    //-------------------------------------------------------------
    // WINDOW Rank Functions
    //-------------------------------------------------------------
    /**
     * <code>CUME_DIST</code> window function.
     */
    CUME_DIST( Function.class ),

    /**
     * <code>DENSE_RANK</code> window function.
     */
    DENSE_RANK( Function.class ),

    /**
     * <code>PERCENT_RANK</code> window function.
     */
    PERCENT_RANK( Function.class ),

    /**
     * <code>RANK</code> window function.
     */
    RANK( Function.class ),

    /**
     * <code>ROW_NUMBER</code> window function.
     */
    ROW_NUMBER( Function.class ),


    //-------------------------------------------------------------
    //                   SPECIAL OPERATORS
    //-------------------------------------------------------------
    ROW( Operator.class ),

    /**
     * A special operator for the subtraction of two DATETIMEs. The format of DATETIME subtraction is:
     *
     * <blockquote><code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")" &lt;interval qualifier&gt;</code></blockquote>
     *
     * This operator is special since it needs to hold the additional interval qualifier specification.
     */
    MINUS_DATE( Operator.class ),

    /**
     * The MULTISET Value Constructor. e.g. "<code>MULTISET[1,2,3]</code>".
     */
    MULTISET_VALUE( Operator.class ),

    /**
     * The MULTISET Query Constructor. e.g. "<code>SELECT dname, MULTISET(SELECT FROM emp WHERE deptno = dept.deptno) FROM dept</code>".
     */
    MULTISET_QUERY( Operator.class ),

    /**
     * The ARRAY Query Constructor. e.g. "<code>SELECT dname, ARRAY(SELECT FROM emp WHERE deptno = dept.deptno) FROM dept</code>".
     */
    ARRAY_QUERY( Operator.class ),

    /**
     * The MAP Query Constructor. e.g. "<code>MAP(SELECT empno, deptno FROM emp)</code>".
     */
    MAP_QUERY( Operator.class ),

    /**
     * The CURSOR constructor. e.g. "<code>SELECT * FROM TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), 'name'))</code>".
     */
    CURSOR( Operator.class ),

    /**
     * The COLUMN_LIST constructor. e.g. the ROW() call in "<code>SELECT * FROM TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), ROW(name, empno)))</code>".
     */
    COLUMN_LIST( Operator.class ),

    /**
     * The <code>UNNEST</code> operator.
     */
    UNNEST( Operator.class ),

    /**
     * The <code>UNNEST WITH ORDINALITY</code> operator.
     */
    UNNEST_WITH_ORDINALITY( Operator.class ),

    /**
     * The <code>LATERAL</code> operator.
     */
    LATERAL( Operator.class ),

    /**
     * The "table function derived table" operator, which a table-valued function into a relation, e.g. "<code>SELECT * FROM TABLE(ramp(5))</code>".
     * <p>
     * This operator has function syntax (with one argument), whereas {@link #EXPLICIT_TABLE} is a prefix operator.
     */
    COLLECTION_TABLE( Operator.class ),

    OVERLAPS( Operator.class ),

    CONTAINS( Operator.class ),

    PRECEDES( Operator.class ),

    IMMEDIATELY_PRECEDES( Operator.class ),

    SUCCEEDS( Operator.class ),

    IMMEDIATELY_SUCCEEDS( Operator.class ),

    PERIOD_EQUALS( Operator.class ),

    VALUES( Operator.class ),

    LITERAL_CHAIN( Operator.class ),

    THROW( Operator.class ),

    JSON_VALUE_EXPRESSION( Operator.class ),

    JSON_VALUE_EXPRESSION_EXCLUDED( Operator.class ),

    JSON_STRUCTURED_VALUE_EXPRESSION( Operator.class ),

    JSON_API_COMMON_SYNTAX( Operator.class ),

    JSON_EXISTS( Function.class ),

    JSON_VALUE( Function.class ),

    JSON_VALUE_ANY( Function.class ),

    JSON_QUERY( Function.class ),

    JSON_OBJECT( Function.class ),

    JSON_OBJECTAGG( AggFunction.class ),

    JSON_ARRAY( Function.class ),

    JSON_ARRAYAGG( AggFunction.class ),

    BETWEEN( Operator.class ),

    SYMMETRIC_BETWEEN( Operator.class ),

    NOT_BETWEEN( Operator.class ),

    SYMMETRIC_NOT_BETWEEN( Operator.class ),

    NOT_LIKE( Operator.class ),

    LIKE( Operator.class ),

    NOT_SIMILAR_TO( Operator.class ),

    SIMILAR_TO( Operator.class ),

    /**
     * Internal operator used to represent the ESCAPE clause of a LIKE or SIMILAR TO expression.
     */
    ESCAPE( Operator.class ),

    CASE( Operator.class ),


    PROCEDURE_CALL( Operator.class ),

    NEW( Operator.class ),

    /**
     * The <code>OVER</code> operator, which applies an aggregate functions to a {#@link SqlWindow window}.
     * <p>
     * Operands are as follows:
     *
     * <ol>
     * <li>name of window function ({@link Call})</li>
     * <li>window name ({@link Literal}) or window in-line specification (@link Operator})</li>
     * </ol>
     */
    OVER( Operator.class ),

    /**
     * An <code>REINTERPRET</code> operator is internal to the planner. When the physical storage of two types is the same, this operator may be used to reinterpret values of one type as the other. This operator is similar to a cast,
     * except that it does not alter the data value. Like a regular cast it accepts one operand and stores the target type as the return type. It performs an overflow check if it has <i>any</i> second operand, whether true or not.
     */
    REINTERPRET( Operator.class ),

    //-------------------------------------------------------------
    //                   FUNCTIONS
    //-------------------------------------------------------------

    /**
     * distance function: <code>DISTANCE(column, ARRAY[], METRIC, WEIGHTS)</code>.
     */
    DISTANCE( Function.class ),

    /**
     * Get metadata of multimedia files
     */
    META( Function.class ),

    /**
     * The character substring function: <code>SUBSTRING(string FROM start [FOR length])</code>.
     * <p>
     * If the length parameter is a constant, the length of the result is the minimum of the length of the input and that length. Otherwise it is the length of the input.
     */
    SUBSTRING( Function.class ),

    /**
     * The {@code REPLACE(string, search, replace)} function. Not standard SQL, but in Oracle and Postgres.
     */
    REPLACE( Function.class ),

    CONVERT( Function.class ),

    /**
     * The <code>TRANSLATE(<i>char_value</i> USING <i>translation_name</i>)</code> function alters the character set of a string value from one base character set to another.
     * <p>
     * It is defined in the SQL standard. See also non-standard {#@link OracleOperatorTable#TRANSLATE3}.
     */
    TRANSLATE( Function.class ),

    OVERLAY( Function.class ),

    /**
     * The "TRIM" function.
     */
    TRIM( TrimFunction.class ),


    POSITION( Function.class ),


    CHAR_LENGTH( Function.class ),


    CHARACTER_LENGTH( Function.class ),


    UPPER( Function.class ),


    LOWER( Function.class ),


    INITCAP( Function.class ),

    /**
     * Uses OperatorTable.useDouble for its return type since we don't know what the result type will be by just looking at the operand types. For example POW(int, int) can return a non integer if the second operand is negative.
     */
    POWER( Function.class ),


    SQRT( Function.class ),

    /**
     * Arithmetic remainder function {@code MOD}.
     *
     * @see #PERCENT_REMAINDER
     */
    MOD( Function.class ),


    LN( Function.class ),


    LOG10( Function.class ),


    ABS( Function.class ),


    ACOS( Function.class ),


    ASIN( Function.class ),


    ATAN( Function.class ),


    ATAN2( Function.class ),


    COS( Function.class ),


    COT( Function.class ),


    DEGREES( Function.class ),


    EXP( Function.class ),


    RADIANS( Function.class ),


    ROUND( Function.class ),


    SIGN( Function.class ),


    SIN( Function.class ),


    TAN( Function.class ),


    TRUNCATE( Function.class ),


    PI( Function.class ),

    /**
     * {@code FIRST} function to be used within {@code MATCH_RECOGNIZE}.
     */
    FIRST( Function.class ),

    /**
     * {@code LAST} function to be used within {@code MATCH_RECOGNIZE}.
     */
    LAST( Function.class ),

    /**
     * {@code PREV} function to be used within {@code MATCH_RECOGNIZE}.
     */
    PREV( Function.class ),

    /**
     * {@code NEXT} function to be used within {@code MATCH_RECOGNIZE}.
     */
    NEXT( Function.class ),

    /**
     * {@code CLASSIFIER} function to be used within {@code MATCH_RECOGNIZE}.
     */
    CLASSIFIER( Function.class ),

    /**
     * {@code MATCH_NUMBER} function to be used within {@code MATCH_RECOGNIZE}.
     */
    MATCH_NUMBER( Function.class ),


    NULLIF( Function.class ),

    /**
     * The COALESCE builtin function.
     */
    COALESCE( Function.class ),

    /**
     * The <code>FLOOR</code> function.
     */
    FLOOR( Function.class ),

    /**
     * The <code>CEIL</code> function.
     */
    CEIL( Function.class ),

    /**
     * The <code>USER</code> function.
     */
    USER( Function.class ),

    /**
     * The <code>CURRENT_USER</code> function.
     */
    CURRENT_USER( Function.class ),

    /**
     * The <code>SESSION_USER</code> function.
     */
    SESSION_USER( Function.class ),

    /**
     * The <code>SYSTEM_USER</code> function.
     */
    SYSTEM_USER( Function.class ),

    /**
     * The <code>CURRENT_PATH</code> function.
     */
    CURRENT_PATH( Function.class ),

    /**
     * The <code>CURRENT_ROLE</code> function.
     */
    CURRENT_ROLE( Function.class ),

    /**
     * The <code>CURRENT_CATALOG</code> function.
     */
    CURRENT_CATALOG( Function.class ),

    /**
     * The <code>CURRENT_SCHEMA</code> function.
     */
    CURRENT_SCHEMA( Function.class ),

    /**
     * The <code>LOCALTIME [(<i>precision</i>)]</code> function.
     */
    LOCALTIME( Function.class ),

    /**
     * The <code>LOCALTIMESTAMP [(<i>precision</i>)]</code> function.
     */
    LOCALTIMESTAMP( Function.class ),

    /**
     * The <code>CURRENT_TIME [(<i>precision</i>)]</code> function.
     */
    CURRENT_TIME( Function.class ),

    /**
     * The <code>CURRENT_TIMESTAMP [(<i>precision</i>)]</code> function.
     */
    CURRENT_TIMESTAMP( Function.class ),

    /**
     * The <code>CURRENT_DATE</code> function.
     */
    CURRENT_DATE( Function.class ),

    /**
     * The <code>TIMESTAMPADD</code> function.
     */
    TIMESTAMP_ADD( Function.class ),

    /**
     * The <code>TIMESTAMPDIFF</code> function.
     */
    TIMESTAMP_DIFF( Function.class ),

    /**
     * Use of the <code>IN_FENNEL</code> operator forces the argument to be evaluated in Fennel. Otherwise acts as identity function.
     */
    IN_FENNEL( Function.class ),

    /**
     * The SQL <code>CAST</code> operator.
     * <p>
     * The SQL syntax is
     *
     * <blockquote><code>CAST(<i>expression</i> AS <i>type</i>)</code></blockquote>
     *
     * When the CAST operator is applies as a {@link Call}, it has two arguments: the expression and the type. The type must not include a constraint, so <code>CAST(x AS INTEGER NOT NULL)</code>,
     * for instance, is invalid.
     * <p>
     * When the CAST operator is applied as a <code>RexCall</code>, the target type is simply stored as the return type, not an explicit operand. For example, the expression <code>CAST(1 + 2 AS DOUBLE)</code> will
     * become a call to <code>CAST</code> with the expression <code>1 + 2</code> as its only operand.
     * <p>
     * The <code>RexCall</code> form can also have a type which contains a <code>NOT NULL</code> constraint. When this expression is implemented, if the value is NULL, an exception will be thrown.
     */
    CAST( Function.class ),

    /**
     * The SQL <code>EXTRACT</code> operator. Extracts a specified field value from a DATETIME or an INTERVAL. E.g.<br>
     * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code>23</code>
     */
    EXTRACT( Function.class ),

    /**
     * The SQL <code>YEAR</code> operator. Returns the Year from a DATETIME  E.g.<br>
     * <code>YEAR(date '2008-9-23')</code> returns <code>2008</code>
     */
    YEAR( Function.class ),

    /**
     * The SQL <code>QUARTER</code> operator. Returns the Quarter from a DATETIME  E.g.<br>
     * <code>QUARTER(date '2008-9-23')</code> returns <code>3</code>
     */
    QUARTER( Function.class ),

    /**
     * The SQL <code>MONTH</code> operator. Returns the Month from a DATETIME  E.g.<br>
     * <code>MONTH(date '2008-9-23')</code> returns <code>9</code>
     */
    MONTH( Function.class ),

    /**
     * The SQL <code>WEEK</code> operator. Returns the Week from a DATETIME  E.g.<br>
     * <code>WEEK(date '2008-9-23')</code> returns <code>39</code>
     */
    WEEK( Function.class ),

    /**
     * The SQL <code>DAYOFYEAR</code> operator. Returns the DOY from a DATETIME  E.g.<br>
     * <code>DAYOFYEAR(date '2008-9-23')</code> returns <code>267</code>
     */
    DAYOFYEAR( Function.class ),

    /**
     * The SQL <code>DAYOFMONTH</code> operator. Returns the Day from a DATETIME  E.g.<br>
     * <code>DAYOFMONTH(date '2008-9-23')</code> returns <code>23</code>
     */
    DAYOFMONTH( Function.class ),

    /**
     * The SQL <code>DAYOFWEEK</code> operator. Returns the DOW from a DATETIME  E.g.<br>
     * <code>DAYOFWEEK(date '2008-9-23')</code> returns <code>2</code>
     */
    DAYOFWEEK( Function.class ),

    /**
     * The SQL <code>HOUR</code> operator. Returns the Hour from a DATETIME  E.g.<br>
     * <code>HOUR(timestamp '2008-9-23 01:23:45')</code> returns <code>1</code>
     */
    HOUR( Function.class ),

    /**
     * The SQL <code>MINUTE</code> operator. Returns the Minute from a DATETIME  E.g.<br>
     * <code>MINUTE(timestamp '2008-9-23 01:23:45')</code> returns <code>23</code>
     */
    MINUTE( Function.class ),

    /**
     * The SQL <code>SECOND</code> operator. Returns the Second from a DATETIME  E.g.<br>
     * <code>SECOND(timestamp '2008-9-23 01:23:45')</code> returns <code>45</code>
     */
    SECOND( Function.class ),

    /**
     * The ELEMENT operator, used to convert a multiset with only one item to a "regular" type. Example ... log(ELEMENT(MULTISET[1])) ...
     */
    ELEMENT( Function.class ),

    /**
     * The item operator {@code [ ... ]}, used to access a given element of an array or map. For example, {@code myArray[3]} or {@code "myMap['foo']"}.
     * <p>
     * The SQL standard calls the ARRAY variant a &lt;array element reference&gt;. Index is 1-based. The standard says to raise "data exception - array element error"
     * but we currently return null.
     * <p>
     * MAP is not standard SQL.
     */
    ITEM( Operator.class ),

    /**
     * The ARRAY Value Constructor. e.g. "<code>ARRAY[1, 2, 3]</code>".
     */
    ARRAY_VALUE_CONSTRUCTOR( Operator.class ),

    /**
     * The MAP Value Constructor, e.g. "<code>MAP['washington', 1, 'obama', 44]</code>".
     */
    MAP_VALUE_CONSTRUCTOR( Operator.class ),

    /**
     * The internal "$SLICE" operator takes a multiset of records and returns a multiset of the first column of those records.
     * <p>
     * It is introduced when multisets of scalar types are created, in order to keep types consistent. For example, <code>MULTISET [5]</code> has type <code>INTEGER MULTISET</code> but is translated to an expression of type
     * <code>RECORD(INTEGER EXPR$0) MULTISET</code> because in our internal representation of multisets, every element must be a record. Applying the "$SLICE" operator to this result converts the type back to an
     * <code>INTEGER MULTISET</code> multiset value.
     * <p>
     * <code>$SLICE</code> is often translated away when the multiset type is converted back to scalar values.
     */
    SLICE( Operator.class ),

    /**
     * The internal "$ELEMENT_SLICE" operator returns the first field of the only element of a multiset.
     * <p>
     * It is introduced when multisets of scalar types are created, in order to keep types consistent. For example, <code>ELEMENT(MULTISET [5])</code> is translated to <code>$ELEMENT_SLICE(MULTISET (VALUES ROW (5 EXPR$0))</code>
     * It is translated away when the multiset type is converted back to scalar values.
     * <p>
     * NOTE: jhyde, 2006/1/9: Usages of this operator are commented out, but I'm not deleting the operator, because some multiset tests are disabled, and we may need this operator to get them working!
     */
    ELEMENT_SLICE( Operator.class ),

    /**
     * The internal "$SCALAR_QUERY" operator returns a scalar value from a record type. It assumes the record type only has one field, and returns that field as the output.
     */
    SCALAR_QUERY( Operator.class ),

    /**
     * The internal {@code $STRUCT_ACCESS} operator is used to access a field of a record.
     * <p>
     * In contrast with {@link #DOT} operator, it never appears in an {@link Node} tree and allows to access fields by position and not by name.
     */
    STRUCT_ACCESS( Operator.class ),

    /**
     * The CARDINALITY operator, used to retrieve the number of elements in a MULTISET, ARRAY or MAP.
     */
    CARDINALITY( Function.class ),

    /**
     * The COLLECT operator. Multiset aggregator function.
     */
    COLLECT( AggFunction.class ),

    /**
     * The FUSION operator. Multiset aggregator function.
     */
    FUSION( AggFunction.class ),

    /**
     * The sequence next value function: <code>NEXT VALUE FOR sequence</code>
     */
    NEXT_VALUE( Operator.class ),

    /**
     * The sequence current value function: <code>CURRENT VALUE FOR sequence</code>
     */
    CURRENT_VALUE( Operator.class ),

    /**
     * The <code>TABLESAMPLE</code> operator.
     * <p>
     * Examples:
     *
     * <ul>
     * <li><code>&lt;query&gt; TABLESAMPLE SUBSTITUTE('sampleName')</code> (non-standard)</li>
     * <li><code>&lt;query&gt; TABLESAMPLE BERNOULLI(&lt;percent&gt;) [REPEATABLE(&lt;seed&gt;)]</code> (standard, but not implemented for FTRS yet)</li>
     * <li><code>&lt;query&gt; TABLESAMPLE SYSTEM(&lt;percent&gt;) [REPEATABLE(&lt;seed&gt;)]</code> (standard, but not implemented for FTRS yet)</li>
     * </ul>
     *
     * Operand #0 is a query or table; Operand #1 is a {#@link SqlSampleSpec} wrapped in a {#@link SqlLiteral}.
     */
    TABLESAMPLE( Operator.class ),

    /**
     * The {@code TUMBLE} group function.
     */
    TUMBLE( Function.class ),

    /**
     * The {@code TUMBLE_START} auxiliary function of the {@code TUMBLE} group function.
     */
    TUMBLE_START( Function.class ),

    /**
     * The {@code TUMBLE_END} auxiliary function of the {@code TUMBLE} group function.
     */
    TUMBLE_END( Function.class ),

    /**
     * The {@code HOP} group function.
     */
    HOP( Function.class ),

    /**
     * The {@code HOP_START} auxiliary function of the {@code HOP} group function.
     */
    HOP_START( Function.class ),

    /**
     * The {@code HOP_END} auxiliary function of the {@code HOP} group function.
     */
    HOP_END( Function.class ),

    /**
     * The {@code SESSION} group function.
     */
    SESSION( Function.class ),

    /**
     * The {@code SESSION_START} auxiliary function of the {@code SESSION} group function.
     */
    SESSION_START( Function.class ),

    /**
     * The {@code SESSION_END} auxiliary function of the {@code SESSION} group function.
     */
    SESSION_END( Function.class ),

    /**
     * {@code |} operator to create alternate patterns within {@code MATCH_RECOGNIZE}.
     * <p>
     * If {@code p1} and {@code p2} are patterns then {@code p1 | p2} is a pattern that matches {@code p1} or {@code p2}.
     */
    PATTERN_ALTER( BinaryOperator.class ),

    /**
     * Operator to concatenate patterns within {@code MATCH_RECOGNIZE}.
     * <p>
     * If {@code p1} and {@code p2} are patterns then {@code p1 p2} is a pattern that matches {@code p1} followed by {@code p2}.
     */
    PATTERN_CONCAT( BinaryOperator.class ),

    /**
     * Operator to quantify patterns within {@code MATCH_RECOGNIZE}.
     * <p>
     * If {@code p} is a pattern then {@code p{3, 5}} is a pattern that matches between 3 and 5 occurrences of {@code p}.
     */
    PATTERN_QUANTIFIER( Operator.class ),

    /**
     * {@code PERMUTE} operator to combine patterns within {@code MATCH_RECOGNIZE}.
     * <p>
     * If {@code p1} and {@code p2} are patterns then {@code PERMUTE (p1, p2)} is a pattern that matches all permutations of {@code p1} and {@code p2}.
     */
    PATTERN_PERMUTE( Operator.class ),

    /**
     * {@code EXCLUDE} operator within {@code MATCH_RECOGNIZE}.
     * <p>
     * If {@code p} is a pattern then {@code {- p -} }} is a pattern that excludes {@code p} from the output.
     */
    PATTERN_EXCLUDE( Operator.class ),

    //-------------------------------------------------------------
    //                   SET OPERATORS
    //-------------------------------------------------------------

    MQL_EQUALS( LangFunctionOperator.class ),

    MQL_SIZE_MATCH( LangFunctionOperator.class ),

    MQL_JSON_MATCH( LangFunctionOperator.class ),

    MQL_REGEX_MATCH( LangFunctionOperator.class ),

    MQL_TYPE_MATCH( LangFunctionOperator.class ),

    MQL_QUERY_VALUE( LangFunctionOperator.class ),

    MQL_SLICE( LangFunctionOperator.class ),

    MQL_ITEM( LangFunctionOperator.class ),

    MQL_ADD_FIELDS( LangFunctionOperator.class ),

    MQL_UPDATE_MIN( LangFunctionOperator.class ),

    MQL_UPDATE_MAX( LangFunctionOperator.class ),

    MQL_UPDATE_ADD_TO_SET( LangFunctionOperator.class ),

    MQL_UPDATE_RENAME( LangFunctionOperator.class ),

    MQL_UPDATE_REPLACE( LangFunctionOperator.class ),

    MQL_REMOVE( LangFunctionOperator.class ),

    MQL_UPDATE( LangFunctionOperator.class ),

    MQL_ELEM_MATCH( LangFunctionOperator.class ),

    MQL_UNWIND( LangFunctionOperator.class ),

    MQL_EXISTS( LangFunctionOperator.class ),

    MQL_LT( LangFunctionOperator.class ),

    MQL_GT( LangFunctionOperator.class ),

    MQL_LTE( LangFunctionOperator.class ),

    MQL_GTE( LangFunctionOperator.class ),

    MQL_MERGE( LangFunctionOperator.class ),

    MQL_PROJECT_INCLUDES( LangFunctionOperator.class ),

    MQL_REPLACE_ROOT( LangFunctionOperator.class ),

    MQL_NOT_UNSET( LangFunctionOperator.class ),

    //-------------------------------------------------------------
    //                   OPENCYPHER OPERATORS
    //-------------------------------------------------------------

    REG_EQUALS( LangFunctionOperator.class ),

    STARTS_WITH( LangFunctionOperator.class ),

    ENDS_WITH( LangFunctionOperator.class ),

    DESERIALIZE( LangFunctionOperator.class ),

    CYPHER_PATH_MATCH( LangFunctionOperator.class ),

    CYPHER_ALL_MATCH( LangFunctionOperator.class ),

    CYPHER_ANY_MATCH( LangFunctionOperator.class ),

    CYPHER_NONE_MATCH( LangFunctionOperator.class ),

    CYPHER_SINGLE_MATCH( LangFunctionOperator.class ),

    DESERIALIZE_DIRECTORY( LangFunctionOperator.class ),

    CYPHER_HAS_PROPERTY( LangFunctionOperator.class ),

    DESERIALIZE_LIST( LangFunctionOperator.class ),

    CYPHER_HAS_LABEL( LangFunctionOperator.class ),

    CYPHER_NODE_MATCH( LangFunctionOperator.class ),

    CYPHER_NODE_EXTRACT( LangFunctionOperator.class ),

    CYPHER_EXTRACT_FROM_PATH( LangFunctionOperator.class ),

    CYPHER_EXTRACT_PROPERTY( LangFunctionOperator.class ),

    CYPHER_EXTRACT_ID( LangFunctionOperator.class ),

    CYPHER_EXTRACT_PROPERTIES( LangFunctionOperator.class ),

    CYPHER_EXTRACT_LABELS( LangFunctionOperator.class ),

    CYPHER_EXTRACT_LABEL( LangFunctionOperator.class ),

    CYPHER_TO_LIST( LangFunctionOperator.class ),

    CYPHER_ADJUST_EDGE( LangFunctionOperator.class ),

    CYPHER_SET_PROPERTY( LangFunctionOperator.class ),

    CYPHER_SET_PROPERTIES( LangFunctionOperator.class ),

    CYPHER_SET_LABELS( LangFunctionOperator.class ),

    CYPHER_REMOVE_LABELS( LangFunctionOperator.class ),

    CYPHER_REMOVE_PROPERTY( LangFunctionOperator.class ),

    CYPHER_REMOVE_PROPERTIES( LangFunctionOperator.class ),

    CYPHER_LIKE( LangFunctionOperator.class ),

    CYPHER_GRAPH_ONLY_LABEL( LangFunctionOperator.class ),

    // CROSS MODEL FUNCTION

    CROSS_MODEL_ITEM( LangFunctionOperator.class ),

    TO_JSON( LangFunctionOperator.class ),

    OF_LIST( LangFunctionOperator.class ),

    REMOVE_NAMES( LangFunctionOperator.class ),

    EXTRACT_NAME( LangFunctionOperator.class );


    @Getter
    private final Class<? extends Operator> clazz;


    OperatorName( Class<? extends Operator> clazz ) {
        this.clazz = clazz;
    }


    final public static List<OperatorName> MQL_OPERATORS = Arrays.asList(
            MQL_EQUALS,
            MQL_GT,
            MQL_GTE,
            MQL_LT,
            MQL_LTE
    );
}