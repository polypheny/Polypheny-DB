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

package org.polypheny.db.algebra.constant;


import com.esri.core.geometry.Operator;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Node;


/**
 * Enumerates the possible types of {@link Node}.
 *
 * The values are immutable, canonical constants, so you can use Kinds to find particular types of expressions quickly.
 * To identity a call to a common operator such as '=', use {@link Node#isA}:
 *
 * <blockquote>
 * exp.{@link Node#isA isA}({@link #EQUALS})
 * </blockquote>
 *
 * Only commonly-used nodes have their own type; other nodes are of type {@link #OTHER}. Some of the values, such as
 * {@link #SET_QUERY}, represent aggregates.
 *
 * To quickly choose between a number of options, use a switch statement:
 *
 * <blockquote>
 * <pre>switch (exp.getKind()) {
 * case {@link #EQUALS}:
 *     ...;
 * case {@link #NOT_EQUALS}:
 *     ...;
 * default:
 *     throw new AssertionError("unexpected");
 * }</pre>
 * </blockquote>
 *
 * Note that we do not even have to check that a {@code SqlNode} is a {@link Call}.
 *
 * To identify a category of expressions, use {@code SqlNode.isA} with an aggregate Kind. The following expression will
 * return <code>true</code> for calls to '=' and '&gt;=', but <code>false</code> for the constant '5', or a call to '+':
 *
 * <blockquote>
 * <pre>exp.isA({@link #COMPARISON Kind.COMPARISON})</pre>
 * </blockquote>
 *
 * RexNode also has a {@code getKind} method; {@code Kind} values are preserved during translation from {@code SqlNode}
 * to {@code RexNode}, where applicable.
 *
 * There is no water-tight definition of "common", but that's OK. There will always be operators that don't have their own
 * kind, and for these we use the {@code SqlOperator}. But for really the common ones, e.g. the many places where we are
 * looking for {@code AND}, {@code OR} and {@code EQUALS}, the enum helps.
 *
 * (If we were using Scala, {@link Operator} would be a case class, and we wouldn't need {@code Kind}. But we're not.)
 */
public enum Kind {

    // the basics

    /**
     * Expression not covered by any other {@link Kind} value.
     *
     * @see #OTHER_FUNCTION
     */
    OTHER,

    /**
     * SELECT statement or sub-query.
     */
    SELECT,

    /**
     * JOIN operator or compound FROM clause.
     *
     * A FROM clause with more than one table is represented as if it were a join. For example, "FROM x, y, z" is represented
     * as "JOIN(x, JOIN(x, y))".
     */
    JOIN,

    /**
     * Identifier
     */
    IDENTIFIER,

    /**
     * A literal.
     */
    LITERAL,

    /**
     * Function that is not a special function.
     *
     * @see #FUNCTION
     */
    OTHER_FUNCTION,

    /**
     * distance functions.
     */
    DISTANCE,

    /**
     * POSITION Function
     */
    POSITION,

    /**
     * EXPLAIN statement
     */
    EXPLAIN,

    /**
     * DESCRIBE SCHEMA statement
     */
    DESCRIBE_NAMESPACE,

    /**
     * DESCRIBE TABLE statement
     */
    DESCRIBE_TABLE,

    /**
     * INSERT statement
     */
    INSERT,

    /**
     * DELETE statement
     */
    DELETE,

    /**
     * UPDATE statement
     */
    UPDATE,

    /**
     * "ALTER scope SET option = value" statement.
     */
    SET_OPTION,

    /**
     * A dynamic parameter.
     */
    DYNAMIC_PARAM,

    /**
     * ORDER BY clause.
     *
     * @see #DESCENDING
     * @see #NULLS_FIRST
     * @see #NULLS_LAST
     */
    ORDER_BY,

    /**
     * WITH clause.
     */
    WITH,

    /**
     * Item in WITH clause.
     */
    WITH_ITEM,

    /**
     * Union
     */
    UNION,

    /**
     * Except
     */
    EXCEPT,

    /**
     * Intersect
     */
    INTERSECT,

    /**
     * AS operator
     */
    AS,

    /**
     * ARGUMENT_ASSIGNMENT operator, {@code =>}
     */
    ARGUMENT_ASSIGNMENT,

    /**
     * DEFAULT operator
     */
    DEFAULT,

    /**
     * OVER operator
     */
    OVER,

    /**
     * FILTER operator
     */
    FILTER,

    /**
     * WITHIN_GROUP operator
     */
    WITHIN_GROUP,

    /**
     * Window specification
     */
    WINDOW,

    /**
     * MERGE statement
     */
    MERGE,

    /**
     * TABLESAMPLE operator
     */
    TABLESAMPLE,

    /**
     * MATCH_RECOGNIZE clause
     */
    MATCH_RECOGNIZE,
    // binary operators

    /**
     * The arithmetic multiplication operator, "*".
     */
    TIMES,

    /**
     * The arithmetic division operator, "/".
     */
    DIVIDE,

    /**
     * The arithmetic remainder operator, "MOD" (and "%" in some dialects).
     */
    MOD,

    /**
     * The arithmetic plus operator, "+".
     *
     * @see #PLUS_PREFIX
     */
    PLUS,

    /**
     * The arithmetic minus operator, "-".
     *
     * @see #MINUS_PREFIX
     */
    MINUS,

    /**
     * the alternation operator in a pattern expression within a match_recognize clause
     */
    PATTERN_ALTER,

    /**
     * the concatenation operator in a pattern expression within a match_recognize clause
     */
    PATTERN_CONCAT,

    // comparison operators

    /**
     * The "IN" operator.
     */
    IN,

    /**
     * The "NOT IN" operator.
     *
     * Only occurs in SqlNode trees. Is expanded to NOT(IN ...) before entering {@link AlgNode} land.
     */
    NOT_IN( "NOT IN" ),

    /**
     * The less-than operator, "&lt;".
     */
    LESS_THAN( "<" ),

    /**
     * The greater-than operator, "&gt;".
     */
    GREATER_THAN( ">" ),

    /**
     * The less-than-or-equal operator, "&lt;=".
     */
    LESS_THAN_OR_EQUAL( "<=" ),

    /**
     * The greater-than-or-equal operator, "&gt;=".
     */
    GREATER_THAN_OR_EQUAL( ">=" ),

    /**
     * The equals operator, "=".
     */
    EQUALS( "=" ),

    /**
     * The not-equals operator, "&#33;=" or "&lt;&gt;".
     * The latter is standard, and preferred.
     */
    NOT_EQUALS( "<>" ),

    /**
     * The is-distinct-from operator.
     */
    IS_DISTINCT_FROM,

    /**
     * The is-not-distinct-from operator.
     */
    IS_NOT_DISTINCT_FROM,

    /**
     * The logical "OR" operator.
     */
    OR,

    /**
     * The logical "AND" operator.
     */
    AND,

    // other infix

    /**
     * Dot
     */
    DOT,

    /**
     * The "OVERLAPS" operator for periods.
     */
    OVERLAPS,

    /**
     * The "CONTAINS" operator for periods.
     */
    CONTAINS,

    /**
     * The "PRECEDES" operator for periods.
     */
    PRECEDES,

    /**
     * The "IMMEDIATELY PRECEDES" operator for periods.
     */
    IMMEDIATELY_PRECEDES( "IMMEDIATELY PRECEDES" ),

    /**
     * The "SUCCEEDS" operator for periods.
     */
    SUCCEEDS,

    /**
     * The "IMMEDIATELY SUCCEEDS" operator for periods.
     */
    IMMEDIATELY_SUCCEEDS( "IMMEDIATELY SUCCEEDS" ),

    /**
     * The "EQUALS" operator for periods.
     */
    PERIOD_EQUALS( "EQUALS" ),

    /**
     * The "LIKE" operator.
     */
    LIKE,

    /**
     * The "SIMILAR" operator.
     */
    SIMILAR,

    /**
     * The "BETWEEN" operator.
     */
    BETWEEN,

    /**
     * A "CASE" expression.
     */
    CASE,

    /**
     * The "NULLIF" operator.
     */
    NULLIF,

    /**
     * The "COALESCE" operator.
     */
    COALESCE,

    /**
     * The "DECODE" function (Oracle).
     */
    DECODE,

    /**
     * The "NVL" function (Oracle).
     */
    NVL,

    /**
     * The "GREATEST" function (Oracle).
     */
    GREATEST,

    /**
     * The "LEAST" function (Oracle).
     */
    LEAST,

    /**
     * The "TIMESTAMP_ADD" function (ODBC, SQL Server, MySQL).
     */
    TIMESTAMP_ADD,

    /**
     * The "TIMESTAMP_DIFF" function (ODBC, SQL Server, MySQL).
     */
    TIMESTAMP_DIFF,

    // prefix operators

    /**
     * The logical "NOT" operator.
     */
    NOT,

    /**
     * The unary plus operator, as in "+1".
     *
     * @see #PLUS
     */
    PLUS_PREFIX,

    /**
     * The unary minus operator, as in "-1".
     *
     * @see #MINUS
     */
    MINUS_PREFIX,

    /**
     * The "EXISTS" operator.
     */
    EXISTS,

    /**
     * The "SOME" quantification operator (also called "ANY").
     */
    SOME,

    /**
     * The "ALL" quantification operator.
     */
    ALL,

    /**
     * The "VALUES" operator.
     */
    VALUES,

    /**
     * Explicit table, e.g. <code>select * from (TABLE t)</code> or <code>TABLE t</code>. See also {@link #COLLECTION_TABLE}.
     */
    EXPLICIT_TABLE,

    /**
     * Scalar query; that is, a sub-query used in an expression context, and returning one row and one column.
     */
    SCALAR_QUERY,

    /**
     * ProcedureCall
     */
    PROCEDURE_CALL,

    /**
     * NewSpecification
     */
    NEW_SPECIFICATION,

    /**
     * Special functions in MATCH_RECOGNIZE.
     */
    FINAL,

    RUNNING,

    PREV,

    NEXT,

    FIRST,

    LAST,

    CLASSIFIER,

    MATCH_NUMBER,

    /**
     * The "SKIP TO FIRST" qualifier of restarting point in a MATCH_RECOGNIZE clause.
     */
    SKIP_TO_FIRST,

    /**
     * The "SKIP TO LAST" qualifier of restarting point in a MATCH_RECOGNIZE clause.
     */
    SKIP_TO_LAST,

    // postfix operators

    /**
     * DESC in ORDER BY. A parse tree, not a true expression.
     */
    DESCENDING,

    /**
     * NULLS FIRST clause in ORDER BY. A parse tree, not a true expression.
     */
    NULLS_FIRST,

    /**
     * NULLS LAST clause in ORDER BY. A parse tree, not a true expression.
     */
    NULLS_LAST,

    /**
     * The "IS TRUE" operator.
     */
    IS_TRUE,

    /**
     * The "IS FALSE" operator.
     */
    IS_FALSE,

    /**
     * The "IS NOT TRUE" operator.
     */
    IS_NOT_TRUE,

    /**
     * The "IS NOT FALSE" operator.
     */
    IS_NOT_FALSE,

    /**
     * The "IS UNKNOWN" operator.
     */
    IS_UNKNOWN,

    /**
     * The "IS NULL" operator.
     */
    IS_NULL,

    /**
     * The "IS NOT NULL" operator.
     */
    IS_NOT_NULL,

    /**
     * The "PRECEDING" qualifier of an interval end-point in a window specification.
     */
    PRECEDING,

    /**
     * The "FOLLOWING" qualifier of an interval end-point in a window specification.
     */
    FOLLOWING,

    /**
     * The field access operator, ".".
     *
     * (Only used at the RexNode level; at SqlNode level, a field-access is part of an identifier.)
     */
    FIELD_ACCESS,

    /**
     * Reference to an input field.
     *
     * (Only used at the RexNode level.)
     */
    INPUT_REF,

    /**
     * Reference to an input field, with a qualified name (and perhaps an identifier)
     *
     * (Only used at the RexNode level.)
     */
    NAME_INDEX_REF,
    /**
     * Reference to an input field, with a qualified name and an identifier
     *
     * (Only used at the RexNode level.)
     */
    TABLE_INPUT_REF,

    /**
     * Reference to an input field, with pattern var as modifier
     *
     * (Only used at the RexNode level.)
     */
    PATTERN_INPUT_REF,
    /**
     * Reference to a sub-expression computed within the current relational operator.
     *
     * (Only used at the RexNode level.)
     */
    LOCAL_REF,

    /**
     * Reference to correlation variable.
     *
     * (Only used at the RexNode level.)
     */
    CORREL_VARIABLE,

    /**
     * the repetition quantifier of a pattern factor in a match_recognize clause.
     */
    PATTERN_QUANTIFIER,

    // functions

    /**
     * The row-constructor function. May be explicit or implicit: {@code VALUES 1, ROW (2)}.
     */
    ROW,

    /**
     * The non-standard constructor used to pass a COLUMN_LIST parameter to a user-defined transform.
     */
    COLUMN_LIST,

    /**
     * The "CAST" operator.
     */
    CAST,

    /**
     * The "NEXT VALUE OF sequence" operator.
     */
    NEXT_VALUE,

    /**
     * The "CURRENT VALUE OF sequence" operator.
     */
    CURRENT_VALUE,

    /**
     * The "FLOOR" function
     */
    FLOOR,

    /**
     * The "CEIL" function
     */
    CEIL,

    /**
     * The "TRIM" function.
     */
    TRIM,

    /**
     * The "LTRIM" function (Oracle).
     */
    LTRIM,

    /**
     * The "RTRIM" function (Oracle).
     */
    RTRIM,

    /**
     * The "EXTRACT" function.
     */
    EXTRACT,

    /**
     * Call to a function using JDBC function syntax.
     */
    JDBC_FN,

    /**
     * The MULTISET value constructor.
     */
    MULTISET_VALUE_CONSTRUCTOR,

    /**
     * The MULTISET query constructor.
     */
    MULTISET_QUERY_CONSTRUCTOR,

    /**
     * The JSON value expression.
     */
    JSON_VALUE_EXPRESSION,

    /**
     * The JSON API common syntax.
     */
    JSON_API_COMMON_SYNTAX,

    /**
     * The {@code JSON_ARRAYAGG} aggregate function.
     */
    JSON_ARRAYAGG,

    /**
     * The {@code JSON_OBJECTAGG} aggregate function.
     */
    JSON_OBJECTAGG,

    /**
     * The "UNNEST" operator.
     */
    UNNEST,

    /**
     * The "LATERAL" qualifier to relations in the FROM clause.
     */
    LATERAL,

    /**
     * Table operator which converts user-defined transform into a relation, for example,
     * <code>select * from TABLE(udx(x, y, z))</code>. See also the {@link #EXPLICIT_TABLE} prefix operator.
     */
    COLLECTION_TABLE,

    /**
     * Array Value Constructor, e.g. {@code Array[1, 2, 3]}.
     */
    ARRAY_VALUE_CONSTRUCTOR,

    /**
     * Array Query Constructor, e.g. {@code Array(select deptno from dept)}.
     */
    ARRAY_QUERY_CONSTRUCTOR,

    /**
     * Map Value Constructor, e.g. {@code Map['washington', 1, 'obama', 44]}.
     */
    MAP_VALUE_CONSTRUCTOR,

    /**
     * Map Query Constructor, e.g. {@code MAP (SELECT empno, deptno FROM emp)}.
     */
    MAP_QUERY_CONSTRUCTOR,

    /**
     * CURSOR constructor, for example, <code>select * from TABLE(udx(CURSOR(select ...), x, y, z))</code>
     */
    CURSOR,

    // internal operators (evaluated in validator) 200-299

    /**
     * Literal chain operator (for composite string literals).
     * An internal operator that does not appear in SQL syntax.
     */
    LITERAL_CHAIN,

    /**
     * Escape operator (always part of LIKE or SIMILAR TO expression).
     * An internal operator that does not appear in SQL syntax.
     */
    ESCAPE,

    /**
     * The internal REINTERPRET operator (meaning a reinterpret cast).
     * An internal operator that does not appear in SQL syntax.
     */
    REINTERPRET,

    /**
     * The internal {@code EXTEND} operator that qualifies a table name in the {@code FROM} clause.
     */
    EXTEND,

    /**
     * The internal {@code CUBE} operator that occurs within a {@code GROUP BY} clause.
     */
    CUBE,

    /**
     * The internal {@code ROLLUP} operator that occurs within a {@code GROUP BY} clause.
     */
    ROLLUP,

    /**
     * The internal {@code GROUPING SETS} operator that occurs within a {@code GROUP BY} clause.
     */
    GROUPING_SETS,

    /**
     * The {@code GROUPING(e, ...)} function.
     */
    GROUPING,

    /**
     * The {@code GROUP_ID()} function.
     */
    GROUP_ID,

    /**
     * The internal "permute" function in a MATCH_RECOGNIZE clause.
     */
    PATTERN_PERMUTE,

    /**
     * The special patterns to exclude enclosing pattern from output in a MATCH_RECOGNIZE clause.
     */
    PATTERN_EXCLUDED,

    // Aggregate functions

    /**
     * The {@code COUNT} aggregate function.
     */
    COUNT,

    /**
     * The {@code SUM} aggregate function.
     */
    SUM,

    /**
     * The {@code SUM0} aggregate function.
     */
    SUM0,

    /**
     * The {@code MIN} aggregate function.
     */
    MIN,

    /**
     * The {@code MAX} aggregate function.
     */
    MAX,

    /**
     * The {@code LEAD} aggregate function.
     */
    LEAD,

    /**
     * The {@code LAG} aggregate function.
     */
    LAG,

    /**
     * The {@code FIRST_VALUE} aggregate function.
     */
    FIRST_VALUE,

    /**
     * The {@code LAST_VALUE} aggregate function.
     */
    LAST_VALUE,

    /**
     * The {@code ANY_VALUE} aggregate function.
     */
    ANY_VALUE,

    /**
     * The {@code COVAR_POP} aggregate function.
     */
    COVAR_POP,

    /**
     * The {@code COVAR_SAMP} aggregate function.
     */
    COVAR_SAMP,

    /**
     * The {@code REGR_COUNT} aggregate function.
     */
    REGR_COUNT,

    /**
     * The {@code REGR_SXX} aggregate function.
     */
    REGR_SXX,

    /**
     * The {@code REGR_SYY} aggregate function.
     */
    REGR_SYY,

    /**
     * The {@code AVG} aggregate function.
     */
    AVG,

    /**
     * The {@code STDDEV_POP} aggregate function.
     */
    STDDEV_POP,

    /**
     * The {@code STDDEV_SAMP} aggregate function.
     */
    STDDEV_SAMP,

    /**
     * The {@code VAR_POP} aggregate function.
     */
    VAR_POP,

    /**
     * The {@code VAR_SAMP} aggregate function.
     */
    VAR_SAMP,

    /**
     * The {@code NTILE} aggregate function.
     */
    NTILE,

    /**
     * The {@code NTH_VALUE} aggregate function.
     */
    NTH_VALUE,

    /**
     * The {@code COLLECT} aggregate function.
     */
    COLLECT,

    /**
     * The {@code FUSION} aggregate function.
     */
    FUSION,

    /**
     * The {@code SINGLE_VALUE} aggregate function.
     */
    SINGLE_VALUE,

    /**
     * The {@code BIT_AND} aggregate function.
     */
    BIT_AND,

    /**
     * The {@code BIT_OR} aggregate function.
     */
    BIT_OR,

    /**
     * The {@code ROW_NUMBER} window function.
     */
    ROW_NUMBER,

    /**
     * The {@code RANK} window function.
     */
    RANK,

    /**
     * The {@code PERCENT_RANK} window function.
     */
    PERCENT_RANK,

    /**
     * The {@code DENSE_RANK} window function.
     */
    DENSE_RANK,

    /**
     * The {@code ROW_NUMBER} window function.
     */
    CUME_DIST,

    // Group functions

    /**
     * The {@code TUMBLE} group function.
     */
    TUMBLE,

    /**
     * The {@code TUMBLE_START} auxiliary function of the {@link #TUMBLE} group function.
     */
    TUMBLE_START,

    /**
     * The {@code TUMBLE_END} auxiliary function of the {@link #TUMBLE} group function.
     */
    TUMBLE_END,

    /**
     * The {@code HOP} group function.
     */
    HOP,

    /**
     * The {@code HOP_START} auxiliary function of the {@link #HOP} group function.
     */
    HOP_START,

    /**
     * The {@code HOP_END} auxiliary function of the {@link #HOP} group function.
     */
    HOP_END,

    /**
     * The {@code SESSION} group function.
     */
    SESSION,

    /**
     * The {@code SESSION_START} auxiliary function of the {@link #SESSION} group function.
     */
    SESSION_START,

    /**
     * The {@code SESSION_END} auxiliary function of the {@link #SESSION} group function.
     */
    SESSION_END,

    /**
     * Column declaration.
     */
    COLUMN_DECL,

    /**
     * Attribute definition.
     */
    ATTRIBUTE_DEF,

    /**
     * {@code CHECK} constraint.
     */
    CHECK,

    /**
     * {@code UNIQUE} constraint.
     */
    UNIQUE,

    /**
     * {@code PRIMARY KEY} constraint.
     */
    PRIMARY_KEY,

    /**
     * {@code FOREIGN KEY} constraint.
     */
    FOREIGN_KEY,

    // DDL and session control statements follow. The list is not exhaustive: feel free to add more.

    /**
     * {@code COMMIT} session control statement.
     */
    COMMIT,

    /**
     * {@code ROLLBACK} session control statement.
     */
    ROLLBACK,

    /**
     * {@code ALTER SESSION} DDL statement.
     */
    ALTER_SESSION,

    /**
     * {@code CREATE NAMESPACE} DDL statement.
     */
    CREATE_NAMESPACE,

    /**
     * {@code CREATE FOREIGN SCHEMA} DDL statement.
     */
    CREATE_FOREIGN_SCHEMA,

    /**
     * {@code ALTER NAMESPACE} DDL statement.
     */
    ALTER_NAMESPACE,

    /**
     * {@code DROP NAMESPACE} DDL statement.
     */
    DROP_NAMESPACE,

    /**
     * {@code CREATE TABLE} DDL statement.
     */
    CREATE_TABLE,

    /**
     * {@code TRUNCATE} DDL statement
     */
    TRUNCATE,

    /**
     * {@code ALTER TABLE} DDL statement.
     */
    ALTER_TABLE,

    /**
     * {@code DROP TABLE} DDL statement.
     */
    DROP_TABLE,

    /**
     * {@code ALTER TABLE xxx DROP COLUMN} DDL statement.
     */
    DROP_COLUMN,

    /**
     * {@code CREATE VIEW} DDL statement.
     */
    CREATE_VIEW,

    /**
     * {@code ALTER VIEW} DDL statement.
     */
    ALTER_VIEW,

    /**
     * {@code DROP VIEW} DDL statement.
     */
    DROP_VIEW,

    /**
     * {@code CREATE MATERIALIZED VIEW} DDL statement.
     */
    CREATE_MATERIALIZED_VIEW,

    /**
     * {@code ALTER MATERIALIZED VIEW} DDL statement.
     */
    ALTER_MATERIALIZED_VIEW,

    /**
     * {@code DROP MATERIALIZED VIEW} DDL statement.
     */
    DROP_MATERIALIZED_VIEW,

    /**
     * {@code CREATE SEQUENCE} DDL statement.
     */
    CREATE_SEQUENCE,

    /**
     * {@code ALTER SEQUENCE} DDL statement.
     */
    ALTER_SEQUENCE,

    /**
     * {@code DROP SEQUENCE} DDL statement.
     */
    DROP_SEQUENCE,

    /**
     * {@code CREATE INDEX} DDL statement.
     */
    CREATE_INDEX,

    /**
     * {@code ALTER INDEX} DDL statement.
     */
    ALTER_INDEX,

    /**
     * {@code DROP INDEX} DDL statement.
     */
    DROP_INDEX,

    /**
     * {@code CREATE TYPE} DDL statement.
     */
    CREATE_TYPE,

    /**
     * {@code DROP TYPE} DDL statement.
     */
    DROP_TYPE,

    /**
     * {@code CREATE FUNCTION} DDL statement.
     */
    CREATE_FUNCTION,

    /**
     * {@code DROP FUNCTION} DDL statement.
     */
    DROP_FUNCTION,

    /**
     * DDL statement not handled above.
     *
     */
    OTHER_DDL,

    CROSS_MODEL_ITEM,

    /**
     * Document model transform document into string representation
     */
    MQL_JSONIFY,

    /**
     * Document model single selected value of field
     */
    MQL_QUERY_VALUE,

    /**
     * Document model item operator, which retrieves from any underlying array
     */
    MQL_ITEM,

    /**
     * Document model {@code $size} operator
     */
    MQL_SIZE_MATCH,

    /**
     * Document model {@code $regex} operator
     */
    MQL_REGEX_MATCH,

    /**
     * Document model {@code $type} operator
     */
    MQL_TYPE_MATCH,

    /**
     * Document model {@code $slice} operator
     */
    MQL_SLICE,

    /**
     * Document model exclusive project {@code $project: 0}
     */
    MQL_EXCLUDE,

    /**
     * Document model {@code $elemMatch} operator
     */
    MQL_ELEM_MATCH,

    /**
     * Document model {@code $unwind} operator
     */
    UNWIND,

    /**
     * Document model {@code UPDATE} operator, which handles only REPLACE during updates
     */
    MQL_UPDATE_REPLACE,

    /**
     * Document model {@code UPDATE} operator, which handles new DOCUMENTS during updates
     */
    MQL_ADD_FIELDS,

    /**
     * Document model {@code UPDATE} operator, which handles removing DOCUMENTS during updates
     */
    MQL_UPDATE_REMOVE,

    /**
     * Document model {@code UPDATE} operator, which handles rename DOCUMENTS during updates
     */
    MQL_UPDATE_RENAME,

    /**
     * Document model {@code UPDATE} operator, which wrapes the other UPDATE operations
     */
    MQL_UPDATE,

    /**
     * Document model {@code $exists} operator
     */
    MQL_EXISTS,
    /*
     * Deserialize operator
     */
    DESERIALIZE,

    /*
     * CYPHER function
     */
    CYPHER_FUNCTION,

    ELEMENT_REF;

    // Most of the static fields are categories, aggregating several kinds into a set.

    /**
     * Category consisting of set-query node types.
     *
     * Consists of: {@link #EXCEPT}, {@link #INTERSECT}, {@link #UNION}.
     */
    public static final EnumSet<Kind> SET_QUERY = EnumSet.of( UNION, INTERSECT, EXCEPT );

    /**
     * Category consisting of all built-in aggregate functions.
     */
    public static final EnumSet<Kind> AGGREGATE = EnumSet.of(
            COUNT,
            SUM,
            SUM0,
            MIN,
            MAX,
            LEAD,
            LAG,
            FIRST_VALUE,
            LAST_VALUE,
            COVAR_POP,
            COVAR_SAMP,
            REGR_COUNT,
            REGR_SXX,
            REGR_SYY,
            AVG,
            STDDEV_POP,
            STDDEV_SAMP,
            VAR_POP,
            VAR_SAMP,
            NTILE,
            COLLECT,
            FUSION,
            SINGLE_VALUE,
            ROW_NUMBER,
            RANK,
            PERCENT_RANK,
            DENSE_RANK,
            CUME_DIST,
            JSON_ARRAYAGG,
            JSON_OBJECTAGG,
            BIT_AND,
            BIT_OR );

    /**
     * Category consisting of all DML operators.
     *
     * Consists of:
     * {@link #INSERT},
     * {@link #UPDATE},
     * {@link #DELETE},
     * {@link #MERGE},
     * {@link #PROCEDURE_CALL}.
     *
     * NOTE jvs 1-June-2006: For now we treat procedure calls as DML; this makes it easy for JDBC clients to call execute or
     * executeUpdate and not have to process dummy cursor results. If in the future we support procedures which return
     * results sets, we'll need to refine this.
     */
    public static final EnumSet<Kind> DML = EnumSet.of(
            INSERT,
            DELETE,
            UPDATE,
            MERGE,
            PROCEDURE_CALL );

    /**
     * Category consisting of all DDL operators.
     */
    public static final EnumSet<Kind> DDL = EnumSet.of(
            COMMIT,
            ROLLBACK,
            ALTER_SESSION,
            CREATE_NAMESPACE,
            CREATE_FOREIGN_SCHEMA,
            DROP_NAMESPACE,
            CREATE_TABLE,
            ALTER_TABLE,
            DROP_TABLE,
            CREATE_VIEW,
            ALTER_VIEW,
            DROP_VIEW,
            CREATE_MATERIALIZED_VIEW,
            ALTER_MATERIALIZED_VIEW,
            DROP_MATERIALIZED_VIEW,
            CREATE_SEQUENCE,
            ALTER_SEQUENCE,
            DROP_SEQUENCE,
            CREATE_INDEX,
            ALTER_INDEX,
            DROP_INDEX,
            CREATE_TYPE,
            DROP_TYPE,
            SET_OPTION,
            TRUNCATE,
            ALTER_NAMESPACE,
            OTHER_DDL );

    /**
     * Category consisting of query node types.
     *
     * Consists of:
     * {@link #SELECT},
     * {@link #EXCEPT},
     * {@link #INTERSECT},
     * {@link #UNION},
     * {@link #VALUES},
     * {@link #ORDER_BY},
     * {@link #EXPLICIT_TABLE}.
     */
    public static final EnumSet<Kind> QUERY = EnumSet.of(
            SELECT,
            UNION,
            INTERSECT,
            EXCEPT,
            VALUES,
            WITH,
            ORDER_BY,
            EXPLICIT_TABLE );

    /**
     * Category consisting of all expression operators.
     *
     * A node is an expression if it is NOT one of the following:
     * {@link #AS},
     * {@link #ARGUMENT_ASSIGNMENT},
     * {@link #DEFAULT},
     * {@link #DESCENDING},
     * {@link #SELECT},
     * {@link #JOIN},
     * {@link #OTHER_FUNCTION},
     * {@link #CAST},
     * {@link #TRIM},
     * {@link #LITERAL_CHAIN},
     * {@link #JDBC_FN},
     * {@link #PRECEDING},
     * {@link #FOLLOWING},
     * {@link #ORDER_BY},
     * {@link #COLLECTION_TABLE},
     * {@link #TABLESAMPLE},
     * or an aggregate function, DML or DDL.
     */
    public static final Set<Kind> EXPRESSION =
            EnumSet.complementOf(
                    concat(
                            EnumSet.of( AS, ARGUMENT_ASSIGNMENT, DEFAULT, RUNNING, FINAL, LAST, FIRST, PREV, NEXT, DESCENDING,
                                    CUBE, ROLLUP, GROUPING_SETS, EXTEND, LATERAL, SELECT, JOIN, OTHER_FUNCTION, POSITION,
                                    CAST, TRIM, FLOOR, CEIL, TIMESTAMP_ADD, TIMESTAMP_DIFF, EXTRACT, LITERAL_CHAIN, JDBC_FN,
                                    PRECEDING, FOLLOWING, ORDER_BY, NULLS_FIRST, NULLS_LAST, COLLECTION_TABLE, TABLESAMPLE,
                                    VALUES, WITH, WITH_ITEM, SKIP_TO_FIRST, SKIP_TO_LAST, JSON_VALUE_EXPRESSION,
                                    JSON_API_COMMON_SYNTAX ),
                            AGGREGATE, DML, DDL ) );

    /**
     * Category of all SQL statement types.
     *
     * Consists of all types in {@link #QUERY}, {@link #DML} and {@link #DDL}.
     */
    public static final EnumSet<Kind> TOP_LEVEL = concat( QUERY, DML, DDL );

    /**
     * Category consisting of regular and special functions.
     *
     * Consists of regular functions {@link #OTHER_FUNCTION} and special functions {@link #ROW}, {@link #TRIM}, {@link #CAST}, {@link #JDBC_FN}.
     */
    public static final Set<Kind> FUNCTION = EnumSet.of(
            OTHER_FUNCTION,
            ROW,
            TRIM,
            LTRIM,
            RTRIM,
            CAST,
            JDBC_FN,
            POSITION );

    /**
     * Category of SqlAvgAggFunction.
     *
     * Consists of {@link #AVG}, {@link #STDDEV_POP}, {@link #STDDEV_SAMP}, {@link #VAR_POP}, {@link #VAR_SAMP}.
     */
    public static final Set<Kind> AVG_AGG_FUNCTIONS = EnumSet.of(
            AVG,
            STDDEV_POP,
            STDDEV_SAMP,
            VAR_POP,
            VAR_SAMP );

    /**
     * Category of SqlCovarAggFunction.
     *
     * Consists of {@link #COVAR_POP}, {@link #COVAR_SAMP}, {@link #REGR_SXX}, {@link #REGR_SYY}.
     */
    public static final Set<Kind> COVAR_AVG_AGG_FUNCTIONS = EnumSet.of(
            COVAR_POP,
            COVAR_SAMP,
            REGR_COUNT,
            REGR_SXX,
            REGR_SYY );

    /**
     * Category of comparison operators.
     *
     * Consists of:
     * {@link #IN},
     * {@link #EQUALS},
     * {@link #NOT_EQUALS},
     * {@link #LESS_THAN},
     * {@link #GREATER_THAN},
     * {@link #LESS_THAN_OR_EQUAL},
     * {@link #GREATER_THAN_OR_EQUAL}.
     */
    public static final Set<Kind> COMPARISON = EnumSet.of(
            IN,
            EQUALS,
            NOT_EQUALS,
            LESS_THAN,
            GREATER_THAN,
            GREATER_THAN_OR_EQUAL,
            LESS_THAN_OR_EQUAL );

    public static final Set<Kind> ORDER = EnumSet.of( ORDER_BY );

    public static final Set<Kind> MQL_KIND = EnumSet.of(
            MQL_QUERY_VALUE,
            MQL_JSONIFY,
            MQL_ITEM,
            MQL_SIZE_MATCH,
            MQL_REGEX_MATCH,
            MQL_TYPE_MATCH,
            MQL_SLICE,
            MQL_EXCLUDE,
            MQL_ELEM_MATCH,
            UNWIND,
            MQL_UPDATE_REPLACE,
            MQL_ADD_FIELDS,
            MQL_UPDATE_REMOVE,
            MQL_UPDATE_RENAME,
            MQL_JSONIFY,
            MQL_UPDATE,
            MQL_EXISTS );

    /**
     * Lower-case name.
     */
    public final String lowerName = name().toLowerCase( Locale.ROOT );
    public final String sql;


    Kind() {
        sql = name();
    }


    Kind( String sql ) {
        this.sql = sql;
    }


    /**
     * Returns the kind that corresponds to this operator but in the opposite direction. Or returns this,
     * if this kind is not reversible.
     *
     * For example, {@code GREATER_THAN.reverse()} returns {@link #LESS_THAN}.
     */
    public Kind reverse() {
        switch ( this ) {
            case GREATER_THAN:
                return LESS_THAN;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL;
            case LESS_THAN:
                return GREATER_THAN;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL;
            default:
                return this;
        }
    }


    /**
     * Returns the kind that you get if you apply NOT to this kind.
     *
     * For example, {@code IS_NOT_NULL.negate()} returns {@link #IS_NULL}.
     *
     * For {@link #IS_TRUE}, {@link #IS_FALSE}, {@link #IS_NOT_TRUE}, {@link #IS_NOT_FALSE}, nullable inputs need to be
     * treated carefully.
     *
     * {@code NOT(IS_TRUE(null))} = {@code NOT(false)} = {@code true}, while {@code IS_FALSE(null)} = {@code false},
     * so {@code NOT(IS_TRUE(X))} should be {@code IS_NOT_TRUE(X)}.
     * On the other hand, {@code IS_TRUE(NOT(null))} = {@code IS_TRUE(null)} = {@code false}.
     *
     * This is why negate() != negateNullSafe() for these operators.
     */
    public Kind negate() {
        switch ( this ) {
            case IS_TRUE:
                return IS_NOT_TRUE;
            case IS_FALSE:
                return IS_NOT_FALSE;
            case IS_NULL:
                return IS_NOT_NULL;
            case IS_NOT_TRUE:
                return IS_TRUE;
            case IS_NOT_FALSE:
                return IS_FALSE;
            case IS_NOT_NULL:
                return IS_NULL;
            case IS_DISTINCT_FROM:
                return IS_NOT_DISTINCT_FROM;
            case IS_NOT_DISTINCT_FROM:
                return IS_DISTINCT_FROM;
            default:
                return this;
        }
    }


    /**
     * Returns the kind that you get if you negate this kind.
     * To conform to null semantics, null value should not be compared.
     *
     * For {@link #IS_TRUE}, {@link #IS_FALSE}, {@link #IS_NOT_TRUE} and {@link #IS_NOT_FALSE}, nullable inputs
     * need to be treated carefully:
     *
     * <ul>
     * <li>NOT(IS_TRUE(null)) = NOT(false) = true</li>
     * <li>IS_TRUE(NOT(null)) = IS_TRUE(null) = false</li>
     * <li>IS_FALSE(null) = false</li>
     * <li>IS_NOT_TRUE(null) = true</li>
     * </ul>
     */
    public Kind negateNullSafe() {
        switch ( this ) {
            case EQUALS:
                return NOT_EQUALS;
            case NOT_EQUALS:
                return EQUALS;
            case LESS_THAN:
                return GREATER_THAN_OR_EQUAL;
            case GREATER_THAN:
                return LESS_THAN_OR_EQUAL;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN;
            case IS_TRUE:
                return IS_FALSE;
            case IS_FALSE:
                return IS_TRUE;
            case IS_NOT_TRUE:
                return IS_NOT_FALSE;
            case IS_NOT_FALSE:
                return IS_NOT_TRUE;
            // (NOT x) IS NULL => x IS NULL
            // Similarly (NOT x) IS NOT NULL => x IS NOT NULL
            case IS_NOT_NULL:
            case IS_NULL:
                return this;
            default:
                return this.negate();
        }
    }


    /**
     * Returns whether this {@code Kind} belongs to a given category.
     *
     * A category is a collection of kinds, not necessarily disjoint.
     * For example, QUERY is { SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY, EXPLICIT_TABLE }.
     *
     * @param category Category
     * @return Whether this kind belongs to the given category
     */
    public final boolean belongsTo( Collection<Kind> category ) {
        return category.contains( this );
    }


    @SafeVarargs
    private static <E extends Enum<E>> EnumSet<E> concat( EnumSet<E> set0, EnumSet<E>... sets ) {
        EnumSet<E> set = set0.clone();
        for ( EnumSet<E> s : sets ) {
            set.addAll( s );
        }
        return set;
    }
}
