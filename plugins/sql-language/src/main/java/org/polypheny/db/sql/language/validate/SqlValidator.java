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

package org.polypheny.db.sql.language.validate;


import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlDelete;
import org.polypheny.db.sql.language.SqlDynamicParam;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlInsert;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlMatchRecognize;
import org.polypheny.db.sql.language.SqlMerge;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlSelect;
import org.polypheny.db.sql.language.SqlUpdate;
import org.polypheny.db.sql.language.SqlWindow;
import org.polypheny.db.sql.language.SqlWith;
import org.polypheny.db.sql.language.SqlWithItem;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Glossary;
import org.polypheny.db.util.Util;


/**
 * Validates the parse tree of a SQL statement, and provides semantic information about the parse tree.
 *
 * To create an instance of the default validator implementation, call {@link SqlValidatorUtil#newValidator}.
 *
 * <h2>Visitor pattern</h2>
 *
 * The validator interface is an instance of the {@link Glossary#VISITOR_PATTERN visitor pattern}.
 * Implementations of the {@link SqlNode#validate} method call the <code>validateXxx</code> method appropriate to the kind of node:
 *
 * <ul>
 * <li>{@link SqlLiteral#validate(SqlValidator, SqlValidatorScope)} calls {@link #validateLiteral(SqlLiteral)};</li>
 * <li>{@link SqlCall#validate(SqlValidator, SqlValidatorScope)} calls {@link #validateCall(SqlCall, SqlValidatorScope)};</li>
 * <li>and so forth.</li>
 * </ul>
 *
 * The {@link SqlNode#validateExpr(SqlValidator, SqlValidatorScope)} method is as {@link SqlNode#validate(SqlValidator, SqlValidatorScope)} but is called when the node is known to be a scalar expression.
 *
 * <h2>Scopes and namespaces</h2>
 *
 * In order to resolve names to objects, the validator builds a map of the structure of the query. This map consists of two types of objects. A {@link SqlValidatorScope} describes the tables and columns accessible at a
 * particular point in the query; and a {@link SqlValidatorNamespace} is a description of a data source used in a query.
 *
 * There are different kinds of namespace for different parts of the query. For example {@link IdentifierNamespace} for table names, {@link SelectNamespace} for SELECT queries, {@link SetopNamespace}
 * for UNION, EXCEPT and INTERSECT. A validator is allowed to wrap namespaces in other objects which implement {@link SqlValidatorNamespace}, so don't try to cast your namespace or use <code>instanceof</code>; use
 * {@link SqlValidatorNamespace#unwrap(Class)} and
 * {@link SqlValidatorNamespace#isWrapperFor(Class)} instead.
 *
 * The validator builds the map by making a quick relScan over the query when the root {@link SqlNode} is first provided. Thereafter, it supplies the correct scope or namespace object when it calls validation methods.
 *
 * The methods {@link #getSelectScope}, {@link #getFromScope}, {@link #getWhereScope}, {@link #getGroupScope}, {@link #getHavingScope}, {@link #getOrderScope} and {@link #getJoinScope} get the correct scope to resolve
 * names in a particular clause of a SQL statement.
 */
public interface SqlValidator extends Validator {

    /**
     * Whether to follow the SQL standard strictly.
     */
    boolean STRICT = Util.getBooleanProperty( "polyphenydb.strict.sql" );

    /**
     * Returns the dialect of SQL (SQL:2003, etc.) this validator recognizes.
     * Default is {@link ConformanceEnum#DEFAULT}.
     *
     * @return dialect of SQL this validator recognizes
     */
    Conformance getConformance();

    /**
     * Returns the catalog reader used by this validator.
     *
     * @return catalog reader
     */
    Snapshot getSnapshot();

    /**
     * Returns the operator table used by this validator.
     *
     * @return operator table
     */
    OperatorTable getOperatorTable();

    /**
     * Validates an expression tree. You can call this method multiple times, but not reentrantly.
     *
     * @param topNode top of expression tree to be validated
     * @return validated tree (possibly rewritten)
     */
    SqlNode validateSql( SqlNode topNode );

    /**
     * Validates an expression tree. You can call this method multiple times, but not reentrantly.
     *
     * @param topNode top of expression tree to be validated
     * @param nameToTypeMap map of simple name to {@link AlgDataType}; used to resolve {@link SqlIdentifier} references
     * @return validated tree (possibly rewritten)
     */
    SqlNode validateParameterizedExpression( SqlNode topNode, Map<String, AlgDataType> nameToTypeMap );

    /**
     * Checks that a query is valid.
     *
     * Valid queries include:
     *
     * <ul>
     * <li><code>SELECT</code> statement,</li>
     * <li>set operation (<code>UNION</code>, <code>INTERSECT</code>, <code>EXCEPT</code>)</li>
     * <li>identifier (e.g. representing use of a table in a FROM clause)</li>
     * <li>query aliased with the <code>AS</code> operator</li>
     * </ul>
     *
     * @param node Query node
     * @param scope Scope in which the query occurs
     * @param targetRowType Desired row type, must not be null, may be the data type 'unknown'.
     * @throws RuntimeException if the query is not valid
     */
    void validateQuery( SqlNode node, SqlValidatorScope scope, AlgDataType targetRowType );

    /**
     * Returns the type assigned to a node by validation, or null if unknown.
     * This allows for queries against nodes such as aliases, which have no type of their own. If you want to assert that the node of interest must have a type, use {@link #getValidatedNodeType} instead.
     *
     * @param node the node of interest
     * @return validated type, or null if unknown or not applicable
     */
    AlgDataType getValidatedNodeTypeIfKnown( SqlNode node );

    /**
     * Resolves an identifier to a fully-qualified name.
     *
     * @param id Identifier
     * @param scope Naming scope
     */
    void validateIdentifier( SqlIdentifier id, SqlValidatorScope scope );

    /**
     * Validates a literal.
     *
     * @param literal Literal
     */
    void validateLiteral( SqlLiteral literal );

    /**
     * Validates a {@link SqlIntervalQualifier}
     *
     * @param qualifier Interval qualifier
     */
    void validateIntervalQualifier( SqlIntervalQualifier qualifier );

    /**
     * Validates an INSERT statement.
     *
     * @param insert INSERT statement
     */
    void validateInsert( SqlInsert insert );

    /**
     * Validates an UPDATE statement.
     *
     * @param update UPDATE statement
     */
    void validateUpdate( SqlUpdate update );

    /**
     * Validates a DELETE statement.
     *
     * @param delete DELETE statement
     */
    void validateDelete( SqlDelete delete );

    /**
     * Validates a MERGE statement.
     *
     * @param merge MERGE statement
     */
    void validateMerge( SqlMerge merge );

    /**
     * Validates a data type expression.
     *
     * @param dataType Data type
     */
    void validateDataType( SqlDataTypeSpec dataType );

    /**
     * Validates a dynamic parameter.
     *
     * @param dynamicParam Dynamic parameter
     */
    void validateDynamicParam( SqlDynamicParam dynamicParam );

    /**
     * Validates the right-hand side of an OVER expression. It might be either an {@link SqlIdentifier identifier} referencing a window, or an {@link SqlWindow inline window specification}.
     *
     * @param windowOrId SqlNode that can be either SqlWindow with all the components of a window spec or a SqlIdentifier with the name of a window spec.
     * @param scope Naming scope
     * @param call the SqlNode if a function call if the window is attached to one.
     */
    void validateWindow( SqlNode windowOrId, SqlValidatorScope scope, SqlCall call );

    /**
     * Validates a MATCH_RECOGNIZE clause.
     *
     * @param pattern MATCH_RECOGNIZE clause
     */
    void validateMatchRecognize( SqlCall pattern );

    /**
     * Validates a call to an operator.
     *
     * @param call Operator call
     * @param scope Naming scope
     */
    void validateCall( SqlCall call, SqlValidatorScope scope );

    /**
     * Validates parameters for aggregate function.
     *
     * @param aggCall Call to aggregate function
     * @param filter Filter ({@code FILTER (WHERE)} clause), or null
     * @param orderList Ordering specification ({@code WITHING GROUP} clause), or null
     * @param scope Syntactic scope
     */
    void validateAggregateParams( SqlCall aggCall, SqlNode filter, SqlNodeList orderList, SqlValidatorScope scope );

    /**
     * Validates a COLUMN_LIST parameter
     *
     * @param function function containing COLUMN_LIST parameter
     * @param argTypes function arguments
     * @param operands operands passed into the function call
     */
    void validateColumnListParams( SqlFunction function, List<AlgDataType> argTypes, List<Node> operands );

    /**
     * Returns whether a SELECT statement is an aggregation. Criteria are:
     * (1) contains GROUP BY, or
     * (2) contains HAVING, or
     * (3) SELECT or ORDER BY
     * clause contains aggregate functions. (Windowed aggregate functions, such as <code>SUM(x) OVER w</code>, don't count.)
     *
     * @param select SELECT statement
     * @return whether SELECT statement is an aggregation
     */
    boolean isAggregate( SqlSelect select );

    /**
     * Returns whether a select list expression is an aggregate function.
     *
     * @param selectNode Expression in SELECT clause
     * @return whether expression is an aggregate function
     */
    @Deprecated
    // to be removed before 2.0
    boolean isAggregate( SqlNode selectNode );

    /**
     * Converts a window specification or window name into a fully-resolved window specification. For example, in <code>SELECT sum(x) OVER (PARTITION BY x ORDER BY y), sum(y) OVER w1, sum(z) OVER (w ORDER BY y) FROM t WINDOW w AS (PARTITION BY x)</code>
     * all aggregations have the same resolved window specification <code>(PARTITION BY x ORDER BY y)</code>.
     *
     * @param windowOrRef Either the name of a window (a {@link SqlIdentifier}) or a window specification (a {@link SqlWindow}).
     * @param scope Scope in which to resolve window names
     * @param populateBounds Whether to populate bounds. Doing so may alter the definition of the window. It is recommended that populate bounds when translating to physical algebra, but not when validating.
     * @return A window
     * @throws RuntimeException Validation exception if window does not exist
     */
    SqlWindow resolveWindow( SqlNode windowOrRef, SqlValidatorScope scope, boolean populateBounds );

    /**
     * Finds the namespace corresponding to a given node.
     *
     * For example, in the query <code>SELECT * FROM (SELECT * FROM t), t1 AS alias</code>, the both items in the FROM clause have a corresponding namespace.
     *
     * @param node Parse tree node
     * @return namespace of node
     */
    SqlValidatorNamespace getSqlNamespace( SqlNode node );

    /**
     * Derives an alias for an expression. If no alias can be derived, returns null if <code>ordinal</code> is less than zero, otherwise generates an alias <code>EXPR$<i>ordinal</i></code>.
     *
     * @param node Expression
     * @param ordinal Ordinal of expression
     * @return derived alias, or null if no alias can be derived and ordinal is less than zero
     */
    String deriveAlias( SqlNode node, int ordinal );

    /**
     * Returns a list of expressions, with every occurrence of "&#42;" or "TABLE.&#42;" expanded.
     *
     * @param selectList Select clause to be expanded
     * @param query Query
     * @param includeSystemVars Whether to include system variables
     * @return expanded select clause
     */
    SqlNodeList expandStar( SqlNodeList selectList, SqlSelect query, boolean includeSystemVars );

    /**
     * Returns the scope that expressions in the WHERE and GROUP BY clause of this query should use. This scope consists of the tables in the FROM clause, and the enclosing scope.
     *
     * @param select Query
     * @return naming scope of WHERE clause
     */
    SqlValidatorScope getWhereScope( SqlSelect select );

    /**
     * Removes a node from the set of validated nodes
     *
     * @param node node to be removed
     */
    void removeValidatedNodeType( SqlNode node );

    /**
     * Returns the appropriate scope for validating a particular clause of a SELECT statement.
     *
     * Consider
     *
     * <blockquote><pre><code>
     * SELECT *
     * FROM foo
     * WHERE EXISTS (
     *    SELECT deptno AS x
     *    FROM emp
     *       JOIN dept ON emp.deptno = dept.deptno
     *    WHERE emp.deptno = 5
     *    GROUP BY deptno
     *    ORDER BY x)
     * </code></pre></blockquote>
     *
     * What objects can be seen in each part of the sub-query?
     *
     * <ul>
     * <li>In FROM ({@link #getFromScope} , you can only see 'foo'.</li>
     * <li>In WHERE ({@link #getWhereScope}), GROUP BY ({@link #getGroupScope}), SELECT ({@code getSelectScope}), and the ON clause of the JOIN ({@link #getJoinScope}) you can see 'emp', 'dept', and 'foo'.</li>
     * <li>In ORDER BY ({@link #getOrderScope}), you can see the column alias 'x'; and tables 'emp', 'dept', and 'foo'.</li>
     * </ul>
     *
     * @param select SELECT statement
     * @return naming scope for SELECT statement
     */
    SqlValidatorScope getSelectScope( SqlSelect select );

    /**
     * Returns the scope for resolving the SELECT, GROUP BY and HAVING clauses.
     * Always a {@link SelectScope}; if this is an aggregation query, the {@link AggregatingScope} is stripped away.
     *
     * @param select SELECT statement
     * @return naming scope for SELECT statement, sans any aggregating scope
     */
    SelectScope getRawSelectScope( SqlSelect select );

    /**
     * Returns a scope containing the objects visible from the FROM clause of a query.
     *
     * @param select SELECT statement
     * @return naming scope for FROM clause
     */
    SqlValidatorScope getFromScope( SqlSelect select );

    /**
     * Returns a scope containing the objects visible from the ON and USING sections of a JOIN clause.
     *
     * @param node The item in the FROM clause which contains the ON or USING expression
     * @return naming scope for JOIN clause
     * @see #getFromScope
     */
    SqlValidatorScope getJoinScope( SqlNode node );

    /**
     * Returns a scope containing the objects visible from the GROUP BY clause of a query.
     *
     * @param select SELECT statement
     * @return naming scope for GROUP BY clause
     */
    SqlValidatorScope getGroupScope( SqlSelect select );

    /**
     * Returns a scope containing the objects visible from the HAVING clause of a query.
     *
     * @param select SELECT statement
     * @return naming scope for HAVING clause
     */
    SqlValidatorScope getHavingScope( SqlSelect select );

    /**
     * Returns the scope that expressions in the SELECT and HAVING clause of this query should use. This scope consists of the FROM clause and the enclosing scope. If the query is aggregating,
     * only columns in the GROUP BY clause may be used.
     *
     * @param select SELECT statement
     * @return naming scope for ORDER BY clause
     */
    SqlValidatorScope getOrderScope( SqlSelect select );

    /**
     * Returns a scope match recognize clause.
     *
     * @param node Match recognize
     * @return naming scope for Match recognize clause
     */
    SqlValidatorScope getMatchRecognizeScope( SqlMatchRecognize node );

    /**
     * Declares a SELECT expression as a cursor.
     *
     * @param select select expression associated with the cursor
     * @param scope scope of the parent query associated with the cursor
     */
    void declareCursor( SqlSelect select, SqlValidatorScope scope );

    /**
     * Pushes a new instance of a function call on to a function call stack.
     */
    void pushFunctionCall();

    /**
     * Removes the topmost entry from the function call stack.
     */
    void popFunctionCall();

    /**
     * Retrieves the name of the parent cursor referenced by a column list parameter.
     *
     * @param columnListParamName name of the column list parameter
     * @return name of the parent cursor
     */
    String getParentCursor( String columnListParamName );

    /**
     * Enables or disables expansion of column references. (Currently this does not apply to the ORDER BY clause; may be fixed in the future.)
     *
     * @param expandColumnReferences new setting
     */
    void setColumnReferenceExpansion( boolean expandColumnReferences );

    /**
     * @return whether column reference expansion is enabled
     */
    boolean getColumnReferenceExpansion();

    /**
     * Returns how NULL values should be collated if an ORDER BY item does not contain NULLS FIRST or NULLS LAST.
     */
    NullCollation getDefaultNullCollation();

    /**
     * Returns expansion of identifiers.
     *
     * @return whether this validator should expand identifiers
     */
    boolean shouldExpandIdentifiers();

    /**
     * Enables or disables rewrite of "macro-like" calls such as COALESCE.
     *
     * @param rewriteCalls new setting
     */
    void setCallRewrite( boolean rewriteCalls );

    /**
     * Derives the type of a constructor.
     *
     * @param scope Scope
     * @param call Call
     * @param unresolvedConstructor TODO
     * @param resolvedConstructor TODO
     * @param argTypes Types of arguments
     * @return Resolved type of constructor
     */
    AlgDataType deriveConstructorType( SqlValidatorScope scope, SqlCall call, SqlFunction unresolvedConstructor, SqlFunction resolvedConstructor, List<AlgDataType> argTypes );

    /**
     * Handles a call to a function which cannot be resolved. Returns a an appropriately descriptive error, which caller must throw.
     *
     * @param call Call
     * @param unresolvedFunction Overloaded function which is the target of the call
     * @param argTypes Types of arguments
     * @param argNames Names of arguments, or null if call by position
     */
    PolyphenyDbException handleUnresolvedFunction( SqlCall call, SqlFunction unresolvedFunction, List<AlgDataType> argTypes, List<String> argNames );

    /**
     * Expands an expression in the ORDER BY clause into an expression with the same semantics as expressions in the SELECT clause.
     *
     * This is made necessary by a couple of dialect 'features':
     *
     * <ul>
     * <li><b>ordinal expressions</b>: In "SELECT x, y FROM t ORDER BY 2", the expression "2" is shorthand for the 2nd item in the select clause, namely "y".</li>
     * <li><b>alias references</b>: In "SELECT x AS a, y FROM t ORDER BY a", the expression "a" is shorthand for the item in the select clause whose alias is "a"</li>
     * </ul>
     *
     * @param select Select statement which contains ORDER BY
     * @param orderExpr Expression in the ORDER BY clause.
     * @return Expression translated into SELECT clause semantics
     */
    SqlNode expandOrderExpr( SqlSelect select, SqlNode orderExpr );

    /**
     * Expands an expression.
     *
     * @param expr Expression
     * @param scope Scope
     * @return Expanded expression
     */
    SqlNode expand( SqlNode expr, SqlValidatorScope scope );

    /**
     * Returns the scope of an OVER or VALUES node.
     *
     * @param node Node
     * @return Scope
     */
    SqlValidatorScope getOverScope( SqlNode node );

    /**
     * Validates that a query is capable of producing a return of given modality (relational or streaming).
     *
     * @param select Query
     * @param modality Modality (streaming or relational)
     * @param fail Whether to throw a user error if does not support required modality
     * @return whether query supports the given modality
     */
    boolean validateModality( SqlSelect select, Modality modality, boolean fail );

    void validateWith( SqlWith with, SqlValidatorScope scope );

    void validateWithItem( SqlWithItem withItem );

    void validateSequenceValue( SqlValidatorScope scope, SqlIdentifier id );

    SqlValidatorScope getWithScope( SqlNode withItem );

}
