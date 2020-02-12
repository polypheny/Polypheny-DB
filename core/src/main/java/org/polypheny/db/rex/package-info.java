
/**
 * Provides a language for representing row-expressions.
 *
 * <h2>Life-cycle</h2>
 *
 * A {@link ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlToRelConverter} converts a SQL parse tree consisting of {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode} objects into a relational expression ({@link ch.unibas.dmi.dbis.polyphenydb.rel.RelNode}).
 * Several kinds of nodes in this tree have row expressions ({@link ch.unibas.dmi.dbis.polyphenydb.rex.RexNode}).
 *
 * After the relational expression has been optimized, a {@link ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.JavaRelImplementor} converts it into to a plan. If the plan is a Java parse tree, row-expressions are
 * translated into equivalent Java expressions.
 *
 * <h2>Expressions</h2>
 *
 *
 * Every row-expression has a type. (Compare with {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode}, which is created before validation, and therefore types may not be available.)
 *
 * Every node in the parse tree is a {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexNode}. Sub-types are:<
 * <ul>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral} represents a boolean, numeric, string, or date constant, or the value <code>NULL</code>.</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexVariable} represents a leaf of the tree. It has sub-types:
 * <ul>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexCorrelVariable} is a correlating variable for nested-loop joins</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef} refers to a field of an input relational expression</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexCall} is a call to an operator or function.  By means of special operators, we can use this construct to represent virtually every non-leaf node in the tree.</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexRangeRef} refers to a collection of contiguous fields from an input relational expression. It usually exists only during translation.</li>
 * </ul>
 * </li>
 * </ul>
 *
 * Expressions are generally created using a {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder} factory.
 *
 * <h2>Related packages</h2>
 * <ul>
 * <li>{@code ch.unibas.dmi.dbis.polyphenydb.sql} SQL object model</li>
 * <li>{@code ch.unibas.dmi.dbis.polyphenydb.plan} Core classes, including {@link ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType} and {@link ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory}.</li>
 * </ul>
 */

package ch.unibas.dmi.dbis.polyphenydb.rex;

