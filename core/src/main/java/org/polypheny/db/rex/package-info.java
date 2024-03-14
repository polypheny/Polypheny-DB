
/**
 * Provides a language for representing row-expressions.
 *
 * <h2>Life-cycle</h2>
 *
 * A {@link org.polypheny.db.languages.NodeToAlgConverter} converts an SQL parse tree consisting of {@link org.polypheny.db.interpreter.Node} objects into an algebra expression ({@link org.polypheny.db.algebra.AlgNode}).
 * Several kinds of nodes in this tree have row expressions ({@link org.polypheny.db.rex.RexNode}).
 *
 * After the algebra expression has been optimized, a {@link org.polypheny.db.algebra.enumerable.JavaAlgImplementor} converts it into to a plan. If the plan is a Java parse tree, row-expressions are
 * translated into equivalent Java expressions.
 *
 * <h2>Expressions</h2>
 *
 *
 * Every row-expression has a type. (Compare with {@link org.polypheny.db.interpreter.Node}, which is created before validation, and therefore types may not be available.)
 *
 * Every node in the parse tree is a {@link org.polypheny.db.rex.RexNode}. Sub-types are:
 * <ul>
 * <li>{@link org.polypheny.db.rex.RexLiteral} represents a boolean, numeric, string, or date constant, or the value <code>NULL</code>.</li>
 * <li>{@link org.polypheny.db.rex.RexVariable} represents a leaf of the tree. It has sub-types:
 * <ul>
 * <li>{@link org.polypheny.db.rex.RexCorrelVariable} is a correlating variable for nested-loop joins</li>
 * <li>{@link org.polypheny.db.rex.RexIndexRef} refers to a field of an input algebra expression</li>
 * <li>{@link org.polypheny.db.rex.RexCall} is a call to an operator or function.  By means of special operators, we can use this construct to represent virtually every non-leaf node in the tree.</li>
 * <li>{@link org.polypheny.db.rex.RexRangeRef} refers to a collection of contiguous fields from an input algebra expression. It usually exists only during translation.</li>
 * </ul>
 * </li>
 * </ul>
 *
 * Expressions are generally created using a {@link org.polypheny.db.rex.RexBuilder} factory.
 *
 * <h2>Related packages</h2>
 * <ul>
 * <li>{@code org.polypheny.db.sql} SQL object model</li>
 * <li>{@code org.polypheny.db.plan} Core classes, including {@link org.polypheny.db.algebra.type.AlgDataType} and {@link org.polypheny.db.algebra.type.AlgDataTypeFactory}.</li>
 * </ul>
 */

package org.polypheny.db.rex;

