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

package org.polypheny.db.nodes;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.BasicNodeVisitor.ArgHandler;
import org.polypheny.db.nodes.Function.FunctionType;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Litmus;

public abstract class OperatorImpl implements Operator {

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.OTHER;
    }


    /**
     * See {@link Kind}. It's possible to have a name that doesn't match the kind
     */
    @Getter
    public final Kind kind;
    /**
     * The name of the operator/function. Ex. "OVERLAY" or "TRIM"
     */
    @Getter
    protected final String name;
    /**
     * used to infer the return type of a call to this operator
     */
    protected final PolyReturnTypeInference returnTypeInference;
    /**
     * used to infer types of unknown operands
     */
    protected final PolyOperandTypeInference operandTypeInference;
    /**
     * used to validate operand types
     */
    protected final PolyOperandTypeChecker operandTypeChecker;

    @Getter
    private OperatorName operatorName;


    public OperatorImpl( String name, Kind kind, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker ) {
        this.name = name;
        this.kind = kind;
        this.returnTypeInference = returnTypeInference;
        this.operandTypeInference = operandTypeInference;
        this.operandTypeChecker = operandTypeChecker;
    }


    public void setOperatorName( OperatorName operatorName ) {
        if ( this.operatorName != null ) {
            throw new GenericRuntimeException( "The operatorName can only be set once." );
        }
        this.operatorName = operatorName;
    }


    /**
     * Returns whether the given operands are valid. If not valid and {@code fail}, throws an assertion error.
     *
     * Similar to {#@link #checkOperandCount}, but some operators may have different valid operands in {@link Node} and {@code RexNode} formats (some examples are CAST and AND),
     * and this method throws internal errors, not user errors.
     */
    @Override
    public boolean validRexOperands( int count, Litmus litmus ) {
        return true;
    }


    /**
     * Creates a call to this operand with an array of operands.
     *
     * The position of the resulting call is the union of the <code>pos</code> and the positions of all the operands.
     *
     * @param pos Parser position
     * @param operands List of arguments
     * @return call to this operator
     */
    @Override
    public final Call createCall( ParserPos pos, Node... operands ) {
        return createCall( null, pos, operands );
    }


    /**
     * Creates a call to this operand with a list of operands contained in a {@link NodeList}.
     *
     * The position of the resulting call inferred from the SqlNodeList.
     *
     * @param nodeList List of arguments
     * @return call to this operator
     */
    @Override
    public final Call createCall( NodeList nodeList ) {
        return createCall( null, nodeList.getPos(), nodeList.toArray() );
    }


    /**
     * Creates a call to this operand with a list of operands.
     *
     * The position of the resulting call is the union of the <code>pos</code> and the positions of all the operands.
     */
    @Override
    public final Call createCall( ParserPos pos, List<? extends Node> operandList ) {
        return createCall( null, pos, operandList.toArray( new Node[0] ) );
    }


    /**
     * Returns a string describing the expected operand types of a call, e.g. "SUBSTR(VARCHAR, INTEGER, INTEGER)".
     */
    @Override
    public final String getAllowedSignatures() {
        return getAllowedSignatures( name );
    }


    /**
     * Returns a string describing the expected operand types of a call, e.g. "SUBSTRING(VARCHAR, INTEGER, INTEGER)" where the name (SUBSTRING in this example) can be replaced by a specified name.
     */
    @Override
    public String getAllowedSignatures( String opNameToUse ) {
        assert operandTypeChecker != null : "If you see this, assign operandTypeChecker a value or override this function";
        return operandTypeChecker.getAllowedSignatures( this, opNameToUse ).trim();
    }


    /**
     * Accepts a {@link NodeVisitor}, visiting each operand of a call. Returns null.
     *
     * @param visitor Visitor
     * @param call Call to visit
     */
    @Override
    public <R> R acceptCall( NodeVisitor<R> visitor, Call call ) {
        for ( Node operand : call.getOperandList() ) {
            if ( operand == null ) {
                continue;
            }
            operand.accept( visitor );
        }
        return null;
    }


    /**
     * Accepts a {@link NodeVisitor}, directing an {@link ArgHandler} to visit an operand of a call.
     *
     * The argument handler allows fine control about how the operands are visited, and how the results are combined.
     *
     * @param visitor Visitor
     * @param call Call to visit
     * @param onlyExpressions If true, ignores operands which are not expressions. For example, in the call to the <code>AS</code> operator
     * @param argHandler Called for each operand
     */
    @Override
    public <R> void acceptCall( NodeVisitor<R> visitor, Call call, boolean onlyExpressions, ArgHandler<R> argHandler ) {
        List<Node> operands = call.getOperandList();
        for ( int i = 0; i < operands.size(); i++ ) {
            argHandler.visitChild( visitor, call, i, operands.get( i ) );
        }
    }


    /**
     * Returns whether this operator is an aggregate function. By default, subclass type is used (an instance of SqlAggFunction is assumed to be an aggregator; anything else is not).
     *
     * Per SQL:2011, there are <dfn>aggregate functions</dfn> and <dfn>window functions</dfn>.
     * Every aggregate function (e.g. SUM) is also a window function.
     * There are window functions that are not aggregate functions, e.g. RANK, NTILE, LEAD, FIRST_VALUE.</p>
     *
     * Collectively, aggregate and window functions are called <dfn>analytic functions</dfn>. Despite its name, this method returns true for every analytic function.
     *
     * @return whether this operator is an analytic function (aggregate function or window function)
     * #@see #requiresOrder()
     */
    @Override
    public boolean isAggregator() {
        return false;
    }


    /**
     * Returns whether this is a window function that requires an OVER clause.
     *
     * For example, returns true for {@code RANK}, {@code DENSE_RANK} and other ranking functions; returns false for {@code SUM}, {@code COUNT}, {@code MIN}, {@code MAX}, {@code AVG}
     * (they can be used as non-window aggregate functions).
     *
     * If {@code requiresOver} returns true, then {@link #isAggregator()} must also return true.
     *
     * #@see #allowsFraming()
     *
     * @see #requiresOrder()
     */
    @Override
    public boolean requiresOver() {
        return false;
    }


    /**
     * Returns whether this is a window function that requires ordering.
     *
     * Per SQL:2011, 2, 6.10: "If &lt;ntile function&gt;, &lt;lead or lag function&gt;, RANK or DENSE_RANK is specified, then the window ordering clause shall be present."
     *
     * @see #isAggregator()
     */
    @Override
    public boolean requiresOrder() {
        return false;
    }


    /**
     * Returns whether this is a group function.
     *
     * Group functions can only appear in the GROUP BY clause.
     *
     * Examples are {@code HOP}, {@code TUMBLE}, {@code SESSION}.
     *
     * Group functions have auxiliary functions, e.g. {@code HOP_START}, but these are not group functions.
     */
    @Override
    public boolean isGroup() {
        return false;
    }


    /**
     * Returns whether this is an group auxiliary function.
     *
     * Examples are {@code HOP_START} and {@code HOP_END} (both auxiliary to {@code HOP}).
     *
     * @see #isGroup()
     */
    @Override
    public boolean isGroupAuxiliary() {
        return false;
    }


    /**
     * @return true iff a call to this operator is guaranteed to always return the same result given the same operands; true is assumed by default
     */
    @Override
    public boolean isDeterministic() {
        return true;
    }


    /**
     * @return true iff it is unsafe to cache query plans referencing this operator; false is assumed by default
     */
    @Override
    public boolean isDynamicFunction() {
        return false;
    }


    /**
     * Method to check if call requires expansion when it has decimal operands.
     * The default implementation is to return true.
     */
    @Override
    public boolean requiresDecimalExpansion() {
        return true;
    }


    /**
     * Returns a template describing how the operator signature is to be built.
     * E.g for the binary + operator the template looks like "{1} {0} {2}" {0} is the operator, subsequent numbers are operands.
     *
     * @param operandsCount is used with functions that can take a variable number of operands
     * @return signature template, or null to indicate that a default template will suffice
     */
    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        return null;
    }


    /**
     * Returns whether a call to this operator is monotonic.
     *
     * Default implementation returns {@link Monotonicity#NOT_MONOTONIC}.
     *
     * @param call Call to this operator with particular arguments and information about the monotonicity of the arguments
     */
    @Override
    public Monotonicity getMonotonicity( OperatorBinding call ) {
        return Monotonicity.NOT_MONOTONIC;
    }

}
