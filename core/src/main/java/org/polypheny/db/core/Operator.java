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

package org.polypheny.db.core;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Litmus;

public abstract class Operator {

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


    public Operator( String name, Kind kind, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker ) {
        this.name = name;
        this.kind = kind;
        this.returnTypeInference = returnTypeInference;
        this.operandTypeInference = operandTypeInference;
        this.operandTypeChecker = operandTypeChecker;
    }


    /**
     * Returns whether the given operands are valid. If not valid and {@code fail}, throws an assertion error.
     *
     * Similar to {#@link #checkOperandCount}, but some operators may have different valid operands in {@link Node} and {@code RexNode} formats (some examples are CAST and AND),
     * and this method throws internal errors, not user errors.
     */
    public boolean validRexOperands( int count, Litmus litmus ) {
        return true;
    }


    public abstract <T extends Call & Node> T createCall( Literal functionQualifier, ParserPos pos, Node... operands );


    /**
     * Creates a call to this operand with an array of operands.
     *
     * The position of the resulting call is the union of the <code>pos</code> and the positions of all the operands.
     *
     * @param pos Parser position
     * @param operands List of arguments
     * @return call to this operator
     */
    public final <T extends Call & Node> T  createCall( ParserPos pos, Node... operands ) {
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
    public final <T extends Call & Node> T createCall( NodeList nodeList ) {
        return createCall( null, nodeList.getPos(), nodeList.toArray() );
    }


    /**
     * Creates a call to this operand with a list of operands.
     *
     * The position of the resulting call is the union of the <code>pos</code> and the positions of all the operands.
     */
    public final <T extends Call & Node> T createCall( ParserPos pos, List<? extends Node> operandList ) {
        return createCall( null, pos, operandList.toArray( new Node[0] ) );
    }


    /**
     * Returns a string describing the expected operand types of a call, e.g. "SUBSTR(VARCHAR, INTEGER, INTEGER)".
     */
    public final String getAllowedSignatures() {
        return getAllowedSignatures( name );
    }


    /**
     * Returns a string describing the expected operand types of a call, e.g. "SUBSTRING(VARCHAR, INTEGER, INTEGER)" where the name (SUBSTRING in this example) can be replaced by a specified name.
     */
    public String getAllowedSignatures( String opNameToUse ) {
        assert operandTypeChecker != null : "If you see this, assign operandTypeChecker a value or override this function";
        return operandTypeChecker.getAllowedSignatures( this, opNameToUse ).trim();
    }

}
