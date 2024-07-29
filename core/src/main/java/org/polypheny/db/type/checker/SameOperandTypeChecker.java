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

package org.polypheny.db.type.checker;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Util;


/**
 * Parameter type-checking strategy where all operand types must be the same.
 */
public class SameOperandTypeChecker implements PolySingleOperandTypeChecker {

    protected final int nOperands;


    public SameOperandTypeChecker( int nOperands ) {
        this.nOperands = nOperands;
    }


    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }


    @Override
    public boolean isOptional( int i ) {
        return false;
    }


    @Override
    public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
        if ( !(callBinding instanceof OperatorBinding) ) {
            throw new GenericRuntimeException( "OperatorBinding and CallBinding do need to inherit differently" );
        }
        return checkOperandTypesImpl( (OperatorBinding) callBinding, throwOnFailure, callBinding );
    }


    protected List<Integer> getOperandList( int operandCount ) {
        return nOperands == -1
                ? Util.range( 0, operandCount )
                : Util.range( 0, nOperands );
    }


    protected boolean checkOperandTypesImpl( OperatorBinding operatorBinding, boolean throwOnFailure, CallBinding callBinding ) {
        int nOperandsActual = nOperands;
        if ( nOperandsActual == -1 ) {
            nOperandsActual = operatorBinding.getOperandCount();
        }
        assert !(throwOnFailure && (callBinding == null));
        AlgDataType[] types = new AlgDataType[nOperandsActual];
        final List<Integer> operandList = getOperandList( operatorBinding.getOperandCount() );
        for ( int i : operandList ) {
            types[i] = operatorBinding.getOperandType( i );
        }
        int prev = -1;
        for ( int i : operandList ) {
            if ( prev >= 0 ) {
                if ( !PolyTypeUtil.isComparable( types[i], types[prev] ) ) {
                    if ( !throwOnFailure ) {
                        return false;
                    }

                    // REVIEW jvs: Why don't we use newValidationSignatureError() here?  It gives more specific diagnostics.
                    throw callBinding.newValidationError( RESOURCE.needSameTypeParameter() );
                }
            }
            prev = i;
        }
        return true;
    }


    /**
     * Similar functionality to {@link PolyOperandTypeChecker#checkOperandTypes(CallBinding, boolean)}, but not part of the interface, and
     * cannot throw an error.
     */
    public boolean checkOperandTypes( OperatorBinding operatorBinding ) {
        return checkOperandTypesImpl( operatorBinding, false, null );
    }


    // implement SqlOperandTypeChecker
    @Override
    public OperandCountRange getOperandCountRange() {
        if ( nOperands == -1 ) {
            return PolyOperandCountRanges.any();
        } else {
            return PolyOperandCountRanges.of( nOperands );
        }
    }


    @Override
    public String getAllowedSignatures( Operator op, String opName ) {
        final String typeName = getTypeName();
        return CoreUtil.getAliasedSignature(
                op,
                opName,
                nOperands == -1
                        ? ImmutableList.of( typeName, typeName, "..." )
                        : Collections.nCopies( nOperands, typeName ) );
    }


    /**
     * Override to change the behavior of {@link PolyOperandTypeChecker#getAllowedSignatures(Operator, String)}.
     */
    protected String getTypeName() {
        return "EQUIVALENT_TYPE";
    }


    @Override
    public boolean checkSingleOperandType( CallBinding callBinding, Node operand, int iFormalOperand, boolean throwOnFailure ) {
        throw new UnsupportedOperationException(); // TODO:
    }

}

