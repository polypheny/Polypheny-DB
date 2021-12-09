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

package org.polypheny.db.type.checker;


import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Static;


/**
 * Parameter type-checking strategy where all operand types except last one must be the same.
 */
public class SameOperandTypeExceptLastOperandChecker extends SameOperandTypeChecker {

    protected final String lastOperandTypeName;


    public SameOperandTypeExceptLastOperandChecker( int nOperands, String lastOperandTypeName ) {
        super( nOperands );
        this.lastOperandTypeName = lastOperandTypeName;
    }


    @Override
    protected boolean checkOperandTypesImpl( OperatorBinding operatorBinding, boolean throwOnFailure, CallBinding callBinding ) {
        int nOperandsActual = nOperands;
        if ( nOperandsActual == -1 ) {
            nOperandsActual = operatorBinding.getOperandCount();
        }
        assert !(throwOnFailure && (callBinding == null));
        AlgDataType[] types = new AlgDataType[nOperandsActual];
        final List<Integer> operandList = getOperandList( operatorBinding.getOperandCount() );
        for ( int i : operandList ) {
            if ( operatorBinding.isOperandNull( i, false ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( i ), Static.RESOURCE.nullIllegal() );
                } else {
                    return false;
                }
            }
            types[i] = operatorBinding.getOperandType( i );
        }
        int prev = -1;
        for ( int i : operandList ) {
            if ( prev >= 0 && i != operandList.get( operandList.size() - 1 ) ) {
                if ( !PolyTypeUtil.isComparable( types[i], types[prev] ) ) {
                    if ( !throwOnFailure ) {
                        return false;
                    }

                    // REVIEW jvs: Why don't we use newValidationSignatureError() here?  It gives more specific diagnostics.
                    throw callBinding.newValidationError( Static.RESOURCE.needSameTypeParameter() );
                }
            }
            prev = i;
        }
        return true;
    }


    @Override
    public String getAllowedSignatures( Operator op, String opName ) {
        final String typeName = getTypeName();
        if ( nOperands == -1 ) {
            return CoreUtil.getAliasedSignature( op, opName, ImmutableList.of( typeName, typeName, "..." ) );
        } else {
            List<String> types = Collections.nCopies( nOperands - 1, typeName );
            types.add( lastOperandTypeName );
            return CoreUtil.getAliasedSignature( op, opName, types );
        }
    }

}

