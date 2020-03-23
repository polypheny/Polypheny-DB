/*
 * Copyright 2019-2020 The Polypheny Project
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
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyTypeUtil;
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
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        return checkOperandTypesImpl( callBinding, throwOnFailure, callBinding );
    }


    protected List<Integer> getOperandList( int operandCount ) {
        return nOperands == -1
                ? Util.range( 0, operandCount )
                : Util.range( 0, nOperands );
    }


    protected boolean checkOperandTypesImpl( SqlOperatorBinding operatorBinding, boolean throwOnFailure, SqlCallBinding callBinding ) {
        int nOperandsActual = nOperands;
        if ( nOperandsActual == -1 ) {
            nOperandsActual = operatorBinding.getOperandCount();
        }
        assert !(throwOnFailure && (callBinding == null));
        RelDataType[] types = new RelDataType[nOperandsActual];
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
     * Similar functionality to {@link #checkOperandTypes(SqlCallBinding, boolean)}, but not part of the interface, and cannot throw an error.
     */
    public boolean checkOperandTypes( SqlOperatorBinding operatorBinding ) {
        return checkOperandTypesImpl( operatorBinding, false, null );
    }


    // implement SqlOperandTypeChecker
    @Override
    public SqlOperandCountRange getOperandCountRange() {
        if ( nOperands == -1 ) {
            return PolyOperandCountRanges.any();
        } else {
            return PolyOperandCountRanges.of( nOperands );
        }
    }


    @Override
    public String getAllowedSignatures( SqlOperator op, String opName ) {
        final String typeName = getTypeName();
        return SqlUtil.getAliasedSignature(
                op,
                opName,
                nOperands == -1
                        ? ImmutableList.of( typeName, typeName, "..." )
                        : Collections.nCopies( nOperands, typeName ) );
    }


    /**
     * Override to change the behavior of {@link #getAllowedSignatures(SqlOperator, String)}.
     */
    protected String getTypeName() {
        return "EQUIVALENT_TYPE";
    }


    @Override
    public boolean checkSingleOperandType( SqlCallBinding callBinding, SqlNode operand, int iFormalOperand, boolean throwOnFailure ) {
        throw new UnsupportedOperationException(); // TODO:
    }
}

