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
import org.polypheny.db.core.CallBinding;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.type.MultisetPolyType;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.util.Static;


/**
 * Parameter type-checking strategy types must be [nullable] Multiset, [nullable] Multiset and the two types must have the
 * same element type
 *
 * @see MultisetPolyType#getComponentType
 */
public class MultisetOperandTypeChecker implements PolyOperandTypeChecker {


    @Override
    public boolean isOptional( int i ) {
        return false;
    }


    @Override
    public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
        final SqlNode op0 = callBinding.operand( 0 );
        if ( !OperandTypes.MULTISET.checkSingleOperandType( callBinding, op0, 0, throwOnFailure ) ) {
            return false;
        }

        final SqlNode op1 = callBinding.operand( 1 );
        if ( !OperandTypes.MULTISET.checkSingleOperandType( callBinding, op1, 0, throwOnFailure ) ) {
            return false;
        }

        // TODO: this won't work if element types are of ROW types and there is a mismatch.
        RelDataType biggest =
                callBinding.getTypeFactory().leastRestrictive(
                        ImmutableList.of(
                                callBinding.getValidator()
                                        .deriveType( callBinding.getScope(), op0 )
                                        .getComponentType(),
                                callBinding.getValidator()
                                        .deriveType( callBinding.getScope(), op1 )
                                        .getComponentType() ) );
        if ( null == biggest ) {
            if ( throwOnFailure ) {
                throw callBinding.newError(
                        Static.RESOURCE.typeNotComparable(
                                op0.getPos().toString(),
                                op1.getPos().toString() ) );
            }

            return false;
        }
        return true;
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.of( 2 );
    }


    @Override
    public String getAllowedSignatures( Operator op, String opName ) {
        return "<MULTISET> " + opName + " <MULTISET>";
    }


    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }

}

