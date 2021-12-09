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


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Static;
import org.polypheny.db.util.Util;


/**
 * Parameter type-checking strategy type must be a literal (whether null is allowed is determined by the constructor).
 * <code>CAST(NULL as ...)</code> is considered to be a NULL literal but not <code>CAST(CAST(NULL as ...) AS ...)</code>
 */
public class LiteralOperandTypeChecker implements PolySingleOperandTypeChecker {

    private boolean allowNull;


    public LiteralOperandTypeChecker( boolean allowNull ) {
        this.allowNull = allowNull;
    }


    @Override
    public boolean isOptional( int i ) {
        return false;
    }


    @Override
    public boolean checkSingleOperandType( CallBinding callBinding, Node node, int iFormalOperand, boolean throwOnFailure ) {
        Util.discard( iFormalOperand );

        if ( CoreUtil.isNullLiteral( node, true ) ) {
            if ( allowNull ) {
                return true;
            }
            if ( throwOnFailure ) {
                throw callBinding.newError( Static.RESOURCE.argumentMustNotBeNull( callBinding.getOperator().getName() ) );
            }
            return false;
        }
        if ( node.getKind() != Kind.LITERAL && node.getKind() != Kind.LITERAL_CHAIN ) {
            if ( throwOnFailure ) {
                throw callBinding.newError( Static.RESOURCE.argumentMustBeLiteral( callBinding.getOperator().getName() ) );
            }
            return false;
        }

        return true;
    }


    @Override
    public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
        return checkSingleOperandType(
                callBinding,
                callBinding.operand( 0 ),
                0,
                throwOnFailure );
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.of( 1 );
    }


    @Override
    public String getAllowedSignatures( Operator op, String opName ) {
        return "<LITERAL>";
    }


    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }

}

