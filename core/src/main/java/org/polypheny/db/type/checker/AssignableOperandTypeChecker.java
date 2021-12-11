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
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.Pair;


/**
 * AssignableOperandTypeChecker implements {@link PolyOperandTypeChecker} by verifying that the type of each argument is
 * assignable to a predefined set of parameter types (under the SQL definition of "assignable").
 */
public class AssignableOperandTypeChecker implements PolyOperandTypeChecker {

    private final List<AlgDataType> paramTypes;
    private final ImmutableList<String> paramNames;


    /**
     * Instantiates this strategy with a specific set of parameter types.
     *
     * @param paramTypes parameter types for operands; index in this array corresponds to operand number
     * @param paramNames parameter names, or null
     */
    public AssignableOperandTypeChecker( List<AlgDataType> paramTypes, List<String> paramNames ) {
        this.paramTypes = ImmutableList.copyOf( paramTypes );
        this.paramNames = paramNames == null ? null : ImmutableList.copyOf( paramNames );
    }


    @Override
    public boolean isOptional( int i ) {
        return false;
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.of( paramTypes.size() );
    }


    @Override
    public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
        // Do not use callBinding.operands(). We have not resolved to a function yet, therefore we do not know the ordered
        // parameter names.
        final List<Node> operands = callBinding.getCall().getOperandList();
        for ( Pair<AlgDataType, Node> pair : Pair.zip( paramTypes, operands ) ) {
            AlgDataType argType = callBinding.getValidator().deriveType( callBinding.getScope(), pair.right );
            if ( !PolyTypeUtil.canAssignFrom( pair.left, argType ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }
        }
        return true;
    }


    @Override
    public String getAllowedSignatures( Operator op, String opName ) {
        StringBuilder sb = new StringBuilder();
        sb.append( opName );
        sb.append( "(" );
        for ( Ord<AlgDataType> paramType : Ord.zip( paramTypes ) ) {
            if ( paramType.i > 0 ) {
                sb.append( ", " );
            }
            if ( paramNames != null ) {
                sb.append( paramNames.get( paramType.i ) ).append( " => " );
            }
            sb.append( "<" );
            sb.append( paramType.e.getFamily() );
            sb.append( ">" );
        }
        sb.append( ")" );
        return sb.toString();
    }


    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }

}

