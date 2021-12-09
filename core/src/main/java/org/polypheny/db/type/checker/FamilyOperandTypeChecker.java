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


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.CoreUtil;


/**
 * Operand type-checking strategy which checks operands for inclusion in type families.
 */
public class FamilyOperandTypeChecker implements PolySingleOperandTypeChecker {

    protected final ImmutableList<PolyTypeFamily> families;
    protected final Predicate<Integer> optional;


    /**
     * Package private. Create using {@link OperandTypes#family}.
     */
    FamilyOperandTypeChecker( List<PolyTypeFamily> families, Predicate<Integer> optional ) {
        this.families = ImmutableList.copyOf( families );
        this.optional = optional;
    }


    @Override
    public boolean isOptional( int i ) {
        return optional.test( i );
    }


    @Override
    public boolean checkSingleOperandType( CallBinding callBinding, Node node, int iFormalOperand, boolean throwOnFailure ) {
        PolyTypeFamily family = families.get( iFormalOperand );
        if ( family == PolyTypeFamily.ANY ) {
            // no need to check
            return true;
        }
        if ( CoreUtil.isNullLiteral( node, false ) ) {
            if ( throwOnFailure ) {
                throw callBinding.getValidator().newValidationError( node, RESOURCE.nullIllegal() );
            } else {
                return false;
            }
        }
        AlgDataType type = callBinding.getValidator().deriveType( callBinding.getScope(), node );
        PolyType typeName = type.getPolyType();

        // Pass type checking for operators if it's of type 'ANY'.
        if ( typeName.getFamily() == PolyTypeFamily.ANY ) {
            return true;
        }

        if ( !family.getTypeNames().contains( typeName ) ) {
            if ( throwOnFailure ) {
                throw callBinding.newValidationSignatureError();
            }
            return false;
        }
        return true;
    }


    @Override
    public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
        if ( families.size() != callBinding.getOperandCount() ) {
            // assume this is an inapplicable sub-rule of a composite rule;
            // don't throw
            return false;
        }

        for ( Ord<? extends Node> op : Ord.zip( callBinding.operands() ) ) {
            if ( !checkSingleOperandType( callBinding, op.e, op.i, throwOnFailure ) ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        final int max = families.size();
        int min = max;
        while ( min > 0 && optional.test( min - 1 ) ) {
            --min;
        }
        return PolyOperandCountRanges.between( min, max );
    }


    @Override
    public String getAllowedSignatures( Operator op, String opName ) {
        return CoreUtil.getAliasedSignature( op, opName, families );
    }


    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }

}

