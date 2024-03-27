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
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.util.Util;


/**
 * Allows multiple {@link PolySingleOperandTypeChecker} rules to be combined into one rule.
 */
public class CompositeSingleOperandTypeChecker extends CompositeOperandTypeChecker implements PolySingleOperandTypeChecker {

    /**
     * Package private. Use {@link OperandTypes#and}, {@link OperandTypes#or}.
     */
    CompositeSingleOperandTypeChecker(
            CompositeOperandTypeChecker.Composition composition,
            ImmutableList<? extends PolySingleOperandTypeChecker> allowedRules,
            String allowedSignatures ) {
        super( composition, allowedRules, allowedSignatures, null );
    }


    @SuppressWarnings("unchecked")
    @Override
    public ImmutableList<? extends PolySingleOperandTypeChecker> getRules() {
        return (ImmutableList<? extends PolySingleOperandTypeChecker>) allowedRules;
    }


    @Override
    public boolean checkSingleOperandType( CallBinding callBinding, Node node, int iFormalOperand, boolean throwOnFailure ) {
        assert !allowedRules.isEmpty();

        final ImmutableList<? extends PolySingleOperandTypeChecker> rules = getRules();
        if ( composition == Composition.SEQUENCE ) {
            return rules.get( iFormalOperand ).checkSingleOperandType( callBinding, node, 0, throwOnFailure );
        }

        int typeErrorCount = 0;

        boolean throwOnAndFailure = (composition == Composition.AND) && throwOnFailure;

        for ( PolySingleOperandTypeChecker rule : rules ) {
            if ( !rule.checkSingleOperandType( callBinding, node, iFormalOperand, throwOnAndFailure ) ) {
                typeErrorCount++;
            }
        }

        boolean ret = switch ( composition ) {
            case AND -> typeErrorCount == 0;
            case OR -> typeErrorCount < allowedRules.size();
            default ->
                // should never come here
                    throw Util.unexpected( composition );
        };

        if ( !ret && throwOnFailure ) {
            // In the case of a composite OR, we want to throw an error describing in more detail what the problem was,
            // hence doing the loop again.
            for ( PolySingleOperandTypeChecker rule : rules ) {
                rule.checkSingleOperandType( callBinding, node, iFormalOperand, true );
            }

            // If no exception thrown, just throw a generic validation signature error.
            throw callBinding.newValidationSignatureError();
        }

        return ret;
    }

}

