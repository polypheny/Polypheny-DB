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


import java.util.AbstractList;
import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.Select;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Static;


/**
 * Parameter type-checking strategy for a set operator (UNION, INTERSECT, EXCEPT).
 * <p>
 * Both arguments must be records with the same number of fields, and the fields must be union-compatible.
 */
public class SetopOperandTypeChecker implements PolyOperandTypeChecker {

    @Override
    public boolean isOptional( int i ) {
        return false;
    }


    @Override
    public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
        assert callBinding.getOperandCount() == 2 : "setops are binary (for now)";
        final AlgDataType[] argTypes = new AlgDataType[callBinding.getOperandCount()];
        int colCount = -1;
        final Validator validator = callBinding.getValidator();
        for ( int i = 0; i < argTypes.length; i++ ) {
            final AlgDataType argType = argTypes[i] = callBinding.getOperandType( i );
            if ( !argType.isStruct() ) {
                if ( throwOnFailure ) {
                    throw new AssertionError( "setop arg must be a struct" );
                } else {
                    return false;
                }
            }

            // Each operand must have the same number of columns.
            final List<AlgDataTypeField> fields = argType.getFields();
            if ( i == 0 ) {
                colCount = fields.size();
                continue;
            }

            if ( fields.size() != colCount ) {
                if ( throwOnFailure ) {
                    Node node = callBinding.operand( i );
                    if ( node.getKind() == Kind.SELECT ) {
                        node = ((Select) node).getSelectList();
                    }
                    throw validator.newValidationError( node, Static.RESOURCE.columnCountMismatchInSetop( callBinding.getOperator().getName() ) );
                } else {
                    return false;
                }
            }
        }

        // The columns must be pairwise union compatible. For each column ordinal, form a 'slice' containing the types of the ordinal'th column j.
        for ( int i = 0; i < colCount; i++ ) {
            final int i2 = i;
            final AlgDataType type =
                    callBinding.getTypeFactory().leastRestrictive(
                            new AbstractList<AlgDataType>() {
                                @Override
                                public AlgDataType get( int index ) {
                                    return argTypes[index].getFields().get( i2 ).getType();
                                }


                                @Override
                                public int size() {
                                    return argTypes.length;
                                }
                            } );
            if ( type == null ) {
                if ( throwOnFailure ) {
                    Node field = CoreUtil.getSelectListItem( callBinding.operand( 0 ), i );
                    throw validator.newValidationError(
                            field,
                            Static.RESOURCE.columnTypeMismatchInSetop(
                                    i + 1, // 1-based
                                    callBinding.getOperator().getName() ) );
                } else {
                    return false;
                }
            }
        }

        return true;
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.of( 2 );
    }


    @Override
    public String getAllowedSignatures( Operator op, String opName ) {
        return "{0} " + opName + " {1}";
    }


    @Override
    public Consistency getConsistency() {
        return Consistency.NONE;
    }

}

