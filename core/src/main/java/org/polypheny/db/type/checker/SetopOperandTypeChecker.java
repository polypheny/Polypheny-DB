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


import java.util.AbstractList;
import java.util.List;
import org.polypheny.db.core.CallBinding;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Node;
import org.polypheny.db.core.Operator;
import org.polypheny.db.core.Validator;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlSelect;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
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
        final RelDataType[] argTypes = new RelDataType[callBinding.getOperandCount()];
        int colCount = -1;
        final Validator validator = callBinding.getValidator();
        for ( int i = 0; i < argTypes.length; i++ ) {
            final RelDataType argType = argTypes[i] = callBinding.getOperandType( i );
            if ( !argType.isStruct() ) {
                if ( throwOnFailure ) {
                    throw new AssertionError( "setop arg must be a struct" );
                } else {
                    return false;
                }
            }

            // Each operand must have the same number of columns.
            final List<RelDataTypeField> fields = argType.getFieldList();
            if ( i == 0 ) {
                colCount = fields.size();
                continue;
            }

            if ( fields.size() != colCount ) {
                if ( throwOnFailure ) {
                    Node node = callBinding.operand( i );
                    if ( node.getKind() == Kind.SELECT ) {
                        node = ((SqlSelect) node).getSelectList();
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
            final RelDataType type =
                    callBinding.getTypeFactory().leastRestrictive(
                            new AbstractList<RelDataType>() {
                                @Override
                                public RelDataType get( int index ) {
                                    return argTypes[index].getFieldList().get( i2 ).getType();
                                }


                                @Override
                                public int size() {
                                    return argTypes.length;
                                }
                            } );
            if ( type == null ) {
                if ( throwOnFailure ) {
                    Node field = SqlUtil.getSelectListItem( callBinding.operand( 0 ), i );
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

