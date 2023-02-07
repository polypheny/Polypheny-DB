/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.sql.language.fun;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Util;


public class SqlDistanceFunction extends SqlFunction {

    private static final PolyOperandTypeChecker OTC_CUSTOM = OperandTypes.family( PolyTypeFamily.ANY, PolyTypeFamily.ANY, PolyTypeFamily.ANY );


    public SqlDistanceFunction() {
        super(
                "DISTANCE",
                Kind.DISTANCE,
                ReturnTypes.DOUBLE,
                null,
                DISTANCE_ARG_CHECKER,
                FunctionCategory.DISTANCE );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        switch ( operandsCount ) {
            case 3:
                return "{0}({1}, {2}, {3})";
            case 4:
                return "{0}({1}, {2}, {3}, {4})";
            default:
                throw new AssertionError();
        }
    }


    private static PolyOperandTypeChecker DISTANCE_ARG_CHECKER = new PolyOperandTypeChecker() {
        @Override
        public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {

            int nOperandsActual = callBinding.getOperandCount();
            AlgDataType[] types = new AlgDataType[nOperandsActual];
            final List<Integer> operandList = Util.range( 0, nOperandsActual );
            for ( int i : operandList ) {
                types[i] = callBinding.getOperandType( i );
            }

            // Make sure the first argument is an array of numeric values
            if ( !PolyTypeUtil.isArray( callBinding.getOperandType( 0 ) )
                    || !PolyTypeUtil.isNumeric( callBinding.getOperandType( 0 ).getComponentType() ) ) {
                if ( throwOnFailure ) {
                    // TODO js: better throws?
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }

            // TODO js: maybe check whether the first argument is a reference to a column?

            // Make sure the second argument is not null
            if ( CoreUtil.isNullLiteral( callBinding.operand( 1 ), false ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( 1 ), RESOURCE.nullIllegal() );
                } else {
                    return false;
                }
            }

            // Make sure the second argument is an array
            if ( !PolyTypeUtil.isArray( callBinding.getOperandType( 1 ) )
                    || !PolyTypeUtil.isNumeric( callBinding.getOperandType( 1 ).getComponentType() ) ) {
                if ( throwOnFailure ) {
                    // TODO js: better throws?
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }

            // Check whether third argument is a string
            // TODO js: implement string check
            if ( CoreUtil.isNullLiteral( callBinding.operand( 2 ), false ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( 2 ), RESOURCE.nullIllegal() );
                } else {
                    return false;
                }
            }

            if ( !PolyTypeUtil.inCharFamily( callBinding.getOperandType( 2 ) ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( 2 ), RESOURCE.expectedCharacter() );
//                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }

            // Check, if present, whether fourth argument is an array
            if ( nOperandsActual == 4 ) {
                if ( CoreUtil.isNullLiteral( callBinding.operand( 3 ), false ) ) {
                    if ( throwOnFailure ) {
                        throw callBinding.getValidator().newValidationError( callBinding.operand( 3 ), RESOURCE.nullIllegal() );
                    } else {
                        return false;
                    }
                }

                if ( (!PolyTypeUtil.isArray( callBinding.getOperandType( 3 ) ) || !PolyTypeUtil.isNumeric( callBinding.getOperandType( 3 ).getComponentType() )) ) {
                    if ( throwOnFailure ) {
                        throw callBinding.newValidationSignatureError();
                    } else {
                        return false;
                    }
                }
            }

            return true;

            /*if ( !throwOnFailure ) {
                return false;
            }

            throw callBinding.newValidationSignatureError();*/
        }


        @Override
        public OperandCountRange getOperandCountRange() {
            return PolyOperandCountRanges.between( 3, 4 );
        }


        @Override
        public String getAllowedSignatures( Operator op, String opName ) {
            return "'DISTANCE(<ARRAY>, <ARRAY>, <STRING>)'" + "\n" +
                    "'DISTANCE(<ARRAY>, <ARRAY>, <STRING>, <ARRAY>)'";
        }


        @Override
        public Consistency getConsistency() {
            // TODO js: double check
            return Consistency.NONE;
        }


        @Override
        public boolean isOptional( int i ) {
            // TODO js: check whether i is 0 or 1 based.
            return i == 3;
        }
    };

}
