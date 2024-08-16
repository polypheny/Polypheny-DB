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

package org.polypheny.db.sql.language.fun;

import static org.polypheny.db.util.Static.RESOURCE;

import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.nodes.CallBinding;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.CoreUtil;


/**
 * Definition of the "ST_Buffer" spatial function.
 * The function has 2 required parameters:
 * geometry {@link org.polypheny.db.type.entity.spatial.PolyGeometry}
 * distance {@link org.polypheny.db.type.entity.numerical.PolyFloat}.
 * and 2 optional:
 * quadrantSegments {@link org.polypheny.db.type.entity.category.PolyNumber}
 * endCapStyle      {@link org.polypheny.db.type.entity.PolyString}
 */
public class SqlStBuffer extends SqlFunction {

    private static final PolyOperandTypeChecker ST_BUFFER_ARG_CHECKER = new PolyOperandTypeChecker() {

        @Override
        public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
            int nOperandsActual = callBinding.getOperandCount();

            // Make sure the first argument is not null
            if ( CoreUtil.isNullLiteral( callBinding.operand( 0 ), false ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( 0 ), RESOURCE.nullIllegal() );
                } else {
                    return false;
                }
            }

            // Make sure the first argument is a geometry
            if ( !PolyTypeUtil.inGeoFamily( callBinding.getOperandType( 0 ) ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( 0 ), RESOURCE.expectedGeometry() );
                } else {
                    return false;
                }
            }

            // Check whether second argument is not null
            if ( CoreUtil.isNullLiteral( callBinding.operand( 1 ), false ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( 1 ), RESOURCE.nullIllegal() );
                } else {
                    return false;
                }
            }
            // Check whether second argument is a float
            if ( (!PolyTypeUtil.isNumeric( callBinding.getOperandType( 1 ) )) ) {
                if ( throwOnFailure ) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }

            // Check, if present, whether third argument is a number
            if ( nOperandsActual == 3 ) {
                // Make sure the argument is not null
                if ( CoreUtil.isNullLiteral( callBinding.operand( 2 ), false ) ) {
                    if ( throwOnFailure ) {
                        throw callBinding.getValidator().newValidationError( callBinding.operand( 2 ), RESOURCE.nullIllegal() );
                    } else {
                        return false;
                    }
                }
                // Make sure the argument is a number
                if ( (!PolyTypeUtil.isNumeric( callBinding.getOperandType( 2 ) )) ) {
                    if ( throwOnFailure ) {
                        throw callBinding.newValidationSignatureError();
                    } else {
                        return false;
                    }
                }
            }

            // Check, if present, whether fourth argument is a string
            if ( nOperandsActual == 4 ) {
                // Make sure the argument is not null
                if ( CoreUtil.isNullLiteral( callBinding.operand( 3 ), false ) ) {
                    if ( throwOnFailure ) {
                        throw callBinding.getValidator().newValidationError( callBinding.operand( 3 ), RESOURCE.nullIllegal() );
                    } else {
                        return false;
                    }
                }
                // Make sure the argument is a string
                if ( !PolyTypeUtil.inCharFamily( callBinding.getOperandType( 3 ) ) ) {
                    if ( throwOnFailure ) {
                        throw callBinding.getValidator().newValidationError( callBinding.operand( 3 ), RESOURCE.expectedCharacter() );
                    } else {
                        return false;
                    }
                }
            }

            return true;

        }


        @Override
        public String getAllowedSignatures( Operator op, String opName ) {
            return "'ST_Buffer(<GEOMETRY>, <FLOAT>)'" + "\n" + "'ST_Buffer(<GEOMETRY>, <FLOAT>, <INTEGER>)'" + "\n" + "'ST_Buffer(<GEOMETRY>, <FLOAT>, <INTEGER>, <STRING>)'";
        }


        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }


        @Override
        public OperandCountRange getOperandCountRange() {
            return PolyOperandCountRanges.between( 2, 4 );
        }


        @Override
        public boolean isOptional( int i ) {
            return i == 3 || i == 4;
        }
    };


    /**
     * Creates the SqlStBuffer.
     */
    public SqlStBuffer() {
        super( "ST_BUFFER", Kind.GEO, ReturnTypes.GEOMETRY, null, ST_BUFFER_ARG_CHECKER, FunctionCategory.GEOMETRY );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        return switch ( operandsCount ) {
            case 2 -> "{0}({1}, {2})";
            case 3 -> "{0}({1}, {2}, {3})";
            case 4 -> "{0}({1}, {2}, {3}, {4})";
            default -> throw new AssertionError();
        };
    }

}
