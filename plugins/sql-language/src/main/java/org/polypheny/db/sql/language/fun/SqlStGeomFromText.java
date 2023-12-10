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
 * Definition of the "ST_GeomFromText" spatial function.
 * The function has a required parameter - WKT string representation
 * and an optional SRID integer.
 */
public class SqlStGeomFromText extends SqlFunction {

    static final PolyOperandTypeChecker ST_GEOMFROMTEXT_ARG_CHECKER = new PolyOperandTypeChecker() {

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

            // Make sure the first argument is a string
            if ( !PolyTypeUtil.inCharFamily( callBinding.getOperandType( 0 ) ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( 0 ), RESOURCE.expectedCharacter() );
                } else {
                    return false;
                }
            }

            // Check, if present, whether second argument is a number
            if ( nOperandsActual == 2 ) {
                if ( CoreUtil.isNullLiteral( callBinding.operand( 1 ), false ) ) {
                    if ( throwOnFailure ) {
                        throw callBinding.getValidator().newValidationError( callBinding.operand( 1 ), RESOURCE.nullIllegal() );
                    } else {
                        return false;
                    }
                }

                if ( (!PolyTypeUtil.isNumeric( callBinding.getOperandType( 1 ) )) ) {
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
            return "'ST_GeomFromText(<STRING>)'" + "\n" + "'ST_GeomFromText(<STRING>, <INTEGER>)'";
        }


        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }


        @Override
        public OperandCountRange getOperandCountRange() {
            return PolyOperandCountRanges.between( 1, 2 );
        }


        @Override
        public boolean isOptional( int i ) {
            return i == 1;
        }
    };


    /**
     * Creates the SqlStGeoFromText.
     */
    public SqlStGeomFromText() {
        super( "ST_GEOMFROMTEXT", Kind.GEO, ReturnTypes.GEOMETRY, null, ST_GEOMFROMTEXT_ARG_CHECKER, FunctionCategory.GEOMETRY );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        switch ( operandsCount ) {
            case 1:
                return "{0}({1})";
            case 2:
                return "{0}({1}, {2})";
            default:
                throw new AssertionError();
        }
    }

}
