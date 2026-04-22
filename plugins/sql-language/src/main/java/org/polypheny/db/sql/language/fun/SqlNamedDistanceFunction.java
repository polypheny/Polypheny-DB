/*
 * Copyright 2019-2026 The Polypheny Project
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

import static org.polypheny.db.util.Static.RESOURCE;

/**
 * Represents an {@link SqlDistanceFunction} function that is not parameterized anymore.
 */
public class SqlNamedDistanceFunction extends SqlFunction {


    public SqlNamedDistanceFunction( String name, Kind kind, FunctionCategory functionCategory ) {
        super( name,
                kind,
                ReturnTypes.DOUBLE,
                null,
                TWO_NUMERIC_ARRAYS,
                functionCategory );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        if ( operandsCount == 3) return "{0}({1}, {2})";
        throw new AssertionError();
    }


    public static final PolyOperandTypeChecker TWO_NUMERIC_ARRAYS = new PolyOperandTypeChecker() {

        /**
         * This method is similar to {@link SqlDistanceFunction#getOperandTypeChecker()#checkOperandTypes(CallBinding, boolean)}.
         */
        @Override
        public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {

            // Make sure the first argument is not null
            if ( CoreUtil.isNullLiteral( callBinding.operand( 0 ), false ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( 0 ), RESOURCE.nullIllegal() );
                } else {
                    return false;
                }
            }
            // Make sure the first argument is an array of numeric values
            if ( !PolyTypeUtil.isArray( callBinding.getOperandType( 0 ) )
                    || !PolyTypeUtil.isNumeric( callBinding.getOperandType( 0 ).getComponentType() ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }
            // Make sure the second argument is not null
            if ( CoreUtil.isNullLiteral( callBinding.operand( 1 ), false ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( 1 ), RESOURCE.nullIllegal() );
                } else {
                    return false;
                }
            }
            // Make sure the second argument is an array of numeric values
            if ( !PolyTypeUtil.isArray( callBinding.getOperandType( 1 ) )
                    || !PolyTypeUtil.isNumeric( callBinding.getOperandType( 1 ).getComponentType() ) ) {
                if ( throwOnFailure ) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
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
            return "'" + opName + "(<NUMERIC ARRAY>, <NUMERIC ARRAY>)'";
        }


        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }


        @Override
        public boolean isOptional( int i ) {
            return false;
        }
    };

}
