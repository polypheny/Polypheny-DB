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

public class SqlStPointFunction extends SqlFunction {


    /**
     *
     * @param isMake Only normal ST_Point allows for SRID, ST_MakePoint not
     */
    public SqlStPointFunction( String name, boolean isMake ) {
        super( name, Kind.GEO, ReturnTypes.GEOMETRY, null, isMake ? ST_ARG_CHECKER : ST_ARG_SRID_CHECKER, FunctionCategory.GEOMETRY );
    }

    static final PolyOperandTypeChecker ST_ARG_SRID_CHECKER = new PolyOperandTypeChecker() {

        @Override
        public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
            int nOperandsActual = callBinding.getOperandCount();

            // Check, if present, whether second argument is a number
            if ( nOperandsActual == 2 || nOperandsActual == 3 ) {
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
            }else {
                if ( throwOnFailure ) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }

            return true;
        }


        @Override
        public String getAllowedSignatures( Operator op, String opName ) {
            return """
                    'ST_Point(<NUMERIC>, <NUMERIC>)'
                    'ST_Point(<NUMERIC>, <NUMERIC>, <NUMERIC>)'
                    """;
        }


        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }


        @Override
        public OperandCountRange getOperandCountRange() {
            return PolyOperandCountRanges.between( 2, 3 );
        }


        @Override
        public boolean isOptional( int i ) {
            return i == 2;
        }
    };

    static final PolyOperandTypeChecker ST_ARG_CHECKER = new PolyOperandTypeChecker() {

        @Override
        public boolean checkOperandTypes( CallBinding callBinding, boolean throwOnFailure ) {
            int nOperandsActual = callBinding.getOperandCount();

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
            }else {
                if ( throwOnFailure ) {
                    throw callBinding.newValidationSignatureError();
                } else {
                    return false;
                }
            }

            return true;
        }


        @Override
        public String getAllowedSignatures( Operator op, String opName ) {
            return """
                    'ST_MakePoint(<DOUBLE>, <DOUBLE>)'
                    'ST_MakePoint(<INTEGER>, <INTEGER>)'
                    """;
        }


        @Override
        public Consistency getConsistency() {
            return Consistency.NONE;
        }


        @Override
        public OperandCountRange getOperandCountRange() {
            return PolyOperandCountRanges.of( 2 );
        }


        @Override
        public boolean isOptional( int i ) {
            return false;
        }
    };
}
