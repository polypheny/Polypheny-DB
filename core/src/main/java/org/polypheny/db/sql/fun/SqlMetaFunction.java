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

package org.polypheny.db.sql.fun;

import static org.polypheny.db.util.Static.RESOURCE;

import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.ReturnTypes;


public class SqlMetaFunction extends SqlFunction {

    public SqlMetaFunction() {
        super( "META",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.VARCHAR_2000,
                null,
                //OperandTypes.family( PolyTypeFamily.BINARY ),
                META_ARG_CHECKER,
                SqlFunctionCategory.MULTIMEDIA );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        if ( operandsCount == 3 ) {
            return "{0}({1}, {2}, {3})";
        } else if ( operandsCount == 2 ) {
            return "{0}({1}, {2})";
        } else if ( operandsCount == 1 ) {
            return "{0}({1})";
        }
        throw new AssertionError();
    }


    private static final PolyOperandTypeChecker META_ARG_CHECKER = new PolyOperandTypeChecker() {

        @Override
        public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
            if ( callBinding.getOperandType( 0 ).getPolyType().getFamily() != PolyTypeFamily.MULTIMEDIA ) {
                throw callBinding.getValidator().newValidationError( callBinding.operand( 0 ), RESOURCE.expectedMultimedia() );
            }
            for ( int i = 1; i < callBinding.getOperandCount(); i++ ) {
                if ( !PolyTypeUtil.inCharFamily( callBinding.getOperandType( i ) ) ) {
                    throw callBinding.getValidator().newValidationError( callBinding.operand( i ), RESOURCE.expectedCharacter() );
                }
            }
            return true;
        }


        @Override
        public SqlOperandCountRange getOperandCountRange() {
            return PolyOperandCountRanges.between( 1, 3 );
        }


        @Override
        public String getAllowedSignatures( SqlOperator op, String opName ) {
            return "'META(<MULTIMEDIA>)'\n'META(<MULTIMEDIA>, <STRING>)'\n'META(<MULTIMEDIA>, <STRING>, <STRING>)'";
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
