/*
 * Copyright 2019-2020 The Polypheny Project
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


import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.type.PolyOperandCountRanges;
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
                SqlFunctionCategory.USER_DEFINED_FUNCTION );
    }

    @Override
    public String getSignatureTemplate( int operandsCount ) {
        if ( operandsCount == 2 ) {
            //META(col, 'height')
            return "{0}({1}, {2})";
        } else if ( operandsCount == 1 ) {
            return "{0}({1})";
        }
        throw new AssertionError();
    }

    @Override
    public SqlIdentifier getSqlIdentifier() {
        return new SqlIdentifier( "meta", SqlParserPos.ZERO );
    }

    private static final PolyOperandTypeChecker META_ARG_CHECKER = new PolyOperandTypeChecker() {

        @Override
        public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
            //todo check
            return true;
        }

        @Override
        public SqlOperandCountRange getOperandCountRange() {
            //todo change to .of(2)
            return PolyOperandCountRanges.between( 1, 2 );
        }

        @Override
        public String getAllowedSignatures( SqlOperator op, String opName ) {
            return "'META(<MULTIMEDIA>, <STRING>)'\n'META(<MULTIMEDIA>)'";
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
