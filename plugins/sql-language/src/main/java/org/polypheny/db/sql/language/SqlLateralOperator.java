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

package org.polypheny.db.sql.language;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * An operator describing a LATERAL specification.
 */
public class SqlLateralOperator extends SqlSpecialOperator {


    public SqlLateralOperator( Kind kind ) {
        super( kind.name(), kind, 200, true, ReturnTypes.ARG0, null, OperandTypes.ANY );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        if ( call.operandCount() == 1 && call.getOperandList().get( 0 ).getKind() == Kind.COLLECTION_TABLE ) {
            // do not create ( ) around the following TABLE clause
            writer.keyword( getName() );
            ((SqlNode) call.operand( 0 )).unparse( writer, 0, 0 );
        } else {
            SqlUtil.unparseFunctionSyntax( this, writer, call );
        }
    }

}

