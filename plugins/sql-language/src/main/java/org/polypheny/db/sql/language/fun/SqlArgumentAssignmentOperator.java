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


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.sql.language.SqlAsOperator;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * Operator that assigns an argument to a function call to a particular named parameter.
 *
 * Not an expression; just a holder to represent syntax until the validator has chance to resolve arguments.
 *
 * Sub-class of {@link SqlAsOperator} ("AS") out of convenience; to be consistent with AS, we reverse the arguments.
 */
public class SqlArgumentAssignmentOperator extends SqlAsOperator {

    public SqlArgumentAssignmentOperator() {
        super( "=>", Kind.ARGUMENT_ASSIGNMENT, 20, true, ReturnTypes.ARG0, InferTypes.RETURN_TYPE, OperandTypes.ANY_ANY );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        // Arguments are held in reverse order to be consistent with base class (AS).
        ((SqlNode) call.operand( 1 )).unparse( writer, leftPrec, getLeftPrec() );
        writer.keyword( getName() );
        ((SqlNode) call.operand( 0 )).unparse( writer, getRightPrec(), rightPrec );
    }

}

