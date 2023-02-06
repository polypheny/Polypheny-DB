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


import java.util.Collections;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.sql.language.validate.SqlValidator;


/**
 * SqlProcedureCallOperator represents the CALL statement. It takes a single operand which is the real SqlCall.
 */
public class SqlProcedureCallOperator extends SqlPrefixOperator {


    public SqlProcedureCallOperator() {
        super( "CALL", Kind.PROCEDURE_CALL, 0, null, null, null );
    }


    // override SqlOperator
    @Override
    public SqlNode rewriteCall( SqlValidator validator, SqlCall call ) {
        // For now, rewrite "CALL f(x)" to "SELECT f(x) FROM VALUES(0)"
        // TODO jvs 18-Jan-2005:  rewrite to SELECT * FROM TABLE f(x) once we support function calls as tables
        return new SqlSelect(
                ParserPos.ZERO,
                null,
                new SqlNodeList(
                        Collections.singletonList( call.operand( 0 ) ),
                        ParserPos.ZERO ),
                (SqlNode) OperatorRegistry.get( OperatorName.VALUES ).createCall(
                        ParserPos.ZERO,
                        OperatorRegistry.get( OperatorName.ROW ).createCall(
                                ParserPos.ZERO,
                                SqlLiteral.createExactNumeric( "0", ParserPos.ZERO ) ) ),
                null,
                null,
                null,
                null,
                null,
                null,
                null );
    }

}

