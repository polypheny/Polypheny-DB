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

package org.polypheny.db.languages.sql;


import java.util.Collections;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.core.StdOperatorRegistry;
import org.polypheny.db.languages.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.languages.sql.validate.SqlValidator;


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
                (SqlNode) StdOperatorRegistry.get( "VALUES" ).createCall(
                        ParserPos.ZERO,
                        SqlStdOperatorTable.ROW.createCall(
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

