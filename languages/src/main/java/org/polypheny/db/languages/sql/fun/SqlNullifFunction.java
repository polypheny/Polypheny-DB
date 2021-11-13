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

package org.polypheny.db.languages.sql.fun;


import java.util.List;
import org.polypheny.db.core.Node;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlFunction;
import org.polypheny.db.core.FunctionCategory;
import org.polypheny.db.core.Kind;
import org.polypheny.db.languages.sql.SqlLiteral;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlNodeList;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * The <code>NULLIF</code> function.
 */
public class SqlNullifFunction extends SqlFunction {


    public SqlNullifFunction() {
        // NOTE jvs 26-July-2006:  We fill in the type strategies here, but normally they are not used because the validator invokes rewriteCall to convert NULLIF into CASE early.
        // However, validator rewrite can optionally be disabled, in which case these strategies are used.
        super(
                "NULLIF",
                Kind.NULLIF,
                ReturnTypes.ARG0_FORCE_NULLABLE,
                null,
                OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED,
                FunctionCategory.SYSTEM );
    }


    // override SqlOperator
    @Override
    public SqlNode rewriteCall( SqlValidator validator, SqlCall call ) {
        List<SqlNode> operands = call.getOperandList();
        ParserPos pos = call.getPos();

        checkOperandCount( validator, getOperandTypeChecker(), call );
        assert operands.size() == 2;

        SqlNodeList whenList = new SqlNodeList( pos );
        SqlNodeList thenList = new SqlNodeList( pos );
        whenList.add( operands.get( 1 ) );
        thenList.add( SqlLiteral.createNull( ParserPos.ZERO ) );
        return SqlCase.createSwitched( pos, operands.get( 0 ), whenList, thenList, Node.clone( operands.get( 0 ) ) );
    }
}

