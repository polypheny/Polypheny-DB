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


import java.util.List;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.validate.SqlValidator;
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
        List<Node> operands = call.getOperandList();
        ParserPos pos = call.getPos();

        checkOperandCount( validator, getOperandTypeChecker(), call );
        assert operands.size() == 2;

        SqlNodeList whenList = new SqlNodeList( pos );
        SqlNodeList thenList = new SqlNodeList( pos );
        whenList.add( operands.get( 1 ) );
        thenList.add( SqlLiteral.createNull( ParserPos.ZERO ) );
        return SqlCase.createSwitched( pos, (SqlNode) operands.get( 0 ), whenList, thenList, (SqlNode) Node.clone( operands.get( 0 ) ) );
    }

}

