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
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.type.PolyTypeTransforms;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Util;


/**
 * The <code>COALESCE</code> function.
 */
public class SqlCoalesceFunction extends SqlFunction {


    public SqlCoalesceFunction() {
        // NOTE jvs 26-July-2006:  We fill in the type strategies here, but normally they are not used because the validator invokes rewriteCall to convert
        // COALESCE into CASE early.  However, validator rewrite can optionally be disabled, in which case these strategies are used.
        super(
                "COALESCE",
                Kind.COALESCE,
                ReturnTypes.cascade( ReturnTypes.LEAST_RESTRICTIVE, PolyTypeTransforms.LEAST_NULLABLE ),
                null,
                OperandTypes.SAME_VARIADIC,
                FunctionCategory.SYSTEM );
    }


    // override SqlOperator
    @Override
    public SqlNode rewriteCall( SqlValidator validator, SqlCall call ) {
        validateQuantifier( validator, call ); // check DISTINCT/ALL

        List<SqlNode> operands = call.getSqlOperandList();

        if ( operands.size() == 1 ) {
            // No CASE needed
            return operands.get( 0 );
        }

        ParserPos pos = call.getPos();

        SqlNodeList whenList = new SqlNodeList( pos );
        SqlNodeList thenList = new SqlNodeList( pos );

        // todo: optimize when know operand is not null.

        for ( SqlNode operand : Util.skipLast( operands ) ) {
            whenList.add( OperatorRegistry.get( OperatorName.IS_NOT_NULL ).createCall( pos, operand ) );
            thenList.add( Node.clone( operand ) );
        }
        SqlNode elseExpr = Util.last( operands );
        assert call.getFunctionQuantifier() == null;
        return SqlCase.createSwitched( pos, null, whenList, thenList, elseExpr );
    }

}

