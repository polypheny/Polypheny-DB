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


import org.polypheny.db.core.nodes.Call;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.validate.Validator;
import org.polypheny.db.core.validate.ValidatorScope;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlSelect;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.languages.sql.validate.SqlValidatorScope;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * SqlCursorConstructor defines the non-standard CURSOR(&lt;query&gt;) constructor.
 */
public class SqlCursorConstructor extends SqlSpecialOperator {


    public SqlCursorConstructor() {
        super(
                "CURSOR",
                Kind.CURSOR, MDX_PRECEDENCE,
                false,
                ReturnTypes.CURSOR,
                null,
                OperandTypes.ANY );
    }


    @Override
    public RelDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        SqlSelect subSelect = call.operand( 0 );
        ((SqlValidator) validator).declareCursor( subSelect, (SqlValidatorScope) scope );
        subSelect.validateExpr( (SqlValidator) validator, (SqlValidatorScope) scope );
        return super.deriveType( validator, scope, call );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.keyword( "CURSOR" );
        final SqlWriter.Frame frame = writer.startList( "(", ")" );
        assert call.operandCount() == 1;
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, rightPrec );
        writer.endList( frame );
    }


    @Override
    public boolean argumentMustBeScalar( int ordinal ) {
        return false;
    }

}

