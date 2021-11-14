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


import org.polypheny.db.core.Kind;
import org.polypheny.db.languages.sql.SqlCall;
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
    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        SqlSelect subSelect = call.operand( 0 );
        validator.declareCursor( subSelect, scope );
        subSelect.validateExpr( validator, scope );
        return super.deriveType( validator, scope, call );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.keyword( "CURSOR" );
        final SqlWriter.Frame frame = writer.startList( "(", ")" );
        assert call.operandCount() == 1;
        call.operand( 0 ).unparse( writer, leftPrec, rightPrec );
        writer.endList( frame );
    }


    @Override
    public boolean argumentMustBeScalar( int ordinal ) {
        return false;
    }
}

