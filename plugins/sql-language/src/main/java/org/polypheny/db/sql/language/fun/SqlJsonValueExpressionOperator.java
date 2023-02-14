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
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.OperandTypes;


/**
 * The JSON value expression operator that indicates that the value expression should be parsed as JSON.
 */
public class SqlJsonValueExpressionOperator extends SqlSpecialOperator {

    private final boolean structured;


    public SqlJsonValueExpressionOperator( String name, boolean structured ) {
        super(
                name,
                Kind.JSON_VALUE_EXPRESSION,
                100,
                true,
                opBinding -> {
                    final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
                    return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true );
                },
                ( callBinding, returnType, operandTypes ) -> {
                    if ( ((SqlCallBinding) callBinding).isOperandNull( 0, false ) ) {
                        final AlgDataTypeFactory typeFactory = callBinding.getTypeFactory();
                        operandTypes[0] = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true );
                    }
                },
                structured ? OperandTypes.ANY : OperandTypes.STRING );
        this.structured = structured;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        ((SqlNode) call.operand( 0 )).unparse( writer, 0, 0 );
        if ( !structured ) {
            writer.keyword( "FORMAT JSON" );
        }
    }

}

