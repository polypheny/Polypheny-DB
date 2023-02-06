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
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.PolyType;


/**
 * Operator that returns the current or next value of a sequence.
 */
public class SqlSequenceValueOperator extends SqlSpecialOperator {

    /**
     * Creates a SqlSequenceValueOperator.
     */
    public SqlSequenceValueOperator( Kind kind ) {
        super( kind.name(), kind, 100 );
        assert kind == Kind.NEXT_VALUE || kind == Kind.CURRENT_VALUE;
    }


    @Override
    public boolean isDeterministic() {
        return false;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.sep(
                kind == Kind.NEXT_VALUE
                        ? "NEXT VALUE FOR"
                        : "CURRENT VALUE FOR" );
        ((SqlNode) call.getOperandList().get( 0 )).unparse( writer, 0, 0 );
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        final AlgDataTypeFactory typeFactory = validator.getTypeFactory();
        return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.BIGINT ), false );
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        List<SqlNode> operands = call.getSqlOperandList();
        assert operands.size() == 1;
        assert operands.get( 0 ) instanceof SqlIdentifier;
        SqlIdentifier id = (SqlIdentifier) operands.get( 0 );
        validator.validateSequenceValue( scope, id );
    }

}

