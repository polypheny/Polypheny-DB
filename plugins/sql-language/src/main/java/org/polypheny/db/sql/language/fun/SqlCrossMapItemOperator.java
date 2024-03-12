/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorScope;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;

public class SqlCrossMapItemOperator extends SqlSpecialOperator {

    public SqlCrossMapItemOperator() {
        super( "MAP", Kind.OTHER_FUNCTION );
    }


    @Override
    public OperatorName getOperatorName() {
        return OperatorName.CROSS_MODEL_ITEM;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, 0 );
        final SqlWriter.Frame frame = writer.startList( "[", "]" );
        ((SqlNode) call.operand( 1 )).unparse( writer, 0, 0 );
        if ( call.operandCount() > 2 ) {
            writer.literal( ":" );
            ((SqlNode) call.operand( 2 )).unparse( writer, 0, 0 );
        }
        writer.endList( frame );
    }


    @Override
    public AlgDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        AlgDataType type = super.deriveType( validator, scope, call );
        /*if ( type instanceof ArrayType ) {
            ((ArrayType) type).setCardinality( maxCardinality ).setDimension( dimension );
        }*/
        //set the operator again, because SqlOperator.deriveType will clear the dimension & cardinality of this constructor
        ((SqlBasicCall) call).setOperator( this );
        return type;
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        // empty on purpose
    }

}
