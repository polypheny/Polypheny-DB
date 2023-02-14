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


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Util;


/**
 * A postfix unary operator.
 */
public class SqlPostfixOperator extends SqlOperator {


    public SqlPostfixOperator( String name, Kind kind, int prec, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker ) {
        super(
                name,
                kind,
                leftPrec( prec, true ),
                rightPrec( prec, true ),
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker );
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.POSTFIX;
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        Util.discard( operandsCount );
        return "{1} {0}";
    }


    @Override
    protected AlgDataType adjustType( SqlValidator validator, SqlCall call, AlgDataType type ) {
        if ( PolyTypeUtil.inCharFamily( type ) ) {
            // Determine coercibility and resulting collation name of unary operator if needed.
            AlgDataType operandType = validator.getValidatedNodeType( call.operand( 0 ) );
            if ( null == operandType ) {
                throw new AssertionError( "operand's type should have been derived" );
            }
            if ( PolyTypeUtil.inCharFamily( operandType ) ) {
                Collation collation = operandType.getCollation();
                assert null != collation : "An implicit or explicit collation should have been set";
                type = validator.getTypeFactory()
                        .createTypeWithCharsetAndCollation( type, type.getCharset(), collation );
            }
        }
        return type;
    }


    @Override
    public boolean validRexOperands( int count, Litmus litmus ) {
        if ( count != 1 ) {
            return litmus.fail( "wrong operand count {} for {}", count, this );
        }
        return litmus.succeed();
    }

}

