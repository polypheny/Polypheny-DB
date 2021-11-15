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


import org.polypheny.db.core.Call;
import org.polypheny.db.core.FunctionCategory;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Literal;
import org.polypheny.db.core.Node;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.core.json.JsonQueryEmptyOrErrorBehavior;
import org.polypheny.db.core.json.JsonQueryWrapperBehavior;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlFunction;
import org.polypheny.db.languages.sql.SqlLiteral;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeTransforms;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * The <code>JSON_QUERY</code> function.
 */
public class SqlJsonQueryFunction extends SqlFunction {

    public SqlJsonQueryFunction() {
        super(
                "JSON_QUERY",
                Kind.OTHER_FUNCTION,
                ReturnTypes.cascade( ReturnTypes.VARCHAR_2000, PolyTypeTransforms.FORCE_NULLABLE ),
                null,
                OperandTypes.family( PolyTypeFamily.ANY, PolyTypeFamily.ANY, PolyTypeFamily.ANY, PolyTypeFamily.ANY ),
                FunctionCategory.SYSTEM );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        return "{0}({1} {2} WRAPPER {3} ON EMPTY {4} ON ERROR)";
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        ((SqlNode) call.operand( 0 )).unparse( writer, 0, 0 );
        final JsonQueryWrapperBehavior wrapperBehavior =
                getEnumValue( call.operand( 1 ) );
        switch ( wrapperBehavior ) {
            case WITHOUT_ARRAY:
                writer.keyword( "WITHOUT ARRAY" );
                break;
            case WITH_CONDITIONAL_ARRAY:
                writer.keyword( "WITH CONDITIONAL ARRAY" );
                break;
            case WITH_UNCONDITIONAL_ARRAY:
                writer.keyword( "WITH UNCONDITIONAL ARRAY" );
                break;
            default:
                throw new IllegalStateException( "unreachable code" );
        }
        writer.keyword( "WRAPPER" );
        unparseEmptyOrErrorBehavior( writer, getEnumValue( call.operand( 2 ) ) );
        writer.keyword( "ON EMPTY" );
        unparseEmptyOrErrorBehavior( writer, getEnumValue( call.operand( 3 ) ) );
        writer.keyword( "ON ERROR" );
        writer.endFunCall( frame );
    }


    @Override
    public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
        if ( operands[1] == null ) {
            operands[1] = SqlLiteral.createSymbol( JsonQueryWrapperBehavior.WITHOUT_ARRAY, pos );
        }
        if ( operands[2] == null ) {
            operands[2] = SqlLiteral.createSymbol( JsonQueryEmptyOrErrorBehavior.NULL, pos );
        }
        if ( operands[3] == null ) {
            operands[3] = SqlLiteral.createSymbol( JsonQueryEmptyOrErrorBehavior.NULL, pos );
        }
        return super.createCall( functionQualifier, pos, operands );
    }


    private void unparseEmptyOrErrorBehavior( SqlWriter writer, JsonQueryEmptyOrErrorBehavior emptyBehavior ) {
        switch ( emptyBehavior ) {
            case NULL:
                writer.keyword( "NULL" );
                break;
            case ERROR:
                writer.keyword( "ERROR" );
                break;
            case EMPTY_ARRAY:
                writer.keyword( "EMPTY ARRAY" );
                break;
            case EMPTY_OBJECT:
                writer.keyword( "EMPTY OBJECT" );
                break;
            default:
                throw new IllegalStateException( "unreachable code" );
        }
    }


    private <E extends Enum<E>> E getEnumValue( SqlNode operand ) {
        return (E) ((SqlLiteral) operand).getValue();
    }

}

