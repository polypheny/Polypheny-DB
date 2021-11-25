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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.core.nodes.Call;
import org.polypheny.db.core.enums.FunctionCategory;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.nodes.Literal;
import org.polypheny.db.core.nodes.Node;
import org.polypheny.db.core.nodes.OperatorBinding;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.core.json.JsonValueEmptyOrErrorBehavior;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlCallBinding;
import org.polypheny.db.languages.sql.SqlDataTypeSpec;
import org.polypheny.db.languages.sql.SqlFunction;
import org.polypheny.db.languages.sql.SqlIdentifier;
import org.polypheny.db.languages.sql.SqlLiteral;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.Static;


/**
 * The <code>JSON_VALUE</code> function.
 */
public class SqlJsonValueFunction extends SqlFunction {

    private final boolean returnAny;


    @Override
    public FunctionType getFunctionType() {
        return FunctionType.JSON_VALUE;
    }


    public SqlJsonValueFunction( String name, boolean returnAny ) {
        super(
                name,
                Kind.OTHER_FUNCTION,
                null,
                ( callBinding, returnType, operandTypes ) -> {
                    RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
                    for ( int i = 0; i < operandTypes.length; ++i ) {
                        operandTypes[i] = typeFactory.createPolyType( PolyType.ANY );
                    }
                },
                null,
                FunctionCategory.SYSTEM );
        this.returnAny = returnAny;
    }


    @Override
    public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
        List<SqlNode> operandList = new ArrayList<>();
        operandList.add( (SqlNode) operands[0] );
        if ( operands[1] == null ) {
            operandList.add( SqlLiteral.createSymbol( JsonValueEmptyOrErrorBehavior.NULL, pos ) );
            operandList.add( SqlLiteral.createNull( pos ) );
        } else {
            operandList.add( (SqlNode) operands[1] );
            operandList.add( (SqlNode) operands[2] );
        }
        if ( operands[3] == null ) {
            operandList.add( SqlLiteral.createSymbol( JsonValueEmptyOrErrorBehavior.NULL, pos ) );
            operandList.add( SqlLiteral.createNull( pos ) );
        } else {
            operandList.add( (SqlNode) operands[3] );
            operandList.add( (SqlNode) operands[4] );
        }
        if ( operands.length == 6 && operands[5] != null ) {
            if ( returnAny ) {
                throw new IllegalArgumentException( "illegal returning clause in json_value_any function" );
            }
            operandList.add( (SqlNode) operands[5] );
        } else if ( !returnAny ) {
            SqlDataTypeSpec defaultTypeSpec =
                    new SqlDataTypeSpec(
                            new SqlIdentifier( "VARCHAR", pos ),
                            2000,
                            -1,
                            null,
                            null,
                            pos );
            operandList.add( defaultTypeSpec );
        }
        return super.createCall( functionQualifier, pos, operandList.toArray( SqlNode.EMPTY_ARRAY ) );
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.between( 5, 6 );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final SqlValidator validator = callBinding.getValidator();
        RelDataType defaultValueOnEmptyType = validator.getValidatedNodeType( callBinding.operand( 2 ) );
        RelDataType defaultValueOnErrorType = validator.getValidatedNodeType( callBinding.operand( 4 ) );
        RelDataType returnType = validator.deriveType( callBinding.getScope(), callBinding.operand( 5 ) );
        if ( !canCastFrom( callBinding, throwOnFailure, defaultValueOnEmptyType, returnType ) ) {
            return false;
        }
        if ( !canCastFrom( callBinding, throwOnFailure, defaultValueOnErrorType, returnType ) ) {
            return false;
        }
        return true;
    }


    @Override
    public RelDataType inferReturnType( OperatorBinding opBinding ) {
        assert opBinding.getOperandCount() == 5 || opBinding.getOperandCount() == 6;
        RelDataType ret;
        if ( opBinding.getOperandCount() == 6 ) {
            ret = opBinding.getOperandType( 5 );
        } else {
            ret = opBinding.getTypeFactory().createPolyType( PolyType.ANY );
        }
        return opBinding.getTypeFactory().createTypeWithNullability( ret, true );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        assert operandsCount == 5 || operandsCount == 6;
        if ( operandsCount == 6 ) {
            return "{0}({1} RETURNING {6} {2} {3} ON EMPTY {4} {5} ON ERROR)";
        }
        return "{0}({1} {2} {3} ON EMPTY {4} {5} ON ERROR)";
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() == 5 || call.operandCount() == 6;
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        ((SqlNode) call.operand( 0 )).unparse( writer, 0, 0 );
        if ( !returnAny ) {
            writer.keyword( "RETURNING" );
            ((SqlNode) call.operand( 5 )).unparse( writer, 0, 0 );
        }
        unparseEnum( writer, call.operand( 1 ) );
        if ( isDefaultLiteral( call.operand( 1 ) ) ) {
            ((SqlNode) call.operand( 2 )).unparse( writer, 0, 0 );
        }
        writer.keyword( "ON" );
        writer.keyword( "EMPTY" );
        unparseEnum( writer, call.operand( 3 ) );
        if ( isDefaultLiteral( call.operand( 3 ) ) ) {
            ((SqlNode) call.operand( 4 )).unparse( writer, 0, 0 );
        }
        writer.keyword( "ON" );
        writer.keyword( "ERROR" );
        writer.endFunCall( frame );
    }


    private void unparseEnum( SqlWriter writer, SqlLiteral literal ) {
        writer.keyword( ((Enum) literal.getValue()).name() );
    }


    private boolean isDefaultLiteral( SqlLiteral literal ) {
        return literal.getValueAs( JsonValueEmptyOrErrorBehavior.class ) == JsonValueEmptyOrErrorBehavior.DEFAULT;
    }


    private boolean canCastFrom( SqlCallBinding callBinding, boolean throwOnFailure, RelDataType inType, RelDataType outType ) {
        if ( PolyTypeUtil.canCastFrom( outType, inType, true ) ) {
            return true;
        }
        if ( throwOnFailure ) {
            throw callBinding.newError( Static.RESOURCE.cannotCastValue( inType.toString(), outType.toString() ) );
        }
        return false;
    }

}

