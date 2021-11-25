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


import java.util.Arrays;
import org.polypheny.db.core.nodes.BasicNodeVisitor.ArgHandler;
import org.polypheny.db.core.nodes.Call;
import org.polypheny.db.core.util.CoreUtil;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.nodes.Node;
import org.polypheny.db.core.nodes.NodeVisitor;
import org.polypheny.db.core.nodes.OperatorBinding;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.core.validate.Validator;
import org.polypheny.db.core.validate.ValidatorScope;
import org.polypheny.db.core.util.ValidatorUtil;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlCallBinding;
import org.polypheny.db.languages.sql.SqlIdentifier;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.languages.sql.validate.SqlValidatorScope;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolySingleOperandTypeChecker;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Static;


/**
 * The dot operator {@code .}, used to access a field of a record. For example, {@code a.b}.
 */
public class SqlDotOperator extends SqlSpecialOperator {

    public SqlDotOperator() {
        super( "DOT", Kind.DOT, 100, true, null, null, null );
    }


    @Override
    public ReduceResult reduceExpr( int ordinal, TokenSequence list ) {
        SqlNode left = list.node( ordinal - 1 );
        SqlNode right = list.node( ordinal + 1 );
        return new ReduceResult(
                ordinal - 1,
                ordinal + 2,
                (SqlNode) createCall(
                        ParserPos.sum( Arrays.asList( left.getPos(), right.getPos(), list.pos( ordinal ) ) ),
                        left,
                        right ) );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.IDENTIFIER );
        ((SqlNode) call.operand( 0 )).unparse( writer, leftPrec, 0 );
        writer.sep( "." );
        ((SqlNode) call.operand( 1 )).unparse( writer, 0, 0 );
        writer.endList( frame );
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.of( 2 );
    }


    @Override
    public <R> void acceptCall( NodeVisitor<R> visitor, Call call, boolean onlyExpressions, ArgHandler<R> argHandler ) {
        if ( onlyExpressions ) {
            // Do not visit operands[1] -- it is not an expression.
            argHandler.visitChild( visitor, (Node) call, 0, ((SqlCall) call).operand( 0 ) );
        } else {
            super.acceptCall( visitor, call, onlyExpressions, argHandler );
        }
    }


    @Override
    public RelDataType deriveType( Validator validator, ValidatorScope scope, Call call ) {
        final Node operand = call.getOperandList().get( 0 );
        final RelDataType nodeType = validator.deriveType( scope, operand );
        assert nodeType != null;
        if ( !nodeType.isStruct() ) {
            throw CoreUtil.newContextException( operand.getPos(), Static.RESOURCE.incompatibleTypes() );
        }

        final SqlNode fieldId = call.operand( 1 );
        final String fieldName = fieldId.toString();
        final RelDataTypeField field = nodeType.getField( fieldName, false, false );
        if ( field == null ) {
            throw CoreUtil.newContextException( fieldId.getPos(), Static.RESOURCE.unknownField( fieldName ) );
        }
        RelDataType type = field.getType();

        // Validate and determine coercibility and resulting collation name of binary operator if needed.
        type = adjustType( (SqlValidator) validator, (SqlCall) call, type );
        ValidatorUtil.checkCharsetAndCollateConsistentIfCharType( type );
        return type;
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        assert call.getOperator().equals( this );
        // Do not visit call.getOperandList().get(1) here.
        // call.getOperandList().get(1) will be validated when deriveType() is called.
        ((SqlNode) call.getOperandList().get( 0 )).validateExpr( validator, operandScope );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final SqlNode left = (SqlNode) callBinding.operand( 0 );
        final SqlNode right = (SqlNode) callBinding.operand( 1 );
        final RelDataType type = callBinding.getValidator().deriveType( callBinding.getScope(), left );
        if ( type.getPolyType() != PolyType.ROW ) {
            return false;
        } else if ( ((SqlIdentifier) type.getSqlIdentifier()).isStar() ) {
            return false;
        }
        final RelDataType operandType = callBinding.getOperandType( 0 );
        final PolySingleOperandTypeChecker checker = getChecker( operandType );
        return checker.checkSingleOperandType( callBinding, right, 0, throwOnFailure );
    }


    private PolySingleOperandTypeChecker getChecker( RelDataType operandType ) {
        switch ( operandType.getPolyType() ) {
            case ROW:
                return OperandTypes.family( PolyTypeFamily.STRING );
            default:
                throw new AssertionError( operandType.getPolyType() );
        }
    }


    @Override
    public boolean validRexOperands( final int count, final Litmus litmus ) {
        return litmus.fail( "DOT is valid only for SqlCall not for RexCall" );
    }


    @Override
    public String getAllowedSignatures( String name ) {
        return "<A>.<B>";
    }


    @Override
    public RelDataType inferReturnType( OperatorBinding opBinding ) {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataType recordType = opBinding.getOperandType( 0 );
        switch ( recordType.getPolyType() ) {
            case ROW:
                final String fieldName = opBinding.getOperandLiteralValue( 1, String.class );
                final RelDataType type = opBinding.getOperandType( 0 )
                        .getField( fieldName, false, false )
                        .getType();
                if ( recordType.isNullable() ) {
                    return typeFactory.createTypeWithNullability( type, true );
                } else {
                    return type;
                }
            default:
                throw new AssertionError();
        }
    }

}
