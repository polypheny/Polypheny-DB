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


import java.util.Arrays;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.MapPolyType;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolySingleOperandTypeChecker;


/**
 * The item operator {@code [ ... ]}, used to access a given element of an array or map. For example, {@code myArray[3]} or {@code "myMap['foo']"}.
 */
public class SqlItemOperator extends SqlSpecialOperator {

    private static final PolySingleOperandTypeChecker ARRAY_OR_MAP =
            OperandTypes.or(
                    OperandTypes.family( PolyTypeFamily.ARRAY ),
                    OperandTypes.family( PolyTypeFamily.MAP ),
                    OperandTypes.family( PolyTypeFamily.ANY ) );


    public SqlItemOperator() {
        super( "ITEM", Kind.OTHER_FUNCTION, 100, true, null, null, null );
    }


    @Override
    public ReduceResult reduceExpr( int ordinal, TokenSequence list ) {
        SqlNode left = list.node( ordinal - 1 );
        SqlNode right = list.node( ordinal + 1 );
        //if expression is of type a[i:j]
        if ( list.size() > 3 && !list.isOp( 3 ) ) {
            SqlNode right2 = list.node( ordinal + 2 );
            return new ReduceResult(
                    ordinal - 1,
                    ordinal + 3,
                    (SqlNode) createCall(
                            ParserPos.sum(
                                    Arrays.asList(
                                            left.getPos(),
                                            right2.getPos(),
                                            list.pos( ordinal ) ) ),
                            left,
                            right, right2 ) );
        }
        return new ReduceResult(
                ordinal - 1,
                ordinal + 2,
                (SqlNode) createCall(
                        ParserPos.sum(
                                Arrays.asList(
                                        left.getPos(),
                                        right.getPos(),
                                        list.pos( ordinal ) ) ),
                        left,
                        right ) );
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
    public OperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.between( 2, 3 );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final SqlNode left = (SqlNode) callBinding.operand( 0 );
        final SqlNode right = (SqlNode) callBinding.operand( 1 );
        if ( !ARRAY_OR_MAP.checkSingleOperandType( callBinding, left, 0, throwOnFailure ) ) {
            return false;
        }
        final AlgDataType operandType = callBinding.getOperandType( 0 );
        final PolySingleOperandTypeChecker checker = getChecker( operandType );
        return checker.checkSingleOperandType( callBinding, right, 0, throwOnFailure );
    }


    private PolySingleOperandTypeChecker getChecker( AlgDataType operandType ) {
        return switch ( operandType.getPolyType() ) {
            case ARRAY -> OperandTypes.family( PolyTypeFamily.INTEGER );
            case MAP -> OperandTypes.family( operandType.unwrap( MapPolyType.class ).orElseThrow().getKeyType().getPolyType().getFamily() );
            case ANY, DYNAMIC_STAR -> OperandTypes.or(
                    OperandTypes.family( PolyTypeFamily.INTEGER ),
                    OperandTypes.family( PolyTypeFamily.CHARACTER ) );
            default -> throw new AssertionError( operandType.getPolyType() );
        };
    }


    @Override
    public String getAllowedSignatures( String name ) {
        return "<ARRAY>[<INTEGER>]\n" + "<MAP>[<VALUE>]";
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final AlgDataType operandType = opBinding.getOperandType( 0 );
        switch ( operandType.getPolyType() ) {
            case ARRAY:
                if ( operandType instanceof ArrayType && ((ArrayType) operandType).getDimension() > 1 ) {
                    long dimension = ((ArrayType) operandType).getDimension() - 1;
                    //set the dimension to -1 if you have a slice operator ([1:1]), so the returnType will be adjusted to array if they are additional itemOperators (without the slice operator)
                    if ( opBinding.getOperandCount() > 2 ) {
                        dimension = -1;
                    }
                    return typeFactory.createArrayType( operandType.getComponentType(), ((ArrayType) operandType).getCardinality(), dimension );
                } else if ( operandType instanceof ArrayType && ((ArrayType) operandType).getDimension() < 0//if dimension was set to -1
                        && !(((SqlCallBinding) opBinding).operand( 0 ) instanceof SqlIdentifier)//e.g. a[1]
                        && !(operandType.getComponentType() instanceof AlgRecordType) ) {//e.g. a.b[1].c.d[1], for unit tests
                    long dimension = ((ArrayType) operandType).getDimension() - 1;
                    if ( opBinding.getOperandCount() > 2 ) {
                        dimension = -1;
                    }//if dimension was set to -1, the returned type will be an array (e.g. a[1:1][1] is of type array and [1:1] sets the cardinality to -1
                    return typeFactory.createArrayType( operandType.getComponentType(), ((ArrayType) operandType).getCardinality(), dimension );
                } else {
                    return typeFactory.createTypeWithNullability( operandType.getComponentType(), true );
                }
            case MAP:
                return typeFactory.createTypeWithNullability( operandType.unwrap( MapPolyType.class ).orElseThrow().getValueType(), true );
            case ANY:
            case DYNAMIC_STAR:
                return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true );
            default:
                throw new AssertionError();
        }
    }

}
