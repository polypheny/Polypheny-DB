/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.sql.fun;


import java.util.Arrays;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlCallBinding;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperandCountRange;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.type.OperandTypes;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.PolySingleOperandTypeChecker;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;


/**
 * The item operator {@code [ ... ]}, used to access a given element of an array or map. For example, {@code myArray[3]} or {@code "myMap['foo']"}.
 */
class SqlItemOperator extends SqlSpecialOperator {

    private static final PolySingleOperandTypeChecker ARRAY_OR_MAP =
            OperandTypes.or(
                    OperandTypes.family( PolyTypeFamily.ARRAY ),
                    OperandTypes.family( PolyTypeFamily.MAP ),
                    OperandTypes.family( PolyTypeFamily.ANY ) );


    SqlItemOperator() {
        super( "ITEM", SqlKind.OTHER_FUNCTION, 100, true, null, null, null );
    }


    @Override
    public ReduceResult reduceExpr( int ordinal, TokenSequence list ) {
        SqlNode left = list.node( ordinal - 1 );
        SqlNode right = list.node( ordinal + 1 );
        return new ReduceResult(
                ordinal - 1,
                ordinal + 2,
                createCall(
                        SqlParserPos.sum(
                                Arrays.asList(
                                        left.getParserPosition(),
                                        right.getParserPosition(),
                                        list.pos( ordinal ) ) ),
                        left,
                        right ) );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        call.operand( 0 ).unparse( writer, leftPrec, 0 );
        final SqlWriter.Frame frame = writer.startList( "[", "]" );
        call.operand( 1 ).unparse( writer, 0, 0 );
        writer.endList( frame );
    }


    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.of( 2 );
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        final SqlNode left = callBinding.operand( 0 );
        final SqlNode right = callBinding.operand( 1 );
        if ( !ARRAY_OR_MAP.checkSingleOperandType( callBinding, left, 0, throwOnFailure ) ) {
            return false;
        }
        final RelDataType operandType = callBinding.getOperandType( 0 );
        final PolySingleOperandTypeChecker checker = getChecker( operandType );
        return checker.checkSingleOperandType( callBinding, right, 0, throwOnFailure );
    }


    private PolySingleOperandTypeChecker getChecker( RelDataType operandType ) {
        switch ( operandType.getSqlTypeName() ) {
            case ARRAY:
                return OperandTypes.family( PolyTypeFamily.INTEGER );
            case MAP:
                return OperandTypes.family( operandType.getKeyType().getSqlTypeName().getFamily() );
            case ANY:
            case DYNAMIC_STAR:
                return OperandTypes.or(
                        OperandTypes.family( PolyTypeFamily.INTEGER ),
                        OperandTypes.family( PolyTypeFamily.CHARACTER ) );
            default:
                throw new AssertionError( operandType.getSqlTypeName() );
        }
    }


    @Override
    public String getAllowedSignatures( String name ) {
        return "<ARRAY>[<INTEGER>]\n" + "<MAP>[<VALUE>]";
    }


    @Override
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataType operandType = opBinding.getOperandType( 0 );
        switch ( operandType.getSqlTypeName() ) {
            case ARRAY:
                return typeFactory.createTypeWithNullability( operandType.getComponentType(), true );
            case MAP:
                return typeFactory.createTypeWithNullability( operandType.getValueType(), true );
            case ANY:
            case DYNAMIC_STAR:
                return typeFactory.createTypeWithNullability( typeFactory.createSqlType( PolyType.ANY ), true );
            default:
                throw new AssertionError();
        }
    }
}
