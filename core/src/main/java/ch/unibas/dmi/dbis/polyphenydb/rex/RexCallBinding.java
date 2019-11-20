/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rex;


import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbException;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Resources.ExInst;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlMonotonicity;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorException;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * <code>RexCallBinding</code> implements {@link SqlOperatorBinding} by referring to an underlying collection of {@link RexNode} operands.
 */
public class RexCallBinding extends SqlOperatorBinding {

    private final List<RexNode> operands;

    private final List<RelCollation> inputCollations;


    public RexCallBinding( RelDataTypeFactory typeFactory, SqlOperator sqlOperator, List<? extends RexNode> operands, List<RelCollation> inputCollations ) {
        super( typeFactory, sqlOperator );
        this.operands = ImmutableList.copyOf( operands );
        this.inputCollations = ImmutableList.copyOf( inputCollations );
    }


    /**
     * Creates a binding of the appropriate type.
     */
    public static RexCallBinding create( RelDataTypeFactory typeFactory, RexCall call, List<RelCollation> inputCollations ) {
        if ( call.getKind() == SqlKind.CAST ) {
            return new RexCastCallBinding( typeFactory, call.getOperator(), call.getOperands(), call.getType(), inputCollations );
        }
        return new RexCallBinding( typeFactory, call.getOperator(), call.getOperands(), inputCollations );
    }


    @Override
    public <T> T getOperandLiteralValue( int ordinal, Class<T> clazz ) {
        final RexNode node = operands.get( ordinal );
        if ( node instanceof RexLiteral ) {
            return ((RexLiteral) node).getValueAs( clazz );
        }
        return clazz.cast( RexLiteral.value( node ) );
    }


    @Override
    public SqlMonotonicity getOperandMonotonicity( int ordinal ) {
        RexNode operand = operands.get( ordinal );

        if ( operand instanceof RexInputRef ) {
            for ( RelCollation ic : inputCollations ) {
                if ( ic.getFieldCollations().isEmpty() ) {
                    continue;
                }

                for ( RelFieldCollation rfc : ic.getFieldCollations() ) {
                    if ( rfc.getFieldIndex() == ((RexInputRef) operand).getIndex() ) {
                        return rfc.direction.monotonicity();
                        // TODO: Is it possible to have more than one RelFieldCollation for a RexInputRef?
                    }
                }
            }
        } else if ( operand instanceof RexCall ) {
            final RexCallBinding binding =
                    RexCallBinding.create( typeFactory, (RexCall) operand, inputCollations );
            return ((RexCall) operand).getOperator().getMonotonicity( binding );
        }

        return SqlMonotonicity.NOT_MONOTONIC;
    }


    @Override
    public boolean isOperandNull( int ordinal, boolean allowCast ) {
        return RexUtil.isNullLiteral( operands.get( ordinal ), allowCast );
    }


    @Override
    public boolean isOperandLiteral( int ordinal, boolean allowCast ) {
        return RexUtil.isLiteral( operands.get( ordinal ), allowCast );
    }


    // implement SqlOperatorBinding
    @Override
    public int getOperandCount() {
        return operands.size();
    }


    // implement SqlOperatorBinding
    @Override
    public RelDataType getOperandType( int ordinal ) {
        return operands.get( ordinal ).getType();
    }


    @Override
    public PolyphenyDbException newError( ExInst<SqlValidatorException> e ) {
        return SqlUtil.newContextException( SqlParserPos.ZERO, e );
    }


    /**
     * To be compatible with {@code SqlCall}, CAST needs to pretend that it has two arguments, the second of which is the target type.
     */
    private static class RexCastCallBinding extends RexCallBinding {

        private final RelDataType type;


        RexCastCallBinding( RelDataTypeFactory typeFactory, SqlOperator sqlOperator, List<? extends RexNode> operands, RelDataType type, List<RelCollation> inputCollations ) {
            super( typeFactory, sqlOperator, operands, inputCollations );
            this.type = type;
        }


        @Override
        public RelDataType getOperandType( int ordinal ) {
            if ( ordinal == 1 ) {
                return type;
            }
            return super.getOperandType( ordinal );
        }
    }
}

