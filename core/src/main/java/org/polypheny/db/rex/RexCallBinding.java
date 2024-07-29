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

package org.polypheny.db.rex;


import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.validate.ValidatorException;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.Resources.ExInst;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.CoreUtil;


/**
 * <code>RexCallBinding</code> implements {@link OperatorBinding} by referring to an underlying collection of {@link RexNode} operands.
 */
public class RexCallBinding extends OperatorBinding {

    @Getter
    private final List<RexNode> operands;

    private final List<AlgCollation> inputCollations;


    public RexCallBinding( AlgDataTypeFactory typeFactory, Operator sqlOperator, List<? extends RexNode> operands, List<AlgCollation> inputCollations ) {
        super( typeFactory, sqlOperator );
        this.operands = ImmutableList.copyOf( operands );
        this.inputCollations = ImmutableList.copyOf( inputCollations );
    }


    /**
     * Creates a binding of the appropriate type.
     */
    public static RexCallBinding create( AlgDataTypeFactory typeFactory, RexCall call, List<AlgCollation> inputCollations ) {
        if ( call.getKind() == Kind.CAST ) {
            return new RexCastCallBinding( typeFactory, call.getOperator(), call.getOperands(), call.getType(), inputCollations );
        }
        return new RexCallBinding( typeFactory, call.getOperator(), call.getOperands(), inputCollations );
    }


    @Override
    public PolyValue getOperandLiteralValue( int ordinal, PolyType type ) {
        final RexNode node = operands.get( ordinal );
        if ( node instanceof RexLiteral ) {
            return ((RexLiteral) node).value;
        }
        return RexLiteral.value( node );
    }


    @Override
    public Monotonicity getOperandMonotonicity( int ordinal ) {
        RexNode operand = operands.get( ordinal );

        if ( operand instanceof RexIndexRef ) {
            for ( AlgCollation ic : inputCollations ) {
                if ( ic.getFieldCollations().isEmpty() ) {
                    continue;
                }

                for ( AlgFieldCollation rfc : ic.getFieldCollations() ) {
                    if ( rfc.getFieldIndex() == ((RexIndexRef) operand).getIndex() ) {
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

        return Monotonicity.NOT_MONOTONIC;
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
    public AlgDataType getOperandType( int ordinal ) {
        return operands.get( ordinal ).getType();
    }


    @Override
    public PolyphenyDbException newError( ExInst<ValidatorException> e ) {
        return CoreUtil.newContextException( ParserPos.ZERO, e );
    }


    /**
     * To be compatible with {@code SqlCall}, CAST needs to pretend that it has two arguments, the second of which is the target type.
     */
    private static class RexCastCallBinding extends RexCallBinding {

        private final AlgDataType type;


        RexCastCallBinding( AlgDataTypeFactory typeFactory, Operator sqlOperator, List<? extends RexNode> operands, AlgDataType type, List<AlgCollation> inputCollations ) {
            super( typeFactory, sqlOperator, operands, inputCollations );
            this.type = type;
        }


        @Override
        public AlgDataType getOperandType( int ordinal ) {
            if ( ordinal == 1 ) {
                return type;
            }
            return super.getOperandType( ordinal );
        }

    }

}

