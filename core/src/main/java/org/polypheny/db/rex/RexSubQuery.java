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
import javax.annotation.Nonnull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.QuantifyOperator;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.type.PolyType;


/**
 * Scalar expression that represents an IN, EXISTS or scalar sub-query.
 */
public class RexSubQuery extends RexCall {

    public final AlgNode alg;


    private RexSubQuery( AlgDataType type, Operator op, ImmutableList<RexNode> operands, AlgNode alg ) {
        super( type, op, operands );
        this.alg = alg;
        this.digest = computeDigest( false );
    }


    /**
     * Creates an IN sub-query.
     */
    public static RexSubQuery in( AlgNode alg, ImmutableList<RexNode> nodes ) {
        final AlgDataType type = type( alg, nodes );
        return new RexSubQuery( type, OperatorRegistry.get( OperatorName.IN ), nodes, alg );
    }


    /**
     * Creates a SOME sub-query.
     *
     * There is no ALL. For {@code x comparison ALL (sub-query)} use instead {@code NOT (x inverse-comparison SOME (sub-query))}.
     * If {@code comparison} is {@code >} then {@code negated-comparison} is {@code <=}, and so forth.
     */
    public static RexSubQuery some( AlgNode alg, ImmutableList<RexNode> nodes, QuantifyOperator op ) {
        assert op.getKind() == Kind.SOME;
        final AlgDataType type = type( alg, nodes );
        return new RexSubQuery( type, op, nodes, alg );
    }


    static AlgDataType type( AlgNode alg, ImmutableList<RexNode> nodes ) {
        assert alg.getTupleType().getFieldCount() == nodes.size();
        final AlgDataTypeFactory typeFactory = alg.getCluster().getTypeFactory();
        boolean nullable = false;
        for ( RexNode node : nodes ) {
            if ( node.getType().isNullable() ) {
                nullable = true;
            }
        }
        for ( AlgDataTypeField field : alg.getTupleType().getFields() ) {
            if ( field.getType().isNullable() ) {
                nullable = true;
            }
        }
        return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.BOOLEAN ), nullable );
    }


    /**
     * Creates an EXISTS sub-query.
     */
    public static RexSubQuery exists( AlgNode alg ) {
        final AlgDataTypeFactory typeFactory = alg.getCluster().getTypeFactory();
        final AlgDataType type = typeFactory.createPolyType( PolyType.BOOLEAN );
        return new RexSubQuery( type, OperatorRegistry.get( OperatorName.EXISTS ), ImmutableList.of(), alg );
    }


    /**
     * Creates a scalar sub-query.
     */
    public static RexSubQuery scalar( AlgNode alg ) {
        final List<AlgDataTypeField> fieldList = alg.getTupleType().getFields();
        assert fieldList.size() == 1;
        final AlgDataTypeFactory typeFactory = alg.getCluster().getTypeFactory();
        final AlgDataType type = typeFactory.createTypeWithNullability( fieldList.get( 0 ).getType(), true );
        return new RexSubQuery( type, OperatorRegistry.get( OperatorName.SCALAR_QUERY ), ImmutableList.of(), alg );
    }


    @Override
    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitSubQuery( this );
    }


    @Override
    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitSubQuery( this, arg );
    }


    @Override
    protected @Nonnull
    String computeDigest( boolean withType ) {
        return computeDigest( withType, null );
    }


    @Override
    protected @Nonnull
    String computeDigest( boolean withType, RexVisitor<String> visitor ) {
        final StringBuilder sb = new StringBuilder( op.getName() );
        sb.append( "(" );
        for ( RexNode operand : operands ) {
            sb.append( visitor == null ? operand : operand.accept( visitor ) );
            sb.append( ", " );
        }
        sb.append( "{\n" );
        sb.append( AlgOptUtil.toString( alg ) );
        sb.append( "})" );
        return sb.toString();
    }


    @Override
    public RexSubQuery clone( AlgDataType type, List<RexNode> operands ) {
        return new RexSubQuery( type, getOperator(), ImmutableList.copyOf( operands ), alg );
    }


    public RexSubQuery clone( AlgNode alg ) {
        return new RexSubQuery( type, getOperator(), operands, alg );
    }


    public RexSubQuery clone( AlgDataType type, List<RexNode> operands, AlgNode alg ) {
        return new RexSubQuery( type, getOperator(), ImmutableList.copyOf( operands ), alg );
    }

}

