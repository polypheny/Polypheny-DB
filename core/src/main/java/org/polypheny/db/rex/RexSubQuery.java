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

package org.polypheny.db.rex;


import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nonnull;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Operator;
import org.polypheny.db.core.QuantifyOperator;
import org.polypheny.db.core.StdOperatorRegistry;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.type.PolyType;


/**
 * Scalar expression that represents an IN, EXISTS or scalar sub-query.
 */
public class RexSubQuery extends RexCall {

    public final RelNode rel;


    private RexSubQuery( RelDataType type, Operator op, ImmutableList<RexNode> operands, RelNode rel ) {
        super( type, op, operands );
        this.rel = rel;
        this.digest = computeDigest( false );
    }


    /**
     * Creates an IN sub-query.
     */
    public static RexSubQuery in( RelNode rel, ImmutableList<RexNode> nodes ) {
        final RelDataType type = type( rel, nodes );
        return new RexSubQuery( type, StdOperatorRegistry.get( "IN" ), nodes, rel );
    }


    /**
     * Creates a SOME sub-query.
     *
     * There is no ALL. For {@code x comparison ALL (sub-query)} use instead {@code NOT (x inverse-comparison SOME (sub-query))}.
     * If {@code comparison} is {@code >} then {@code negated-comparison} is {@code <=}, and so forth.
     */
    public static RexSubQuery some( RelNode rel, ImmutableList<RexNode> nodes, QuantifyOperator op ) {
        assert op.getKind() == Kind.SOME;
        final RelDataType type = type( rel, nodes );
        return new RexSubQuery( type, op, nodes, rel );
    }


    static RelDataType type( RelNode rel, ImmutableList<RexNode> nodes ) {
        assert rel.getRowType().getFieldCount() == nodes.size();
        final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
        boolean nullable = false;
        for ( RexNode node : nodes ) {
            if ( node.getType().isNullable() ) {
                nullable = true;
            }
        }
        for ( RelDataTypeField field : rel.getRowType().getFieldList() ) {
            if ( field.getType().isNullable() ) {
                nullable = true;
            }
        }
        return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.BOOLEAN ), nullable );
    }


    /**
     * Creates an EXISTS sub-query.
     */
    public static RexSubQuery exists( RelNode rel ) {
        final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
        final RelDataType type = typeFactory.createPolyType( PolyType.BOOLEAN );
        return new RexSubQuery( type, StdOperatorRegistry.get( "EXISTS" ), ImmutableList.of(), rel );
    }


    /**
     * Creates a scalar sub-query.
     */
    public static RexSubQuery scalar( RelNode rel ) {
        final List<RelDataTypeField> fieldList = rel.getRowType().getFieldList();
        assert fieldList.size() == 1;
        final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
        final RelDataType type = typeFactory.createTypeWithNullability( fieldList.get( 0 ).getType(), true );
        return new RexSubQuery( type, StdOperatorRegistry.get( "SCALAR_QUERY" ), ImmutableList.of(), rel );
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
        final StringBuilder sb = new StringBuilder( op.getName() );
        sb.append( "(" );
        for ( RexNode operand : operands ) {
            sb.append( operand );
            sb.append( ", " );
        }
        sb.append( "{\n" );
        sb.append( RelOptUtil.toString( rel ) );
        sb.append( "})" );
        return sb.toString();
    }


    @Override
    public RexSubQuery clone( RelDataType type, List<RexNode> operands ) {
        return new RexSubQuery( type, getOperator(), ImmutableList.copyOf( operands ), rel );
    }


    public RexSubQuery clone( RelNode rel ) {
        return new RexSubQuery( type, getOperator(), operands, rel );
    }


    public RexSubQuery clone( RelDataType type, List<RexNode> operands, RelNode rel ) {
        return new RexSubQuery( type, getOperator(), ImmutableList.copyOf( operands ), rel );
    }

}

