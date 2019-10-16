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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlQuantifyOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * Scalar expression that represents an IN, EXISTS or scalar sub-query.
 */
public class RexSubQuery extends RexCall {

    public final RelNode rel;


    private RexSubQuery( RelDataType type, SqlOperator op, ImmutableList<RexNode> operands, RelNode rel ) {
        super( type, op, operands );
        this.rel = rel;
        this.digest = computeDigest( false );
    }


    /**
     * Creates an IN sub-query.
     */
    public static RexSubQuery in( RelNode rel, ImmutableList<RexNode> nodes ) {
        final RelDataType type = type( rel, nodes );
        return new RexSubQuery( type, SqlStdOperatorTable.IN, nodes, rel );
    }


    /**
     * Creates a SOME sub-query.
     *
     * There is no ALL. For {@code x comparison ALL (sub-query)} use instead {@code NOT (x inverse-comparison SOME (sub-query))}.
     * If {@code comparison} is {@code >} then {@code negated-comparison} is {@code <=}, and so forth.
     */
    public static RexSubQuery some( RelNode rel, ImmutableList<RexNode> nodes, SqlQuantifyOperator op ) {
        assert op.kind == SqlKind.SOME;
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
        return typeFactory.createTypeWithNullability( typeFactory.createSqlType( SqlTypeName.BOOLEAN ), nullable );
    }


    /**
     * Creates an EXISTS sub-query.
     */
    public static RexSubQuery exists( RelNode rel ) {
        final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
        final RelDataType type = typeFactory.createSqlType( SqlTypeName.BOOLEAN );
        return new RexSubQuery( type, SqlStdOperatorTable.EXISTS, ImmutableList.of(), rel );
    }


    /**
     * Creates a scalar sub-query.
     */
    public static RexSubQuery scalar( RelNode rel ) {
        final List<RelDataTypeField> fieldList = rel.getRowType().getFieldList();
        assert fieldList.size() == 1;
        final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
        final RelDataType type = typeFactory.createTypeWithNullability( fieldList.get( 0 ).getType(), true );
        return new RexSubQuery( type, SqlStdOperatorTable.SCALAR_QUERY, ImmutableList.of(), rel );
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
}

