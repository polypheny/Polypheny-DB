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

package ch.unibas.dmi.dbis.polyphenydb.adapter.pig;


import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import static ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind.INPUT_REF;
import static ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind.LITERAL;


/**
 * Implementation of {@link Filter} in {@link PigRel#CONVENTION Pig calling convention}.
 */
public class PigFilter extends Filter implements PigRel {

    /**
     * Creates a PigFilter.
     */
    public PigFilter( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition ) {
        super( cluster, traitSet, input, condition );
        assert getConvention() == CONVENTION;
    }


    @Override
    public Filter copy( RelTraitSet traitSet, RelNode input, RexNode condition ) {
        return new PigFilter( getCluster(), traitSet, input, condition );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        implementor.addStatement( getPigFilterStatement( implementor ) );
    }


    /**
     * Override this method so it looks down the tree to find the table this node is acting on.
     */
    @Override
    public RelOptTable getTable() {
        return getInput().getTable();
    }


    /**
     * Generates Pig Latin filtering statements, for example
     *
     * <pre>table = FILTER table BY score &gt; 2.0;</pre>
     */
    private String getPigFilterStatement( Implementor implementor ) {
        Preconditions.checkState( containsOnlyConjunctions( condition ) );
        String relationAlias = implementor.getPigRelationAlias( this );
        List<String> filterConditionsConjunction = new ArrayList<>();
        for ( RexNode node : RelOptUtil.conjunctions( condition ) ) {
            filterConditionsConjunction.add( getSingleFilterCondition( implementor, node ) );
        }
        String allFilterConditions = String.join( " AND ", filterConditionsConjunction );
        return relationAlias + " = FILTER " + relationAlias + " BY " + allFilterConditions + ';';
    }


    private String getSingleFilterCondition( Implementor implementor, RexNode node ) {
        switch ( node.getKind() ) {
            case EQUALS:
                return getSingleFilterCondition( implementor, "==", (RexCall) node );
            case LESS_THAN:
                return getSingleFilterCondition( implementor, "<", (RexCall) node );
            case LESS_THAN_OR_EQUAL:
                return getSingleFilterCondition( implementor, "<=", (RexCall) node );
            case GREATER_THAN:
                return getSingleFilterCondition( implementor, ">", (RexCall) node );
            case GREATER_THAN_OR_EQUAL:
                return getSingleFilterCondition( implementor, ">=", (RexCall) node );
            default:
                throw new IllegalArgumentException( "Cannot translate node " + node );
        }
    }


    private String getSingleFilterCondition( Implementor implementor, String op, RexCall call ) {
        final String fieldName;
        final String literal;
        final RexNode left = call.operands.get( 0 );
        final RexNode right = call.operands.get( 1 );
        if ( left.getKind() == LITERAL ) {
            if ( right.getKind() != INPUT_REF ) {
                throw new IllegalArgumentException( "Expected a RexCall with a single field and single literal" );
            } else {
                fieldName = implementor.getFieldName( this, ((RexInputRef) right).getIndex() );
                literal = getLiteralAsString( (RexLiteral) left );
            }
        } else if ( right.getKind() == LITERAL ) {
            if ( left.getKind() != INPUT_REF ) {
                throw new IllegalArgumentException( "Expected a RexCall with a single field and single literal" );
            } else {
                fieldName = implementor.getFieldName( this, ((RexInputRef) left).getIndex() );
                literal = getLiteralAsString( (RexLiteral) right );
            }
        } else {
            throw new IllegalArgumentException( "Expected a RexCall with a single field and single literal" );
        }

        return '(' + fieldName + ' ' + op + ' ' + literal + ')';
    }


    private boolean containsOnlyConjunctions( RexNode condition ) {
        return RelOptUtil.disjunctions( condition ).size() == 1;
    }


    /**
     * TODO: do proper literal to string conversion + escaping
     */
    private String getLiteralAsString( RexLiteral literal ) {
        return '\'' + RexLiteral.stringValue( literal ) + '\'';
    }
}

