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

package org.polypheny.db.adapter.pig;


import static org.polypheny.db.algebra.constant.Kind.INPUT_REF;
import static org.polypheny.db.algebra.constant.Kind.LITERAL;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;


/**
 * Implementation of {@link Filter} in {@link PigAlg#CONVENTION Pig calling convention}.
 */
public class PigFilter extends Filter implements PigAlg {

    /**
     * Creates a PigFilter.
     */
    public PigFilter( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        super( cluster, traitSet, input, condition );
        assert getConvention() == CONVENTION;
    }


    @Override
    public Filter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
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
    public AlgOptTable getTable() {
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
        for ( RexNode node : AlgOptUtil.conjunctions( condition ) ) {
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
        return AlgOptUtil.disjunctions( condition ).size() == 1;
    }


    /**
     * TODO: do proper literal to string conversion + escaping
     */
    private String getLiteralAsString( RexLiteral literal ) {
        return '\'' + RexLiteral.stringValue( literal ) + '\'';
    }

}

