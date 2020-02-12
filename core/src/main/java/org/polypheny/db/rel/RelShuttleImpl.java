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

package ch.unibas.dmi.dbis.polyphenydb.rel;


import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableFunctionScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalAggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalCorrelate;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalExchange;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalIntersect;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalMatch;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalMinus;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalSort;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalUnion;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalValues;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.calcite.linq4j.Ord;


/**
 * Basic implementation of {@link RelShuttle} that calls {@link RelNode#accept(RelShuttle)} on each child, and {@link RelNode#copy(ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet, java.util.List)} if
 * any children change.
 */
public class RelShuttleImpl implements RelShuttle {

    protected final Deque<RelNode> stack = new ArrayDeque<>();


    /**
     * Visits a particular child of a parent.
     */
    protected RelNode visitChild( RelNode parent, int i, RelNode child ) {
        stack.push( parent );
        try {
            RelNode child2 = child.accept( this );
            if ( child2 != child ) {
                final List<RelNode> newInputs = new ArrayList<>( parent.getInputs() );
                newInputs.set( i, child2 );
                return parent.copy( parent.getTraitSet(), newInputs );
            }
            return parent;
        } finally {
            stack.pop();
        }
    }


    protected RelNode visitChildren( RelNode rel ) {
        for ( Ord<RelNode> input : Ord.zip( rel.getInputs() ) ) {
            rel = visitChild( rel, input.i, input.e );
        }
        return rel;
    }


    @Override
    public RelNode visit( LogicalAggregate aggregate ) {
        return visitChild( aggregate, 0, aggregate.getInput() );
    }


    @Override
    public RelNode visit( LogicalMatch match ) {
        return visitChild( match, 0, match.getInput() );
    }


    @Override
    public RelNode visit( TableScan scan ) {
        return scan;
    }


    @Override
    public RelNode visit( TableFunctionScan scan ) {
        return visitChildren( scan );
    }


    @Override
    public RelNode visit( LogicalValues values ) {
        return values;
    }


    @Override
    public RelNode visit( LogicalFilter filter ) {
        return visitChild( filter, 0, filter.getInput() );
    }


    @Override
    public RelNode visit( LogicalProject project ) {
        return visitChild( project, 0, project.getInput() );
    }


    @Override
    public RelNode visit( LogicalJoin join ) {
        return visitChildren( join );
    }


    @Override
    public RelNode visit( LogicalCorrelate correlate ) {
        return visitChildren( correlate );
    }


    @Override
    public RelNode visit( LogicalUnion union ) {
        return visitChildren( union );
    }


    @Override
    public RelNode visit( LogicalIntersect intersect ) {
        return visitChildren( intersect );
    }


    @Override
    public RelNode visit( LogicalMinus minus ) {
        return visitChildren( minus );
    }


    @Override
    public RelNode visit( LogicalSort sort ) {
        return visitChildren( sort );
    }


    @Override
    public RelNode visit( LogicalExchange exchange ) {
        return visitChildren( exchange );
    }


    @Override
    public RelNode visit( RelNode other ) {
        return visitChildren( other );
    }
}

