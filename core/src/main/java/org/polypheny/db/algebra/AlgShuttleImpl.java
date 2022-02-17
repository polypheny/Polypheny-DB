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

package org.polypheny.db.algebra;


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.core.TableFunctionScan;
import org.polypheny.db.algebra.core.TableScan;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.logical.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.LogicalCorrelate;
import org.polypheny.db.algebra.logical.LogicalExchange;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.logical.LogicalIntersect;
import org.polypheny.db.algebra.logical.LogicalJoin;
import org.polypheny.db.algebra.logical.LogicalMatch;
import org.polypheny.db.algebra.logical.LogicalMinus;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.logical.LogicalSort;
import org.polypheny.db.algebra.logical.LogicalUnion;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Basic implementation of {@link AlgShuttle} that calls {@link AlgNode#accept(AlgShuttle)} on each child, and {@link AlgNode#copy(AlgTraitSet, java.util.List)} if
 * any children change.
 */
public class AlgShuttleImpl implements AlgShuttle {

    protected final Deque<AlgNode> stack = new ArrayDeque<>();


    /**
     * Visits a particular child of a parent.
     */
    protected <T extends AlgNode> T visitChild( T parent, int i, AlgNode child ) {
        stack.push( parent );
        try {
            AlgNode child2 = child.accept( this );
            if ( child2 != child ) {
                final List<AlgNode> newInputs = new ArrayList<>( parent.getInputs() );
                newInputs.set( i, child2 );
                //noinspection unchecked
                return (T) parent.copy( parent.getTraitSet(), newInputs );
            }
            return parent;
        } finally {
            stack.pop();
        }
    }


    protected <T extends AlgNode> T visitChildren( T alg ) {
        for ( Ord<AlgNode> input : Ord.zip( alg.getInputs() ) ) {
            alg = visitChild( alg, input.i, input.e );
        }
        return alg;
    }


    @Override
    public AlgNode visit( LogicalAggregate aggregate ) {
        return visitChild( aggregate, 0, aggregate.getInput() );
    }


    @Override
    public AlgNode visit( LogicalMatch match ) {
        return visitChild( match, 0, match.getInput() );
    }


    @Override
    public AlgNode visit( TableScan scan ) {
        return scan;
    }


    @Override
    public AlgNode visit( TableFunctionScan scan ) {
        return visitChildren( scan );
    }


    @Override
    public AlgNode visit( LogicalValues values ) {
        return values;
    }


    @Override
    public AlgNode visit( LogicalFilter filter ) {
        return visitChild( filter, 0, filter.getInput() );
    }


    @Override
    public AlgNode visit( LogicalProject project ) {
        return visitChild( project, 0, project.getInput() );
    }


    @Override
    public AlgNode visit( LogicalJoin join ) {
        return visitChildren( join );
    }


    @Override
    public AlgNode visit( LogicalCorrelate correlate ) {
        return visitChildren( correlate );
    }


    @Override
    public AlgNode visit( LogicalUnion union ) {
        return visitChildren( union );
    }


    @Override
    public AlgNode visit( LogicalIntersect intersect ) {
        return visitChildren( intersect );
    }


    @Override
    public AlgNode visit( LogicalMinus minus ) {
        return visitChildren( minus );
    }


    @Override
    public AlgNode visit( LogicalSort sort ) {
        return visitChildren( sort );
    }


    @Override
    public AlgNode visit( LogicalExchange exchange ) {
        return visitChildren( exchange );
    }


    @Override
    public AlgNode visit( LogicalConditionalExecute lce ) {
        return visitChildren( lce );
    }


    @Override
    public AlgNode visit( LogicalConstraintEnforcer enforcer ) {
        return visitChildren( enforcer );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        return visitChildren( other );
    }

}

