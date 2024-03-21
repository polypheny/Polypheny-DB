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
 */

package org.polypheny.db.processing;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelMatch;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.plan.AlgTraitSet;

/**
 * Shuttle for creating a deep copy of a query plan.
 */
public class DeepCopyShuttle extends AlgShuttleImpl {

    private AlgTraitSet copy( final AlgTraitSet other ) {
        return AlgTraitSet.createEmpty().merge( other );
    }


    @Override
    public AlgNode visit( LogicalRelScan scan ) {
        final AlgNode node = super.visit( scan );
        return new LogicalRelScan( node.getCluster(), copy( node.getTraitSet() ), node.getEntity() );
    }


    @Override
    public AlgNode visit( LogicalRelTableFunctionScan scan ) {
        final AlgNode node = super.visit( scan );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalRelValues values ) {
        final Values node = (Values) super.visit( values );
        return new LogicalRelValues( node.getCluster(), copy( node.getTraitSet() ), node.getTupleType(), node.getTuples() );
    }


    @Override
    public AlgNode visit( LogicalRelFilter filter ) {
        final LogicalRelFilter node = (LogicalRelFilter) super.visit( filter );
        return new LogicalRelFilter(
                node.getCluster(),
                copy( node.getTraitSet() ),
                node.getInput().accept( this ),
                node.getCondition(),
                node.getVariablesSet() );
    }


    @Override
    public AlgNode visit( LogicalRelProject project ) {
        final Project node = (Project) super.visit( project );
        return new LogicalRelProject(
                node.getCluster(),
                copy( node.getTraitSet() ),
                node.getInput(),
                node.getProjects(),
                node.getTupleType() );
    }


    @Override
    public AlgNode visit( LogicalRelJoin join ) {
        final AlgNode node = super.visit( join );
        return new LogicalRelJoin(
                node.getCluster(),
                copy( node.getTraitSet() ),
                super.visit( join.getLeft() ),
                super.visit( join.getRight() ),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType(),
                join.isSemiJoinDone() );
    }


    @Override
    public AlgNode visit( LogicalRelCorrelate correlate ) {
        final AlgNode node = super.visit( correlate );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalRelUnion union ) {
        final AlgNode node = super.visit( union );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalRelIntersect intersect ) {
        final AlgNode node = super.visit( intersect );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalRelMinus minus ) {
        final AlgNode node = super.visit( minus );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalRelAggregate aggregate ) {
        final AlgNode node = super.visit( aggregate );
        return new LogicalRelAggregate(
                node.getCluster(),
                copy( node.getTraitSet() ),
                super.visit( aggregate.getInput() ),
                aggregate.indicator,
                aggregate.getGroupSet(),
                aggregate.groupSets,
                aggregate.getAggCallList() );
    }


    @Override
    public AlgNode visit( LogicalRelMatch match ) {
        final AlgNode node = super.visit( match );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalRelSort sort ) {
        final AlgNode node = super.visit( sort );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalRelExchange exchange ) {
        final AlgNode node = super.visit( exchange );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalConditionalExecute lce ) {
        return new LogicalConditionalExecute(
                lce.getCluster(),
                copy( lce.getTraitSet() ),
                super.visit( lce.getLeft() ),
                super.visit( lce.getRight() ),
                lce.getCondition(),
                lce.getExceptionClass(),
                lce.getExceptionMessage() );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        final AlgNode node = super.visit( other );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }

}
