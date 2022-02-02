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
 */

package org.polypheny.db.processing;

import com.google.common.collect.ImmutableList;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.TableFunctionScan;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.logical.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.LogicalCorrelate;
import org.polypheny.db.algebra.logical.LogicalExchange;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.logical.LogicalIntersect;
import org.polypheny.db.algebra.logical.LogicalJoin;
import org.polypheny.db.algebra.logical.LogicalMatch;
import org.polypheny.db.algebra.logical.LogicalMinus;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.logical.LogicalScan;
import org.polypheny.db.algebra.logical.LogicalSort;
import org.polypheny.db.algebra.logical.LogicalUnion;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.plan.AlgTraitSet;

/**
 * Shuttle for creating a deep copy of a query plan.
 */
public class DeepCopyShuttle extends AlgShuttleImpl {

    private AlgTraitSet copy( final AlgTraitSet other ) {
        return AlgTraitSet.createEmpty().merge( other );
    }


    @Override
    public AlgNode visit( Scan scan ) {
        final AlgNode node = super.visit( scan );
        return new LogicalScan( node.getCluster(), copy( node.getTraitSet() ), node.getTable() );
    }


    @Override
    public AlgNode visit( TableFunctionScan scan ) {
        final AlgNode node = super.visit( scan );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalValues values ) {
        final Values node = (Values) super.visit( values );
        return new LogicalValues( node.getCluster(), copy( node.getTraitSet() ), node.getRowType(), node.getTuples() );
    }


    @Override
    public AlgNode visit( LogicalFilter filter ) {
        final LogicalFilter node = (LogicalFilter) super.visit( filter );
        return new LogicalFilter(
                node.getCluster(),
                copy( node.getTraitSet() ),
                node.getInput().accept( this ),
                node.getCondition(),
                node.getVariablesSet() );
    }


    @Override
    public AlgNode visit( LogicalProject project ) {
        final Project node = (Project) super.visit( project );
        return new LogicalProject(
                node.getCluster(),
                copy( node.getTraitSet() ),
                node.getInput(),
                node.getProjects(),
                node.getRowType() );
    }


    @Override
    public AlgNode visit( LogicalJoin join ) {
        final AlgNode node = super.visit( join );
        return new LogicalJoin(
                node.getCluster(),
                copy( node.getTraitSet() ),
                this.visit( join.getLeft() ),
                this.visit( join.getRight() ),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType(),
                join.isSemiJoinDone(),
                ImmutableList.copyOf( join.getSystemFieldList() ) );
    }


    @Override
    public AlgNode visit( LogicalCorrelate correlate ) {
        final AlgNode node = super.visit( correlate );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalUnion union ) {
        final AlgNode node = super.visit( union );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalIntersect intersect ) {
        final AlgNode node = super.visit( intersect );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalMinus minus ) {
        final AlgNode node = super.visit( minus );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalAggregate aggregate ) {
        final AlgNode node = super.visit( aggregate );
        return new LogicalAggregate(
                node.getCluster(),
                copy( node.getTraitSet() ),
                visit( aggregate.getInput() ),
                aggregate.indicator,
                aggregate.getGroupSet(),
                aggregate.groupSets,
                aggregate.getAggCallList() );
    }


    @Override
    public AlgNode visit( LogicalMatch match ) {
        final AlgNode node = super.visit( match );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalSort sort ) {
        final AlgNode node = super.visit( sort );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalExchange exchange ) {
        final AlgNode node = super.visit( exchange );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public AlgNode visit( LogicalConditionalExecute lce ) {
        return new LogicalConditionalExecute(
                lce.getCluster(),
                copy( lce.getTraitSet() ),
                visit( lce.getLeft() ),
                visit( lce.getRight() ),
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
