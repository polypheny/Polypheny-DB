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
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.TableFunctionScan;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalConditionalExecute;
import org.polypheny.db.rel.logical.LogicalCorrelate;
import org.polypheny.db.rel.logical.LogicalExchange;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalIntersect;
import org.polypheny.db.rel.logical.LogicalJoin;
import org.polypheny.db.rel.logical.LogicalMatch;
import org.polypheny.db.rel.logical.LogicalMinus;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalSort;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalUnion;
import org.polypheny.db.rel.logical.LogicalValues;

public class RelDeepCopyShuttle extends RelShuttleImpl {

    private RelTraitSet copy( final RelTraitSet other ) {
        return RelTraitSet.createEmpty().merge( other );
    }


    @Override
    public RelNode visit( TableScan scan ) {
        final RelNode node = super.visit( scan );
        return new LogicalTableScan( node.getCluster(), copy( node.getTraitSet() ), node.getTable() );
    }


    @Override
    public RelNode visit( TableFunctionScan scan ) {
        final RelNode node = super.visit( scan );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public RelNode visit( LogicalValues values ) {
        final Values node = (Values) super.visit( values );
        return new LogicalValues( node.getCluster(), copy( node.getTraitSet() ), node.getRowType(), node.getTuples() );
    }


    @Override
    public RelNode visit( LogicalFilter filter ) {
        final LogicalFilter node = (LogicalFilter) super.visit( filter );
        return new LogicalFilter( node.getCluster(), copy( node.getTraitSet() ), node.getInput().accept( this ), node.getCondition(), node.getVariablesSet() );
    }


    @Override
    public RelNode visit( LogicalProject project ) {
        final Project node = (Project) super.visit( project );
        return new LogicalProject( node.getCluster(), copy( node.getTraitSet() ), node.getInput(), node.getProjects(), node.getRowType() );
    }


    @Override
    public RelNode visit( LogicalJoin join ) {
        final RelNode node = super.visit( join );
        return new LogicalJoin( node.getCluster(), copy( node.getTraitSet() ), this.visit( join.getLeft() ), this.visit( join.getRight() ), join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(), ImmutableList.copyOf( join.getSystemFieldList() ) );
    }


    @Override
    public RelNode visit( LogicalCorrelate correlate ) {
        final RelNode node = super.visit( correlate );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public RelNode visit( LogicalUnion union ) {
        final RelNode node = super.visit( union );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public RelNode visit( LogicalIntersect intersect ) {
        final RelNode node = super.visit( intersect );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public RelNode visit( LogicalMinus minus ) {
        final RelNode node = super.visit( minus );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public RelNode visit( LogicalAggregate aggregate ) {
        final RelNode node = super.visit( aggregate );
        return new LogicalAggregate( node.getCluster(), copy( node.getTraitSet() ), visit( aggregate.getInput() ), aggregate.indicator, aggregate.getGroupSet(), aggregate.groupSets, aggregate.getAggCallList() );
    }


    @Override
    public RelNode visit( LogicalMatch match ) {
        final RelNode node = super.visit( match );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public RelNode visit( LogicalSort sort ) {
        final RelNode node = super.visit( sort );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public RelNode visit( LogicalExchange exchange ) {
        final RelNode node = super.visit( exchange );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }


    @Override
    public RelNode visit( LogicalConditionalExecute lce ) {
        return new LogicalConditionalExecute( lce.getCluster(), copy( lce.getTraitSet() ), visit( lce.getLeft() ), visit( lce.getRight() ), lce.getCondition(), lce.getExceptionClass(), lce.getExceptionMessage() );
    }


    @Override
    public RelNode visit( RelNode other ) {
        final RelNode node = super.visit( other );
        return node.copy( copy( node.getTraitSet() ), node.getInputs() );
    }

}
