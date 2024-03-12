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

package org.polypheny.db.algebra.logical.lpg;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;


public class LogicalLpgModify extends LpgModify<Entity> implements RelationalTransformable {


    /**
     * Subclass of {@link LpgModify} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgModify( AlgCluster cluster, AlgTraitSet traits, Entity entity, AlgNode input, Operation operation, List<PolyString> ids, List<? extends RexNode> operations ) {
        super( cluster, traits, entity, input, operation, ids, operations, AlgOptUtil.createDmlRowType( Kind.INSERT, cluster.getTypeFactory() ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalLpgModify( inputs.get( 0 ).getCluster(), traitSet, entity, inputs.get( 0 ), operation, ids, operations );
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> inputs, List<Entity> entities, Snapshot snapshot ) {
        List<AlgNode> modifies = new ArrayList<>();

        // modify of nodes
        Modify<?> nodeModify = RelationalTransformable.getModify( entities.get( 0 ), inputs.get( 0 ), operation );
        modifies.add( nodeModify );

        // modify of properties
        if ( inputs.get( 1 ) != null ) {
            Modify<?> nodePropertyModify = RelationalTransformable.getModify( entities.get( 1 ), inputs.get( 1 ), operation );
            modifies.add( nodePropertyModify );
        }

        if ( inputs.size() == 2 ) {
            return modifies;
        }

        // modify of edges
        Modify<?> edgeModify = RelationalTransformable.getModify( entities.get( 2 ), inputs.get( 2 ), operation );
        modifies.add( edgeModify );

        // modify of edge properties
        if ( inputs.get( 3 ) != null ) {
            Modify<?> edgePropertyModify = RelationalTransformable.getModify( entities.get( 3 ), inputs.get( 3 ), operation );
            modifies.add( edgePropertyModify );
        }

        return modifies;
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
