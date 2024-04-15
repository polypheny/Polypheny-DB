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

package org.polypheny.db.algebra.logical.document;

import java.util.List;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.polyalg.arguments.EntityArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;

@SuperBuilder(toBuilder = true)
public class LogicalDocumentScan extends DocumentScan<Entity> implements RelationalTransformable {

    /**
     * Subclass of {@link DocumentScan} not targeted at any particular engine or calling convention.
     */
    public LogicalDocumentScan( AlgCluster cluster, AlgTraitSet traitSet, Entity document ) {
        super( cluster, traitSet.replace( ModelTrait.DOCUMENT ), document );
    }


    public static AlgNode create( AlgCluster cluster, Entity collection ) {
        return new LogicalDocumentScan( cluster, cluster.traitSet(), collection );
    }


    public static AlgNode create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        return create( cluster, args.getArg( 0, EntityArg.class ).getEntity() );
    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<Entity> entities, Snapshot snapshot ) {
        return List.of( AlgOptRule.convert( LogicalRelScan.create( getCluster(), entities.get( 0 ) ), ModelTrait.RELATIONAL ) );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public PolyAlgArgs collectAttributes() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );
        return args.put( 0, new EntityArg( entity ) );
    }

}
