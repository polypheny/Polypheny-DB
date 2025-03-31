/*
 * Copyright 2019-2025 The Polypheny Project
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
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.arguments.EntityArg;
import org.polypheny.db.algebra.polyalg.arguments.EnumArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;


public class LogicalLpgModify extends LpgModify<Entity> implements RelationalTransformable {


    /**
     * Subclass of {@link LpgModify} not targeted at any particular engine or calling convention.
     */
    public LogicalLpgModify( AlgCluster cluster, AlgTraitSet traits, Entity entity, AlgNode input, Operation operation, List<PolyString> ids, List<? extends RexNode> operations ) {
        super( cluster, traits, entity, input, operation, ids, operations, AlgOptUtil.createDmlRowType( Kind.INSERT, cluster.getTypeFactory() ) );
    }


    public static LogicalLpgModify create( AlgNode input, Entity entity, Operation operation, List<PolyString> ids, List<? extends RexNode> operations ) {
        final AlgCluster cluster = input.getCluster();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalLpgModify( cluster, traitSet, entity, input, operation, ids, operations );
    }


    public static LogicalLpgModify create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        EntityArg entity = args.getArg( "entity", EntityArg.class );
        EnumArg<Operation> op = args.getEnumArg( "operation", Operation.class );
        List<PolyString> ids = args.getListArg( "ids", StringArg.class ).map( s -> PolyString.of( s.getArg() ) );
        List<? extends RexNode> operations = args.getListArg( "updates", RexArg.class ).map( RexArg::getNode );
        return create( children.get( 0 ), entity.getEntity(), op.getArg(), ids, operations );
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


    @Override
    public PolyAlgArgs bindArguments() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );

        args.put( "entity", new EntityArg( entity, Catalog.snapshot(), DataModel.GRAPH ) )
                .put( "operation", new EnumArg<>( getOperation(), ParamType.MODIFY_OP_ENUM ) );
        if ( ids != null ) {
            args.put( "ids", new ListArg<>( ids, s -> new StringArg( s.value ) ) );
        }
        if ( operations != null ) {
            args.put( "updates", new ListArg<>( operations, RexArg::new ) );
        }

        return args;
    }

}
