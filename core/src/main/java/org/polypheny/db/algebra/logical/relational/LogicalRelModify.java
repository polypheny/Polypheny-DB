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

package org.polypheny.db.algebra.logical.relational;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.arguments.BooleanArg;
import org.polypheny.db.algebra.polyalg.arguments.EntityArg;
import org.polypheny.db.algebra.polyalg.arguments.EnumArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.RexArg;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;


/**
 * Sub-class of {@link RelModify} not targeted at any particular engine or calling convention.
 */
public final class LogicalRelModify extends RelModify<Entity> {


    /**
     * Creates a LogicalModify.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public LogicalRelModify(
            AlgCluster cluster,
            AlgTraitSet traitSet,
            Entity table,
            AlgNode input,
            Operation operation,
            List<String> updateColumns,
            List<? extends RexNode> sourceExpressions,
            boolean flattened ) {
        super( cluster, traitSet.replace( ModelTrait.RELATIONAL ), table, input, operation, updateColumns, sourceExpressions, flattened );
    }


    public LogicalRelModify(
            AlgTraitSet traits,
            Entity table,
            AlgNode child,
            Operation operation,
            List<String> targets,
            List<? extends RexNode> sources ) {
        super( child.getCluster(), traits, table, child, operation, targets, sources, false );
    }


    /**
     * Creates a LogicalModify.
     */
    public static LogicalRelModify create(
            Entity table,
            AlgNode input,
            Operation operation,
            List<String> updateColumns,
            List<? extends RexNode> sourceExpressions,
            boolean flattened ) {
        final AlgCluster cluster = input.getCluster();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalRelModify( cluster, traitSet, table, input, operation, updateColumns, sourceExpressions, flattened );
    }


    public static LogicalRelModify create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        EntityArg entity = args.getArg( "table", EntityArg.class );
        EnumArg<Operation> op = args.getEnumArg( "operation", Operation.class );
        List<String> updateColumns = args.getListArg( "targets", StringArg.class ).map( StringArg::getArg );
        List<? extends RexNode> sourceExpressions = args.getListArg( "sources", RexArg.class ).map( RexArg::getNode );
        BooleanArg flattened = args.getArg( "flattened", BooleanArg.class );

        updateColumns = updateColumns.isEmpty() ? null : updateColumns;
        sourceExpressions = sourceExpressions.isEmpty() ? null : sourceExpressions;
        return create( entity.getEntity(), children.get( 0 ), op.getArg(), updateColumns, sourceExpressions, flattened.toBool() );
    }


    @Override
    public LogicalRelModify copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return (LogicalRelModify) new LogicalRelModify( getCluster(), traitSet, entity, sole( inputs ), getOperation(), getUpdateColumns(), getSourceExpressions(), isFlattened() ).streamed( streamed );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public PolyAlgArgs collectAttributes() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );

        if ( getUpdateColumns() != null ) {
            args.put( "targets", new ListArg<>( getUpdateColumns(), StringArg::new ) );
        }
        if ( getSourceExpressions() != null ) {
            args.put( "sources", new ListArg<>( getSourceExpressions(), RexArg::new ) );
        }

        return args.put( "table", new EntityArg( entity ) )
                .put( "operation", new EnumArg<>( getOperation(), ParamType.MODIFY_OP_ENUM ) )
                .put( "flattened", new BooleanArg( isFlattened() ) );
    }

}

