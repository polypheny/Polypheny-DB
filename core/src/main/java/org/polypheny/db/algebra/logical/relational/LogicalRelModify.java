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


    @Override
    public LogicalRelModify copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return (LogicalRelModify) new LogicalRelModify( getCluster(), traitSet, entity, sole( inputs ), getOperation(), getUpdateColumns(), getSourceExpressions(), isFlattened() ).streamed( streamed );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}

