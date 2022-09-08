/*
 * Copyright 2019-2022 The Polypheny Project
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
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModelTrait;


/**
 * Sub-class of {@link Modify} not targeted at any particular engine or calling convention.
 */
public final class LogicalModify extends Modify {

    @Getter
    @Setter
    @Accessors(fluent = true)
    boolean isStreamed = false;


    /**
     * Creates a LogicalModify.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public LogicalModify( AlgOptCluster cluster, AlgTraitSet traitSet, AlgOptTable table, CatalogReader schema, AlgNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        super( cluster, traitSet.replace( ModelTrait.RELATIONAL ), table, schema, input, operation, updateColumnList, sourceExpressionList, flattened );
    }


    /**
     * Creates a LogicalModify.
     */
    public static LogicalModify create( AlgOptTable table, CatalogReader schema, AlgNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        final AlgOptCluster cluster = input.getCluster();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalModify( cluster, traitSet, table, schema, input, operation, updateColumnList, sourceExpressionList, flattened );
    }


    @Override
    public LogicalModify copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalModify( getCluster(), traitSet, table, catalogReader, sole( inputs ), getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened() ).isStreamed( isStreamed );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}

