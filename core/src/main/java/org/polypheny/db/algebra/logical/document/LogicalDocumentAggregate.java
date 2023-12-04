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

package org.polypheny.db.algebra.logical.document;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.DocumentAggregateCall;
import org.polypheny.db.algebra.core.document.DocumentAggregate;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;


public class LogicalDocumentAggregate extends DocumentAggregate {

    /**
     * Subclass of {@link DocumentAggregate} not targeted at any particular engine or calling convention.
     */
    protected LogicalDocumentAggregate( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, RexNode group, List<DocumentAggregateCall> aggCalls ) {
        super( cluster, traits, child, group, aggCalls );
    }


    /**
     * Creates a LogicalAggregate.
     */
    public static LogicalDocumentAggregate create( final AlgNode input, RexNode group, List<DocumentAggregateCall> aggCalls ) {
        return create_( input, group, aggCalls );
    }


    private static LogicalDocumentAggregate create_( final AlgNode input, RexNode group, List<DocumentAggregateCall> aggCalls ) {
        final AlgOptCluster cluster = input.getCluster();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalDocumentAggregate( cluster, traitSet, input, group, aggCalls );
    }


    @Override
    protected AlgDataType deriveRowType() {
        AlgDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        builder.add( "_id", null, DocumentType.ofDoc() );

        for ( DocumentAggregateCall aggCall : aggCalls ) {
            builder.add( aggCall.name, null, DocumentType.ofDoc() );
        }

        return builder.build();
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalDocumentAggregate( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), group, aggCalls );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
