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
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.document.DocumentFilter;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;

@EqualsAndHashCode(callSuper = true)
@Value
public class LogicalDocumentFilter extends DocumentFilter {

    /**
     * Subclass of {@link DocumentFilter} not targeted at any particular engine or calling convention.
     */
    public LogicalDocumentFilter( AlgCluster cluster, AlgTraitSet traits, AlgNode input, RexNode condition ) {
        super( cluster, traits, input, condition );
    }


    public static LogicalDocumentFilter create( AlgNode node, RexNode condition ) {
        return new LogicalDocumentFilter( node.getCluster(), node.getTraitSet(), node, condition );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, inputs.get( 0 ), condition );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    protected AlgNode copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return new LogicalDocumentFilter( input.getCluster(), traitSet, input, condition );
    }


    @Override
    public String toString() {
        return "LogicalDocumentFilter{" +
                "digest='" + digest + '\'' +
                '}';
    }

}
