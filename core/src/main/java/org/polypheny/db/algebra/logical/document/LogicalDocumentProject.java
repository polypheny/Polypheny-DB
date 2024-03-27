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
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.document.DocumentProject;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;


public class LogicalDocumentProject extends DocumentProject {

    /**
     * Subclass of {@link DocumentProject} not targeted at any particular engine or calling convention.
     */
    public LogicalDocumentProject( AlgCluster cluster, AlgTraitSet traits, AlgNode input, Map<String, ? extends RexNode> includes, List<String> excludes ) {
        super( cluster, traits, input, includes, excludes );
    }


    public static LogicalDocumentProject create( AlgNode node, Map<String, RexNode> includes, List<String> excludes ) {
        return new LogicalDocumentProject( node.getCluster(), node.getTraitSet(), node, includes, excludes );
    }


    public static LogicalDocumentProject create( AlgNode node, List<RexNode> includes, List<String> includesName ) {
        return create( node, Pair.zip( includesName, includes ).stream().collect( Collectors.toMap( e -> e.left, e -> e.right ) ), List.of() );
    }


    @Override
    public LogicalDocumentProject copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalDocumentProject( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), includes, excludes );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
