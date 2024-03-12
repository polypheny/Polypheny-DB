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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentUnwind;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;

public class LogicalDocumentUnwind extends DocumentUnwind {

    public LogicalDocumentUnwind( AlgCluster cluster, AlgTraitSet traits, String path, AlgNode node ) {
        super( cluster, traits, node, path );
    }


    public static LogicalDocumentUnwind create( String path, AlgNode node ) {
        return new LogicalDocumentUnwind( node.getCluster(), node.getTraitSet(), path, node );
    }


    @Override
    public LogicalDocumentUnwind copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalDocumentUnwind( inputs.get( 0 ).getCluster(), traitSet, path, inputs.get( 0 ) );
    }

}
