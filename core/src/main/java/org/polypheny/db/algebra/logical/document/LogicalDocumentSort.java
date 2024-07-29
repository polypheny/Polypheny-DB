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
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.algebra.core.document.DocumentSort;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;


public class LogicalDocumentSort extends DocumentSort implements DocumentAlg {

    /**
     * Subclass of {@link DocumentSort} not targeted at any particular engine or calling convention.
     */
    public LogicalDocumentSort( AlgCluster cluster, AlgTraitSet traits, AlgNode child, AlgCollation collation, List<RexNode> targets, RexNode offset, RexNode fetch ) {
        super( cluster, traits, child, collation, targets, offset, fetch );
    }


    public static AlgNode create( AlgNode node, AlgCollation collation, List<RexNode> targets, RexNode offset, RexNode fetch ) {
        collation = AlgCollationTraitDef.INSTANCE.canonize( collation );
        AlgTraitSet traitSet = node.getTraitSet().replace( Convention.NONE ).replace( collation );

        return new LogicalDocumentSort( node.getCluster(), traitSet, node, collation, targets, offset, fetch );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalDocumentSort( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), collation, fieldExps, offset, fetch );
    }


    @Override
    public DocType getDocType() {
        return DocType.SORT;
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
