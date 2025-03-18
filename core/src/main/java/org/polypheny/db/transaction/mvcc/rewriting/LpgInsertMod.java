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

package org.polypheny.db.transaction.mvcc.rewriting;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgIdentifier;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.transaction.mvcc.EntryIdentifierRegistry;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;

public class LpgInsertMod implements AlgTreeModification<LogicalLpgModify, LogicalLpgModify> {

    private final long versionIdentifier;


    public LpgInsertMod( long versionIdentifier ) {
        this.versionIdentifier = versionIdentifier;
    }


    @Override
    public LogicalLpgModify apply( LogicalLpgModify modify ) {
        AlgNode input = modify.getInput();
        if ( input instanceof LogicalLpgValues values ) {
            return rewriteUsingValues( modify, values );
        }
        return rewriteUsingIdentifier( modify );
    }


    private LogicalLpgModify rewriteUsingValues( LogicalLpgModify modify, LogicalLpgValues values ) {
        EntryIdentifierRegistry registry = Catalog.getInstance().getSnapshot().getLogicalEntity( modify.getEntity().getId() ).orElseThrow().getEntryIdentifiers();

        List<PolyEdge> edges = values.getEdges();
        List<PolyNode> nodes = values.getNodes();

        edges.forEach( edge -> {
            edge.getProperties().put( IdentifierUtils.getIdentifierKeyAsPolyString(), registry.getNextEntryIdentifier().asPolyBigDecimal() );
            edge.getProperties().put( IdentifierUtils.getVersionKeyAsPolyString(), IdentifierUtils.getVersionAsPolyBigDecimal( versionIdentifier, false ) );
        } );

        nodes.forEach( node -> {
            node.getProperties().put( IdentifierUtils.getIdentifierKeyAsPolyString(), registry.getNextEntryIdentifier().asPolyBigDecimal() );
            node.getProperties().put( IdentifierUtils.getVersionKeyAsPolyString(), IdentifierUtils.getVersionAsPolyBigDecimal( versionIdentifier, false ) );
        } );

        //LogicalLpgValues newValues = new LogicalLpgValues( values.getCluster(), values.getTraitSet(), nodes, edges,  )
        return null;
    }


    private LogicalLpgModify rewriteUsingIdentifier( LogicalLpgModify modify ) {
        LogicalLpgIdentifier identifier = LogicalLpgIdentifier.create(
                modify.getEntity(),
                modify.getInput()
        );
        return modify.copy( List.of( identifier ) );
    }

}
