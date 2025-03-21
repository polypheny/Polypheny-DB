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

import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.db.transaction.mvcc.MvccUtils;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;

public class AlgTreeRewriter extends AlgModifyingShuttle {

    private final Statement statement;
    private boolean includesMvccEntities = false;

    public AlgTreeRewriter( Statement statement ) {
        this.statement = statement;
    }

    public AlgRoot process( AlgRoot root ) {
        AlgNode rootAlg = root.alg.accept( this );

        if ( !pendingModifications.isEmpty() ) {
            Iterator<DeferredAlgTreeModification> iterator = pendingModifications.iterator();
            while ( iterator.hasNext() ) {
                DeferredAlgTreeModification modification = iterator.next();
                if ( modification.notTargets( rootAlg ) ) {
                    continue;
                }
                rootAlg = modification.apply( rootAlg );
                iterator.remove();
            }
        }

        if ( !pendingModifications.isEmpty() ) {
            throw new IllegalStateException( "No pending tree modifications must be left on root level." );
        }

        if ( !(rootAlg instanceof Modify<?>) && includesMvccEntities ) {
            rootAlg = switch ( rootAlg.getModel() ) {
                case RELATIONAL -> new RelMvccResultProjectionMod().apply( rootAlg );
                case DOCUMENT -> new DocMvccResultProjectionMod().apply( rootAlg );
                case GRAPH -> throw new NotImplementedException( "MVCC graph selects not supported yet" );
            };
        }



        Kind kind = switch ( root.kind ) {
            case UPDATE, DELETE -> Kind.INSERT;
            default -> root.kind;
        };
        return root.withAlg( rootAlg ).withKind( kind );
    }


    @Override
    public AlgNode visit( LogicalRelScan scan ) {
        if ( MvccUtils.isInNamespaceUsingMvcc( scan.getEntity() ) ) {
            pendingModifications.add( new RelScanSnapshotMod( scan, statement ) );
            includesMvccEntities = true;
        }
        return scan;
    }


    @Override
    public AlgNode visit( LogicalRelValues values ) {
        // check for identifier not needed as this is done during sql validation
        return values;
    }


    @Override
    public AlgNode visit( LogicalRelModify modify ) {

        LogicalRelModify modify1 = visitChildren( modify );

        if ( modify1.getOperation() == Operation.UPDATE ) {
            IdentifierUtils.throwIfContainsDisallowedFieldName( new HashSet<>( modify.getUpdateColumns() ) );
        }

        if ( !MvccUtils.isInNamespaceUsingMvcc( modify1.getEntity() ) ) {
            return modify1;
        }

        statement.getTransaction().addWrittenEntitiy( modify1.getEntity() );

        return switch ( modify1.getOperation() ) {
            case INSERT -> new RelInsertMod().apply( modify1 );
            case UPDATE -> new RelUpdateMod( statement ).apply( modify1 );
            case DELETE -> new RelDeleteMod( statement ).apply( modify1 );
            default -> modify1;
        };
    }


    @Override
    public AlgNode visit( LogicalLpgModify modify ) {

        LogicalLpgModify modify1 = visitChildren( modify );

        if ( !MvccUtils.isInNamespaceUsingMvcc( modify.getEntity() ) ) {
            return modify1;
        }

        statement.getTransaction().addWrittenEntitiy( modify.getEntity() );

        return switch ( modify1.getOperation() ) {
            case INSERT -> new LpgInsertMod( statement.getTransaction().getSequenceNumber() ).apply( modify1 );
            //case UPDATE -> new LpgUpdateMod( statement ).apply( modify1 );
            //case DELETE -> new LpgDeleteMod( statement ).apply( modify1 );
            default -> modify1;
        };
    }


    @Override
    public AlgNode visit( LogicalLpgScan scan ) {
        if ( MvccUtils.isInNamespaceUsingMvcc( scan.getEntity() ) ) {
            //pendingModifications.add( new DeferredAlgTreeModification( scan, Modification.LIMIT_LPG_SCAN_TO_SNAPSHOT, statement ) );
            includesMvccEntities = true;
        }
        return scan;
    }


    @Override
    public AlgNode visit( LogicalLpgValues values ) {
        for ( PolyNode node : values.getNodes() ) {
            IdentifierUtils.throwIfContainsDisallowedFieldName( node.getProperties() );
        }
        for ( PolyEdge edge : values.getEdges() ) {
            IdentifierUtils.throwIfContainsDisallowedFieldName( edge.getProperties() );
        }
        return values;
    }


    @Override
    public AlgNode visit( LogicalDocumentModify modify ) {

        LogicalDocumentModify modify1 = visitChildren( modify );

        if ( !MvccUtils.isInNamespaceUsingMvcc( modify.getEntity() ) ) {
            return modify1;
        }

        Stream.concat(
                Stream.concat(
                        modify1.getRenames().keySet().stream(),
                        modify1.getRemoves().stream()
                ),
                modify1.getUpdates().keySet().stream()
        ).forEach( IdentifierUtils::throwIfIsDisallowedFieldName );

        statement.getTransaction().addWrittenEntitiy( modify.getEntity() );

        return switch ( modify1.getOperation() ) {
            case INSERT -> new DocInsertMod( statement.getTransaction().getSequenceNumber() ).apply( modify1 );
            case UPDATE -> new DocUpdateMod( statement ).apply( modify1 );
            case DELETE -> new DocDeleteMod( statement ).apply( modify1 );
            default -> modify1;
        };
    }


    @Override
    public AlgNode visit( LogicalDocumentScan scan ) {
        if ( MvccUtils.isInNamespaceUsingMvcc( scan.getEntity() ) ) {
            pendingModifications.add( new DocScanSnapshotMod( scan, statement ) );
            includesMvccEntities = true;
        }
        return scan;
    }


    @Override
    public AlgNode visit( LogicalDocumentValues values ) {
        IdentifierUtils.throwIfContainsDisallowedFieldName( values.getDocuments() );
        return values;
    }


}
