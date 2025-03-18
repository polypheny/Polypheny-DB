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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.DeepCopyShuttle;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.db.transaction.mvcc.MvccUtils;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.numerical.PolyLong;

public class DocDeleteMod extends DocFieldUpdateMod<LogicalDocumentModify, LogicalDocumentModify> {

    private static final AlgDataType ANY_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.ANY, false );
    public static final AlgDataType CHAR_255_ALG_TYPE = AlgDataTypeFactoryImpl.DEFAULT.createPolyType( PolyType.CHAR, 255 );
    private static final AlgDataType DOCUMENT_ALG_TYPE = new DocumentType( List.of(), List.of() );
    private static final AlgDataType ARRAY_TYPE = new ArrayType( CHAR_255_ALG_TYPE, false );

    private final Statement statement;


    public DocDeleteMod( Statement statement ) {
        this.statement = statement;
    }


    @Override
    public LogicalDocumentModify apply( LogicalDocumentModify node ) {
        LogicalDocumentModify updatingSubtree = createDeleting( node );
        MvccUtils.executeDmlAlgTree( AlgRoot.of( updatingSubtree, Kind.UPDATE ), statement, node.getEntity().getNamespaceId() );
        return createInsertingSubtree( node );
    }


    private LogicalDocumentModify createInsertingSubtree( LogicalDocumentModify originalModify ) {
        DocSnapshotFilterRewriter filterRewriter = new DocSnapshotFilterRewriter( CommitState.COMMITTED, originalModify.getEntity() );
        AlgNode input = filterRewriter.visit( originalModify.getInput() );

        input = createIdModification( input );
        input = createVersionIdModification( input, statement );
        input = createEntryIdModification( input );

        return LogicalDocumentModify.create(
                originalModify.getEntity(),
                input,
                Operation.INSERT,
                null,
                null,
                null
        );
    }


    private LogicalDocumentProject createEntryIdModification( AlgNode node ) {
        Map<String, RexNode> eidIncludes = new HashMap<>();
        eidIncludes.put( null, new RexCall(
                        DOCUMENT_ALG_TYPE,
                        OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_ADD_FIELDS ),
                        new RexIndexRef( 0, DOCUMENT_ALG_TYPE ),
                        new RexLiteral(
                                PolyList.of( IdentifierUtils.getIdentifierKeyAsPolyString() ),
                                ARRAY_TYPE,
                                PolyType.ARRAY
                        ),
                        new RexCall(
                                ANY_ALG_TYPE,
                                OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MINUS ),
                                new RexLiteral(
                                        PolyLong.of( 0 ),
                                        DOCUMENT_ALG_TYPE,
                                        PolyType.DOCUMENT
                                ),
                                new RexNameRef(
                                        List.of( IdentifierUtils.IDENTIFIER_KEY ),
                                        null,
                                        DOCUMENT_ALG_TYPE
                                )
                        )
                )
        );

        return LogicalDocumentProject.create(
                node,
                eidIncludes
        );
    }


    private LogicalDocumentModify createDeleting( LogicalDocumentModify originalModify ) {
        originalModify = (LogicalDocumentModify) new DeepCopyShuttle().visit( originalModify );

        DocSnapshotFilterRewriter filterRewriter = new DocSnapshotFilterRewriter( CommitState.UNCOMMITTED, originalModify.getEntity() );
        AlgNode input = filterRewriter.visit( originalModify.getInput() );

        return LogicalDocumentModify.create(
                originalModify.getEntity(),
                input,
                Operation.DELETE,
                null,
                null,
                null
        );
    }

}
