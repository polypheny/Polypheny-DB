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
import org.polypheny.db.ResultIterator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.db.transaction.mvcc.MvccUtils;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyLong;

public class DocScanSnapshotMod extends DeferredAlgTreeModification<LogicalDocumentScan, LogicalDocumentFilter> {

    private static final PolyString MQL_IDENTIFIER_KEY = PolyString.of( "_id" );
    private static final AlgDataType BOOLEAN_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.BOOLEAN, true );
    private static final AlgDataType DOCUMENT_ALG_TYPE = new DocumentType( List.of(), List.of() );


    protected DocScanSnapshotMod( LogicalDocumentScan target, Statement statement ) {
        super( target, statement );
    }


    @Override
    public LogicalDocumentFilter apply( LogicalDocumentScan node ) {
        HashMap<Long, Long> documents = getDocumentsInScope( node );
        return buildDocScopeFilter( documents, node );
    }


    private HashMap<Long, Long> getDocumentsInScope( AlgNode node ) {
        String queryTemplate = """
                db.%s.aggregate([
                {
                    "$match": {
                    "$or": [
                        { "_vid": { "$gt": 0, "$lt": %d } },
                        { "_vid": %d }
                    ]
                    }
                },
                {
                    "$group": {
                    "_id": "$_eid",
                    "_vid": { "$max": "$_vid" }
                    }
                }
                ]);
                """;

        Statement localStatement = statement.orElseThrow();
        String query = String.format(
                queryTemplate,
                node.getEntity().getName(),
                localStatement.getTransaction().getSequenceNumber(),
                -localStatement.getTransaction().getSequenceNumber() );
        List<List<PolyValue>> res;
        HashMap<Long, Long> documents = new HashMap<>();

        // ToDo TH: Optimization - Replace this with something more efficient once $abs works properly in mql

        try ( ResultIterator iterator = MvccUtils.executeStatement( QueryLanguage.from( "mql" ), query, node.getEntity().getNamespaceId(), localStatement ).getIterator() ) {
            res = iterator.getNextBatch();
            res.forEach( r -> {
                PolyDocument document = r.get( 0 ).asDocument();

                long eid = document.get( MQL_IDENTIFIER_KEY ).asBigDecimal().longValue();
                if ( eid < 0 ) {
                    eid = -eid;
                }

                long newVid = document.get( IdentifierUtils.getVersionKeyAsPolyString() ).asBigDecimal().longValue();

                documents.compute( eid, ( key, existingVersion ) ->
                        (existingVersion == null) ? newVid : Math.max( existingVersion, newVid )
                );
            } );
        }
        return documents;
    }


    private LogicalDocumentFilter buildDocScopeFilter( HashMap<Long, Long> documents, AlgNode input ) {
        List<RexCall> documentConditions = documents.entrySet().stream()
                .map( d -> new RexCall(
                                BOOLEAN_ALG_TYPE,
                                OperatorRegistry.get( OperatorName.AND ),
                                new RexCall(
                                        BOOLEAN_ALG_TYPE,
                                        OperatorRegistry.get( QueryLanguage.from( "mql" ), OperatorName.MQL_EQUALS ),
                                        new RexNameRef( IdentifierUtils.IDENTIFIER_KEY, null, IdentifierUtils.IDENTIFIER_ALG_TYPE ),
                                        new RexLiteral( PolyLong.of( d.getKey() ), DOCUMENT_ALG_TYPE, PolyType.DOCUMENT )
                                ),
                                new RexCall(
                                        BOOLEAN_ALG_TYPE,
                                        OperatorRegistry.get( QueryLanguage.from( "mql" ), OperatorName.MQL_EQUALS ),
                                        new RexNameRef( IdentifierUtils.VERSION_KEY, null, IdentifierUtils.VERSION_ALG_TYPE ),
                                        new RexLiteral( PolyLong.of( d.getValue() ), DOCUMENT_ALG_TYPE, PolyType.DOCUMENT )

                                )
                        )
                )
                .toList();

        RexNode condition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.OR ),
                documentConditions
        );
        return LogicalDocumentFilter.create( input, condition );
    }
}
