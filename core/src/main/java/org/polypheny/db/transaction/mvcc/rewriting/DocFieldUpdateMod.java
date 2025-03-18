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
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.BsonUtil;

public abstract class DocFieldUpdateMod<T, U extends AlgNode> implements AlgTreeModification<T, U> {

    protected static final AlgDataType CHAR_255_ALG_TYPE = AlgDataTypeFactoryImpl.DEFAULT.createPolyType( PolyType.CHAR, 255 );
    protected static final AlgDataType DOCUMENT_ALG_TYPE = new DocumentType( List.of(), List.of() );
    protected static final AlgDataType ARRAY_TYPE = new ArrayType( CHAR_255_ALG_TYPE, false );


    protected LogicalDocumentProject createIdModification( AlgNode node ) {
        Map<String, RexNode> idIncludes = new HashMap<>();
        idIncludes.put( null, new RexCall(
                        DOCUMENT_ALG_TYPE,
                        OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_ADD_FIELDS ),
                        new RexIndexRef( 0, DOCUMENT_ALG_TYPE ),
                        new RexLiteral(
                                PolyList.of( PolyString.of( "_id" ) ),
                                ARRAY_TYPE,
                                PolyType.ARRAY
                        ),
                        new RexLiteral(
                                PolyString.of( BsonUtil.getObjectId() ),
                                DOCUMENT_ALG_TYPE,
                                PolyType.DOCUMENT
                        )
                )
        );

        return LogicalDocumentProject.create(
                node,
                idIncludes
        );
    }


    protected LogicalDocumentProject createVersionIdModification( AlgNode node, Statement statement ) {
        Map<String, RexNode> vidIncludes = new HashMap<>();
        vidIncludes.put( null, new RexCall(
                        DOCUMENT_ALG_TYPE,
                        OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_ADD_FIELDS ),
                        new RexIndexRef( 0, DOCUMENT_ALG_TYPE ),
                        new RexLiteral(
                                PolyList.of( IdentifierUtils.getVersionKeyAsPolyString() ),
                                ARRAY_TYPE,
                                PolyType.ARRAY
                        ),
                        new RexLiteral(
                                IdentifierUtils.getVersionAsPolyLong( statement.getTransaction().getSequenceNumber(), false ),
                                DOCUMENT_ALG_TYPE,
                                PolyType.DOCUMENT
                        )
                )
        );

        return LogicalDocumentProject.create(
                node,
                vidIncludes
        );
    }

}
