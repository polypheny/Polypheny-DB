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
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyLong;


public class DocCommitStateFilterMod extends DeferredAlgTreeModification<LogicalDocumentFilter, LogicalDocumentFilter> {

    private static final AlgDataType BOOLEAN_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.BOOLEAN, true );
    private static final AlgDataType DOCUMENT_ALG_TYPE = new DocumentType( List.of(), List.of() );

    private final CommitState removedCommitState;


    public DocCommitStateFilterMod( CommitState removedCommitState, LogicalDocumentFilter target ) {
        super( target );
        this.removedCommitState = removedCommitState;
    }


    @Override
    public LogicalDocumentFilter apply( LogicalDocumentFilter node ) {
        Operator filterOperator = switch ( removedCommitState ) {
            case COMMITTED -> OperatorRegistry.get( QueryLanguage.from( "mql" ), OperatorName.MQL_GTE );
            case UNCOMMITTED -> OperatorRegistry.get( QueryLanguage.from( "mql" ), OperatorName.MQL_LT );
        };

        RexNode committedCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                filterOperator,
                new RexNameRef( IdentifierUtils.VERSION_KEY, null, IdentifierUtils.VERSION_ALG_TYPE ),
                new RexLiteral( PolyBigDecimal.of( 0 ), DOCUMENT_ALG_TYPE, PolyType.DOCUMENT )
        );
        RexNode newCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.AND ),
                List.of( node.getCondition(), committedCondition )
        );

        return LogicalDocumentFilter.create( node.getInput(), newCondition );
    }

}
