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

import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;

public class RelCommitStateFilterMod extends DeferredAlgTreeModification<LogicalRelFilter, LogicalRelFilter> {
    private static final AlgDataType BOOLEAN_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.BOOLEAN, true );
    private final CommitState removedCommitState;

    public enum CommitState {
        UNCOMMITTED,
        COMMITTED
    }

    public RelCommitStateFilterMod(CommitState removedCommitState, LogicalRelFilter target, Statement statement) {
        super(target, statement);
        this.removedCommitState = removedCommitState;
    }

    @Override
    public LogicalRelFilter apply( LogicalRelFilter node ) {
        Operator filterOperator = switch(removedCommitState) {
            case COMMITTED -> OperatorRegistry.get(OperatorName.GREATER_THAN_OR_EQUAL);
            case UNCOMMITTED -> OperatorRegistry.get(OperatorName.LESS_THAN);
        };

        RexCall commitStateCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                filterOperator,
                new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE ),
                new RexLiteral(
                        PolyBigDecimal.of( 0 ),
                        IdentifierUtils.VERSION_ALG_TYPE,
                        IdentifierUtils.VERSION_ALG_TYPE.getPolyType()
                )
        );

        RexCall extendedCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.AND ),
                node.getCondition(),
                commitStateCondition
        );

        return node.copy( node.getTraitSet(), node.getInput(), extendedCondition );
    }

}
