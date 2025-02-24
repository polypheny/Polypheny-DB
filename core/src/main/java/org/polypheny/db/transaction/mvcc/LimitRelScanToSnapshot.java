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

package org.polypheny.db.transaction.mvcc;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.util.ImmutableBitSet;

public class LimitRelScanToSnapshot extends DeferredAlgTreeModification<LogicalRelScan, LogicalRelJoin> {

    private static final AlgDataType BOOLEAN_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.BOOLEAN, true );


    protected LimitRelScanToSnapshot( LogicalRelScan target, Statement statement ) {
        super( target, statement );
    }


    public LogicalRelJoin apply( LogicalRelScan node ) {
        LogicalRelFilter relevantVersionsFilter = buildRelRelevantVersionsFilter( target );
        LogicalRelProject identifierVersionProject = buildRelIdentifierVersionProject( relevantVersionsFilter );
        LogicalRelAggregate newestVersionAggregate = buildNewestVersionAggregate( identifierVersionProject );
        LogicalRelFilter deletedEntriesFilter = buildDeletedEntriesFilter( target );
        return buildScopeScanJoin( deletedEntriesFilter, newestVersionAggregate );
    }


    private LogicalRelFilter buildRelRelevantVersionsFilter( AlgNode input ) {
        RexCall selfReadCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.EQUALS ),
                new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE ),
                new RexLiteral(
                        IdentifierUtils.getVersionAsPolyBigDecimal( statement.getTransaction().getSequenceNumber(), false ),
                        IdentifierUtils.VERSION_ALG_TYPE,
                        PolyType.BIGINT
                )
        );

        RexCall versionInSnapshotCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ),
                new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE ),
                new RexLiteral(
                        IdentifierUtils.getVersionAsPolyBigDecimal( statement.getTransaction().getSequenceNumber(), true ),
                        IdentifierUtils.VERSION_ALG_TYPE,
                        PolyType.BIGINT
                )
        );

        RexCall versionCommittedCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ),
                new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE ),
                new RexLiteral(
                        PolyBigDecimal.of( 0 ),
                        IdentifierUtils.VERSION_ALG_TYPE,
                        PolyType.BIGINT
                )
        );

        RexCall scopeCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.OR ),
                selfReadCondition,
                new RexCall(
                        BOOLEAN_ALG_TYPE,
                        OperatorRegistry.get( OperatorName.AND ),
                        versionInSnapshotCondition,
                        versionCommittedCondition
                )
        );

        return LogicalRelFilter.create(
                input,
                scopeCondition,
                ImmutableSet.of()
        );
    }


    private LogicalRelProject buildRelIdentifierVersionProject( LogicalRelFilter input ) {
        return LogicalRelProject.create(
                input,
                List.of(
                        new RexCall(
                                IdentifierUtils.IDENTIFIER_ALG_TYPE,
                                OperatorRegistry.get( OperatorName.ABS ),
                                new RexIndexRef( 0, IdentifierUtils.IDENTIFIER_ALG_TYPE )
                        ),
                        new RexCall(
                                IdentifierUtils.VERSION_ALG_TYPE,
                                OperatorRegistry.get( OperatorName.ABS ),
                                new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE )
                        )
                ),
                List.of(
                        IdentifierUtils.IDENTIFIER_KEY,
                        IdentifierUtils.VERSION_KEY
                )
        );
    }


    private LogicalRelAggregate buildNewestVersionAggregate( LogicalRelProject input ) {
        ImmutableBitSet rightAggregateGroupSet = ImmutableBitSet.of( 0 );
        AggregateCall rightAggregateCall = AggregateCall.create(
                OperatorRegistry.getAgg( OperatorName.MAX ),
                false,
                false,
                List.of( 1 ),
                -1,
                AlgCollations.EMPTY,
                IdentifierUtils.VERSION_ALG_TYPE,
                "max_vid"
        );

        return LogicalRelAggregate.create(
                input,
                rightAggregateGroupSet,
                List.of( rightAggregateGroupSet ),
                List.of( rightAggregateCall )
        );
    }


    private LogicalRelJoin buildScopeScanJoin( AlgNode left, LogicalRelAggregate right ) {
        int leftColumnCount = left.getTupleType().getFieldCount();
        RexCall identifierJoinCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.EQUALS ),
                new RexCall(
                        IdentifierUtils.IDENTIFIER_ALG_TYPE,
                        OperatorRegistry.get( OperatorName.ABS ),
                        new RexIndexRef( 0, IdentifierUtils.IDENTIFIER_ALG_TYPE )
                ),
                new RexIndexRef( leftColumnCount, IdentifierUtils.IDENTIFIER_ALG_TYPE ) // TODO TH: this is broken for different column counts then 2 xD. Adjust index accordingly
        );

        RexCall versionJoinCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.EQUALS ),
                new RexCall(
                        IdentifierUtils.VERSION_ALG_TYPE,
                        OperatorRegistry.get( OperatorName.ABS ),
                        new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE )
                ),

                new RexIndexRef( leftColumnCount + 1, IdentifierUtils.VERSION_ALG_TYPE ) // TODO TH: this is broken for different column counts then 2 xD. Adjust index accordingly
        );

        RexCall joinCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.AND ),
                identifierJoinCondition,
                versionJoinCondition
        );

        return LogicalRelJoin.create(
                left,
                right,
                joinCondition,
                Set.of(),
                JoinAlgType.INNER,
                true
        );
    }


    private LogicalRelFilter buildDeletedEntriesFilter( AlgNode targetedScan ) {
        return LogicalRelFilter.create(
                targetedScan,
                new RexCall(
                        BOOLEAN_ALG_TYPE,
                        OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ),
                        new RexIndexRef( 0, IdentifierUtils.IDENTIFIER_ALG_TYPE ),
                        new RexLiteral(
                                PolyBigDecimal.of( 0 ),
                                IdentifierUtils.IDENTIFIER_ALG_TYPE,
                                PolyType.BIGINT
                        )
                ),
                true
        );
    }
}
