package org.polypheny.db.transaction.locking;

import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;

public class DeferredAlgTreeModification {

    private static final AlgDataType BOOLEAN_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.BOOLEAN, true );
    private static final AlgDataType INTEGER_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.INTEGER, true );


    private final AlgNode target;
    private final Modification modification;
    private final Statement statement;


    public enum Modification {
        LIMIT_REL_SCAN_TO_SNAPSHOT,
        LIMIT_DOC_SCAN_TO_SNAPSHOT,
        LIMIT_LPG_SCAN_TO_SNAPSHOT,
    }


    public DeferredAlgTreeModification( AlgNode target, Modification modification, Statement statement ) {
        this.target = target;
        this.modification = modification;
        this.statement = statement;
    }


    public <T extends AlgNode> boolean notTargets( T parent ) {
        return !parent.getInputs().contains( target );
    }


    public <T extends AlgNode> T applyOrSkip( T parent ) {
        if ( notTargets( parent ) ) {
            return parent;
        }

        AlgNode newInput = switch ( modification ) {
            case LIMIT_REL_SCAN_TO_SNAPSHOT -> applyLimitRelScanToSnapshot();
            case LIMIT_DOC_SCAN_TO_SNAPSHOT -> applyLimitDocScanToSnapshot( parent );
            case LIMIT_LPG_SCAN_TO_SNAPSHOT -> applyLimitLpgScanToSnapshot( parent );
        };

        return replaceInput( parent, newInput );
    }


    private <T extends AlgNode> T replaceInput( T node, AlgNode newInput ) {
        List<AlgNode> inputs = node.getInputs().stream()
                .map( input -> input == target ? newInput : input )
                .toList();
        return (T) node.copy( node.getTraitSet(), inputs );
    }


    private AlgNode applyLimitRelScanToSnapshot() {
        LogicalRelFilter scopeFilter = buildRelScopeFilter( target );
        LogicalRelProject identifierVersionProject = createRelIdentifierVersionProject( scopeFilter );
        LogicalRelAggregate rightAggregate = createRelVersionAggregate( identifierVersionProject );
        return createVersionScanJoin( target, rightAggregate );
    }


    private LogicalRelProject createRelIdentifierVersionProject( LogicalRelFilter input ) {
        return LogicalRelProject.create(
                input,
                List.of(
                        new RexIndexRef( 0, IdentifierUtils.IDENTIFIER_ALG_TYPE ),
                        new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE )
                ),
                List.of(
                        IdentifierUtils.IDENTIFIER_KEY,
                        IdentifierUtils.VERSION_KEY
                )
        );
    }


    private LogicalRelAggregate createRelVersionAggregate( LogicalRelProject input ) {
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


    private LogicalRelJoin createVersionScanJoin( AlgNode left, LogicalRelAggregate right ) {
        RexCall identifierJoinCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.EQUALS ),
                new RexIndexRef( 0, IdentifierUtils.IDENTIFIER_ALG_TYPE ),
                new RexIndexRef( 4, IdentifierUtils.IDENTIFIER_ALG_TYPE )
        );

        RexCall versionJoinCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.EQUALS ),
                new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE ),
                new RexIndexRef( 5, IdentifierUtils.VERSION_ALG_TYPE )
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
                JoinAlgType.INNER
        );
    }


    private LogicalRelFilter buildRelScopeFilter( AlgNode input ) {
        RexCall selfReadCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.EQUALS ),
                new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE ),
                new RexLiteral(
                        IdentifierUtils.getVersionAsPolyLong( statement.getTransaction().getSequenceNumber(), false ),
                        IdentifierUtils.VERSION_ALG_TYPE,
                        PolyType.BIGINT
                )
        );

        RexCall versionInSnapshotCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ),
                new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE ),
                new RexLiteral(
                        IdentifierUtils.getVersionAsPolyLong( statement.getTransaction().getSequenceNumber(), true ),
                        IdentifierUtils.VERSION_ALG_TYPE,
                        PolyType.BIGINT
                )
        );

        RexCall versionCommittedCondition = new RexCall(
                BOOLEAN_ALG_TYPE,
                OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ),
                new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE ),
                new RexLiteral(
                        PolyLong.of( 0 ),
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


    private AlgNode applyLimitDocScanToSnapshot( AlgNode parent ) {
        // TODO TH: implement
        throw new NotImplementedException();
    }


    private AlgNode applyLimitLpgScanToSnapshot( AlgNode parent ) {
        // TODO TH: implement
        throw new NotImplementedException();
    }
}
