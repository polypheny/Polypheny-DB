package org.polypheny.db.transaction.locking;

import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.ImmutableBitSet;

public class DeferredAlgTreeModification {

    private static final AlgDataType BOOLEAN_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.BOOLEAN, true );
    private static final AlgDataType INTEGER_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.INTEGER, true );
    private static final AlgDataType DOCUMENT_ALG_TYPE = new DocumentType( List.of(), List.of() );
    private static final PolyString IDENTIFIER_KEY = PolyString.of( "_id" );


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


    public <T extends AlgNode> boolean notTargetsChildOf( T parent ) {
        return !parent.getInputs().contains( target );
    }


    public <T extends AlgNode> boolean notTargets( T node ) {
        return !target.equals( node );
    }


    public <T extends AlgNode> T applyToChildOrSkip( T parent ) {
        if ( notTargetsChildOf( parent ) ) {
            return parent;
        }
        AlgNode newInput = applyOrSkip( target );
        return replaceInput( parent, newInput );
    }


    public <T extends AlgNode> T applyOrSkip( T node ) {
        if ( notTargets( node ) ) {
            return node;
        }
        AlgNode newInput = switch ( modification ) {
            case LIMIT_REL_SCAN_TO_SNAPSHOT -> applyLimitRelScanToSnapshot();
            case LIMIT_DOC_SCAN_TO_SNAPSHOT -> applyLimitDocScanToSnapshot( node );
            case LIMIT_LPG_SCAN_TO_SNAPSHOT -> applyLimitLpgScanToSnapshot( node );
        };
        return (T) newInput;
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


    private LogicalRelAggregate createRelVersionAggregate( LogicalRelProject input ) {
        ImmutableBitSet rightAggregateGroupSet = ImmutableBitSet.of( 0 );
        AggregateCall rightAggregateCall = AggregateCall.create(
                OperatorRegistry.getAgg( OperatorName.MAX ), // TODO: TH actually take max(abs(x)). how is this done?
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
                new RexCall(
                        IdentifierUtils.VERSION_ALG_TYPE,
                        OperatorRegistry.get( OperatorName.ABS ),
                        new RexIndexRef( 1, IdentifierUtils.VERSION_ALG_TYPE )
                ),
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


    private AlgNode applyLimitDocScanToSnapshot( AlgNode node ) {
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

        String query = String.format(
                queryTemplate,
                node.getEntity().getName(),
                statement.getTransaction().getSequenceNumber(),
                -statement.getTransaction().getSequenceNumber() );
        List<List<PolyValue>> res;
        HashMap<Long, Long> documents = new HashMap<>();

        // ToDo: replace this with something more efficient once $abs works properly in mql
        try ( ResultIterator iterator = executeStatement( QueryLanguage.from( "mql" ), query, node.getEntity().getNamespaceId() ).getIterator() ) {
            res = iterator.getNextBatch();
            res.forEach( r -> {
                PolyDocument document = r.get( 0 ).asDocument();

                long eid = document.get( IDENTIFIER_KEY ).asLong().longValue();
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
                                        new RexNameRef( "_eid", null, IdentifierUtils.IDENTIFIER_ALG_TYPE ),
                                        new RexLiteral( PolyLong.of( d.getKey() ), DOCUMENT_ALG_TYPE, PolyType.DOCUMENT )
                                ),
                                new RexCall(
                                        BOOLEAN_ALG_TYPE,
                                        OperatorRegistry.get( QueryLanguage.from( "mql" ), OperatorName.MQL_EQUALS ),
                                        new RexNameRef( "_vid", null, IdentifierUtils.VERSION_ALG_TYPE ),
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


    private AlgNode applyLimitLpgScanToSnapshot( AlgNode parent ) {
        // TODO TH: implement
        throw new NotImplementedException();
    }


    private ExecutedContext executeStatement( QueryLanguage language, String query, long namespaceId ) {
        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( statement.getTransaction().getOrigin() )
                        .namespaceId( namespaceId )
                        .transactionManager( statement.getTransaction().getTransactionManager() )
                        .isMvccInternal( true )
                        .build(), statement.getTransaction() ).get( 0 );

        if ( context.getException().isPresent() ) {
            //ToDo TH: properly handle this
            throw new RuntimeException( context.getException().get() );
        }

        return context.execute( context.getStatement() );
    }

}
