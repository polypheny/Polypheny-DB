package org.polypheny.db.transaction.locking;

import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;

public class DeferredAlgTreeModification {

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
        String queryTemplate = """
                    SELECT t1.*
                    FROM %s t1
                    JOIN (
                        SELECT _eid, MAX(_vid) AS max_vid
                        FROM %s
                        WHERE (_vid > 0 AND _vid <= %d)
                        OR _vid = %d
                        GROUP BY _eid
                        ) t2
                        ON t1._eid = t2._eid AND t1._vid = t2.max_vid;
                """;

        String query = String.format(
                queryTemplate,
                target.getEntity().getName(),
                target.getEntity().getName(),
                statement.getTransaction().getSequenceNumber(),
                statement.getTransaction().getSequenceNumber());

        return generateSubtree( query, QueryLanguage.from( "sql" ) ).getInput( 0 );
    }


    private AlgNode applyLimitDocScanToSnapshot( AlgNode parent ) {
        throw new NotImplementedException();
    }


    private AlgNode applyLimitLpgScanToSnapshot( AlgNode parent ) {
        throw new NotImplementedException();
    }


    private AlgNode generateSubtree( String query, QueryLanguage language ) {
        QueryContext context = QueryContext.builder()
                .query( query )
                .language( language )
                .origin( statement.getTransaction().getOrigin() )
                .namespaceId( target.getEntity().getNamespaceId() )
                .transactionManager( statement.getTransaction().getTransactionManager() )
                .isMvccInternal( true )
                .build();

        Processor processor = context.getLanguage().processorSupplier().get();
        List<QueryContext.ParsedQueryContext> parsedQueries = context.getLanguage().parser().apply( context );
        assert parsedQueries.size() == 1;
        QueryContext.ParsedQueryContext parsed = parsedQueries.get(0);

        if (parsed.getLanguage().validatorSupplier() != null) {
            Pair<Node, AlgDataType> validated = processor.validate(
                    statement.getTransaction(),
                    parsed.getQueryNode().get(),
                    RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
            parsed = ParsedQueryContext.fromQuery( parsed.getQuery(), validated.left, parsed );
        }

        return processor.translate( statement, parsed ).alg;
    }

}
