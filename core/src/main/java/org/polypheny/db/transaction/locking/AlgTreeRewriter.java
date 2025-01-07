/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.transaction.locking;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.common.Filter;
import org.polypheny.db.algebra.core.lpg.LpgMatch;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.document.LogicalDocIdCollector;
import org.polypheny.db.algebra.logical.document.LogicalDocIdentifier;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.document.LogicalDocumentTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgIdCollector;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgIdentifier;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgTransformer;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIdCollector;
import org.polypheny.db.algebra.logical.relational.LogicalRelIdentifier;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelMatch;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.transaction.Transaction;

public class AlgTreeRewriter extends AlgShuttleImpl {

    private final Transaction transaction;
    private AlgNode collectorInsertPosition;
    private boolean containsIdentifierKey;


    public AlgTreeRewriter( Transaction transaction ) {
        this.transaction = transaction;
        this.collectorInsertPosition = null;
        this.containsIdentifierKey = false;
    }


    public void reset() {
        collectorInsertPosition = null;
    }


    public AlgRoot process( AlgRoot root ) {
        AlgNode rootAlg = root.alg.accept( this );
        if ( collectorInsertPosition == null ) {
            reset();
            return root.withAlg( rootAlg );
        }
        if ( !collectorInsertPosition.equals( rootAlg ) ) {
            throw new RuntimeException( "Should never throw!" );
        }
        AlgNode newRootAlg = null;
        switch ( rootAlg.getModel() ) {
            case RELATIONAL -> newRootAlg = LogicalRelIdCollector.create( rootAlg, transaction, findEntity( rootAlg ) );
            case DOCUMENT -> newRootAlg = LogicalDocIdCollector.create( rootAlg, transaction, findEntity( rootAlg ) );
            case GRAPH -> newRootAlg = LogicalLpgIdCollector.create( rootAlg, transaction, findEntity( rootAlg ) );
        }
        assert newRootAlg != null;
        reset();
        return root.withAlg( newRootAlg );
    }


    private List<AlgNode> getLastNNodes( int count ) {
        List<AlgNode> result = new ArrayList<>();
        for ( Iterator<AlgNode> it = stack.descendingIterator(); it.hasNext(); ) {
            AlgNode node = it.next();
            if ( count-- <= 0 ) {
                break;
            }
            result.add( 0, node );
        }
        return result;
    }


    private void updateCollectorInsertPosition( AlgNode current ) {
        List<AlgNode> trace = getLastNNodes( 2 );

        if ( trace.isEmpty() ) {
            collectorInsertPosition = current;
            return;
        }

        AlgNode first = trace.get( 0 );
        if ( first instanceof Filter ) {
            collectorInsertPosition = first;
            return;
        }
        if ( trace.size() == 1 ) {
            collectorInsertPosition = (first instanceof LpgMatch) ? first : current;
            return;
        }

        AlgNode second = trace.get( 1 );
        collectorInsertPosition = (second instanceof Filter) ? second : current;
    }


    private boolean isTarget( AlgNode node ) {
        if ( collectorInsertPosition == null ) {
            return false;
        }
        return node.getInputs().contains( collectorInsertPosition );
    }


    private List<AlgNode> modifyInputs( List<AlgNode> inputs ) {
        return inputs.stream().map( current -> {
            if ( !current.equals( collectorInsertPosition ) ) {
                return current;
            }
            return modifyInput( current );
        } ).collect( Collectors.toCollection( LinkedList::new ) );
    }


    private AlgNode modifyInput( AlgNode input ) {
        collectorInsertPosition = null;
        switch ( input.getModel() ) {
            case RELATIONAL -> {
                return LogicalRelIdCollector.create( input, transaction, findEntity( input ) );
            }
            case DOCUMENT -> {
                return LogicalDocIdCollector.create( input, transaction, findEntity( input ) );
            }
            case GRAPH -> {
                return LogicalLpgIdCollector.create( input, transaction, findEntity( input ) );
            }
        }
        return input;
    }


    private Entity findEntity( AlgNode node ) {
        Entity entity = null;
        while ( entity == null && node != null ) {
            entity = node.getEntity();
            if ( node.getInputs().isEmpty() ) {
                continue;
            }
            node = node.getInput( 0 );
        }
        return entity;
    }


    @Override
    public AlgNode visit( LogicalRelAggregate aggregate ) {
        LogicalRelAggregate aggregate1 = visitChild( aggregate, 0, aggregate.getInput() );
        if ( isTarget( aggregate1 ) ) {
            return aggregate1.copy( modifyInput( aggregate1.getInput() ) );
        }
        return aggregate1;
    }


    @Override
    public AlgNode visit( LogicalRelMatch match ) {
        LogicalRelMatch match1 = visitChild( match, 0, match.getInput() );
        if ( isTarget( match1 ) ) {
            return match1.copy( modifyInput( match1.getInput() ) );
        }
        return match1;
    }


    @Override
    public AlgNode visit( LogicalRelScan scan ) {
        //ToDo TH: decide whether to keep this once serializable SI is in sight...
        /*
        if ( MvccUtils.isInNamespaceUsingMvcc( scan.getEntity() ) ) {
            updateCollectorInsertPosition( scan );
        }
        */
        return scan;
    }


    @Override
    public AlgNode visit( LogicalRelTableFunctionScan scan ) {
        LogicalRelTableFunctionScan tableFunctionScan1 = visitChildren( scan );
        if ( isTarget( tableFunctionScan1 ) ) {
            List<AlgNode> newInputs = modifyInputs( tableFunctionScan1.getInputs() );
            return tableFunctionScan1.copy( newInputs );
        }
        return tableFunctionScan1;
    }


    @Override
    public AlgNode visit( LogicalRelValues values ) {
        // check for identifier not needed as this is done during sql validation
        return values;
    }


    @Override
    public AlgNode visit( LogicalRelFilter filter ) {
        LogicalRelFilter filter1 = visitChild( filter, 0, filter.getInput() );
        if ( isTarget( filter1 ) ) {
            return filter1.copy( modifyInput( filter1.getInput() ) );
        }
        return filter1;
    }


    @Override
    public AlgNode visit( LogicalRelProject project ) {
        LogicalRelProject project1 = visitChildren( project );
        if ( isTarget( project1 ) ) {
            return project1.copy( modifyInput( project1.getInput() ) );
        }
        return project1;
    }


    @Override
    public AlgNode visit( LogicalRelJoin join ) {
        LogicalRelJoin join1 = visitChildren( join );
        if ( isTarget( join1 ) ) {
            AlgNode left = modifyInput( join1.getLeft() );
            AlgNode right = modifyInput( join1.getRight() );
            return join1.copy( left, right );
        }
        return join1;
    }


    @Override
    public AlgNode visit( LogicalRelCorrelate correlate ) {
        LogicalRelCorrelate correlate1 = visitChildren( correlate );
        if ( isTarget( correlate1 ) ) {
            AlgNode left = modifyInput( correlate1.getLeft() );
            AlgNode right = modifyInput( correlate1.getRight() );
            return correlate.copy( left, right );
        }
        return correlate1;
    }


    @Override
    public AlgNode visit( LogicalRelUnion union ) {
        LogicalRelUnion union1 = visitChildren( union );
        if ( isTarget( union1 ) ) {
            return union1.copy( modifyInputs( union1.getInputs() ) );
        }
        return union1;
    }


    @Override
    public AlgNode visit( LogicalRelIntersect intersect ) {
        LogicalRelIntersect intersect1 = visitChildren( intersect );
        if ( isTarget( intersect1 ) ) {
            return intersect1.copy( modifyInputs( intersect1.getInputs() ) );
        }
        return intersect1;
    }


    @Override
    public AlgNode visit( LogicalRelMinus minus ) {
        LogicalRelMinus minus1 = visitChildren( minus );
        if ( isTarget( minus1 ) ) {
            return minus1.copy( modifyInputs( minus1.getInputs() ) );
        }
        return minus1;
    }


    @Override
    public AlgNode visit( LogicalRelSort sort ) {
        LogicalRelSort sort1 = visitChildren( sort );
        if ( isTarget( sort1 ) ) {
            return sort1.copy( modifyInputs( sort1.getInputs() ) );
        }
        return sort1;
    }


    @Override
    public AlgNode visit( LogicalRelExchange exchange ) {
        LogicalRelExchange exchange1 = visitChildren( exchange );
        if ( isTarget( exchange1 ) ) {
            return exchange1.copy( modifyInputs( exchange1.getInputs() ) );
        }
        return exchange1;
    }


    @Override
    public AlgNode visit( LogicalConditionalExecute lce ) {
        LogicalConditionalExecute lce1 = visitChildren( lce );
        if ( isTarget( lce1 ) ) {
            return lce1.copy( modifyInputs( lce1.getInputs() ) );
        }
        return lce1;
    }


    @Override
    public AlgNode visit( LogicalRelModify modify ) {


        LogicalRelModify modify1 = visitChildren( modify );
        if ( isTarget( modify1 ) ) {
            modify1 = modify1.copy( modifyInputs( modify1.getInputs() ) );
        }
        if ( !MvccUtils.isInNamespaceUsingMvcc( modify.getEntity() ) ) {
            return modify1;
        }
        if ( containsIdentifierKey ) {
            IdentifierUtils.throwIllegalFieldName();
        }
        switch ( modify1.getOperation() ) {
            case INSERT:
                AlgNode input = modify1.getInput();
                LogicalRelIdentifier identifier = LogicalRelIdentifier.create(
                        transaction.getTransactionTimestamp(),
                        modify1.getEntity(),
                        input,
                        input.getTupleType()
                );
                return modify1.copy( modify1.getTraitSet(), List.of( identifier ) );
            default:
                return modify1;
        }
    }


    @Override
    public AlgNode visit( LogicalConstraintEnforcer enforcer ) {
        LogicalConstraintEnforcer enforcer1 = visitChildren( enforcer );
        if ( isTarget( enforcer1 ) ) {
            return enforcer1.copy( modifyInputs( enforcer1.getInputs() ) );
        }
        return enforcer1;
    }


    @Override
    public AlgNode visit( LogicalLpgModify modify ) {
        LogicalLpgModify modify1 = visitChildren( modify );
        if ( isTarget( modify1 ) ) {
            modify1 = modify1.copy( modifyInputs( modify1.getInputs() ) );
        }
        if ( !MvccUtils.isInNamespaceUsingMvcc( modify.getEntity() ) ) {
            return modify1;
        }
        if ( containsIdentifierKey ) {
            IdentifierUtils.throwIllegalFieldName();
        }
        switch ( modify1.getOperation() ) {
            case INSERT:
                AlgNode input = modify1.getInput();
                LogicalLpgIdentifier identifier = LogicalLpgIdentifier.create(
                        transaction.getTransactionTimestamp(),
                        modify1.getEntity(),
                        input
                );
                return modify1.copy( modify1.getTraitSet(), List.of( identifier ) );
            case UPDATE:
                IdentifierUtils.throwIfContainsDisallowedKey( modify1 );
            default:
                return modify1;
        }
    }


    @Override
    public AlgNode visit( LogicalLpgScan scan ) {
        //ToDo TH: decide whether to keep this once serializable SI is in sight...
        /*
        if ( MvccUtils.isInNamespaceUsingMvcc( scan.getEntity() ) ) {
            updateCollectorInsertPosition( scan );
        }
        */
        return scan;
    }


    @Override
    public AlgNode visit( LogicalLpgValues values ) {

        return values;
    }


    @Override
    public AlgNode visit( LogicalLpgFilter filter ) {
        LogicalLpgFilter filter1 = visitChildren( filter );
        if ( isTarget( filter1 ) ) {
            filter1 = filter1.copy( modifyInputs( filter1.getInputs() ) );
        }
        return filter1;
    }


    @Override
    public AlgNode visit( LogicalLpgMatch match ) {
        LogicalLpgMatch match1 = visitChildren( match );
        if ( isTarget( match1 ) ) {
            match1 = match1.copy( modifyInputs( match1.getInputs() ) );
        }
        return match1;
    }


    @Override
    public AlgNode visit( LogicalLpgProject project ) {
        LogicalLpgProject project1 = visitChildren( project );
        if ( isTarget( project1 ) ) {
            project1 = project1.copy( modifyInputs( project1.getInputs() ) );
        }
        return project1;
    }


    @Override
    public AlgNode visit( LogicalLpgAggregate aggregate ) {
        LogicalLpgAggregate aggregate1 = visitChildren( aggregate );
        if ( isTarget( aggregate1 ) ) {
            aggregate1 = aggregate1.copy( modifyInputs( aggregate1.getInputs() ) );
        }
        return aggregate1;
    }


    @Override
    public AlgNode visit( LogicalLpgSort sort ) {
        LogicalLpgSort sort1 = visitChildren( sort );
        if ( isTarget( sort1 ) ) {
            sort1 = sort1.copy( modifyInputs( sort1.getInputs() ) );
        }
        return sort1;
    }


    @Override
    public AlgNode visit( LogicalLpgUnwind unwind ) {
        LogicalLpgUnwind unwind1 = visitChildren( unwind );
        if ( isTarget( unwind1 ) ) {
            unwind1 = unwind1.copy( modifyInputs( unwind1.getInputs() ) );
        }
        return unwind1;
    }


    @Override
    public AlgNode visit( LogicalLpgTransformer transformer ) {
        LogicalLpgTransformer unwind1 = visitChildren( transformer );
        if ( isTarget( unwind1 ) ) {
            unwind1 = unwind1.copy( modifyInputs( unwind1.getInputs() ) );
        }
        return unwind1;
    }


    @Override
    public AlgNode visit( LogicalDocumentModify modify ) {
        LogicalDocumentModify modify1 = visitChildren( modify );
        if ( isTarget( modify1 ) ) {
            modify1 = modify1.copy( modifyInputs( modify1.getInputs() ) );
        }
        if ( !MvccUtils.isInNamespaceUsingMvcc( modify.getEntity() ) ) {
            return modify1;
        }
        if ( containsIdentifierKey ) {
            IdentifierUtils.throwIllegalFieldName();
        }
        switch ( modify1.getOperation() ) {
            case INSERT:
                AlgNode input = modify1.getInput();
                LogicalDocIdentifier identifier = LogicalDocIdentifier.create(
                        transaction.getTransactionTimestamp(),
                        modify1.getEntity(),
                        input
                );
                return modify1.copy( modify1.getTraitSet(), List.of( identifier ) );
            case UPDATE:
                IdentifierUtils.throwIfContainsDisallowedKey( modify1.getUpdates().keySet() );
                return modify1;

        }
        return modify1;
    }


    @Override
    public AlgNode visit( LogicalDocumentAggregate aggregate ) {
        LogicalDocumentAggregate aggregate1 = visitChildren( aggregate );
        if ( isTarget( aggregate1 ) ) {
            aggregate1 = aggregate1.copy( modifyInputs( aggregate1.getInputs() ) );
        }
        return aggregate1;
    }


    @Override
    public AlgNode visit( LogicalDocumentFilter filter ) {
        LogicalDocumentFilter filter1 = visitChildren( filter );
        if ( isTarget( filter1 ) ) {
            filter1 = filter1.copy( modifyInputs( filter1.getInputs() ) );
        }
        return filter1;
    }


    @Override
    public AlgNode visit( LogicalDocumentProject project ) {
        LogicalDocumentProject project1 = visitChildren( project );
        if ( isTarget( project1 ) ) {
            project1 = project1.copy( modifyInputs( project1.getInputs() ) );
        }
        return project1;
    }


    @Override
    public AlgNode visit( LogicalDocumentScan scan ) {
        //ToDo TH: decide whether to keep this once serializable SI is in sight...
        /*
        if ( MvccUtils.isInNamespaceUsingMvcc( scan.getEntity() ) ) {
            updateCollectorInsertPosition( scan );
        }
        */
        return visitChildren( scan );
    }


    @Override
    public AlgNode visit( LogicalDocumentSort sort ) {
        LogicalDocumentSort sort1 = visitChildren( sort );
        if ( isTarget( sort1 ) ) {
            sort1 = sort1.copy( modifyInputs( sort1.getInputs() ) );
        }
        return sort1;
    }


    @Override
    public AlgNode visit( LogicalDocumentTransformer transformer ) {
        LogicalDocumentTransformer sort1 = visitChildren( transformer );
        if ( isTarget( sort1 ) ) {
            sort1 = sort1.copy( modifyInputs( sort1.getInputs() ) );
        }
        return sort1;
    }


    @Override
    public AlgNode visit( LogicalDocumentValues values ) {
        containsIdentifierKey |= IdentifierUtils.containsDisallowedKeys( values.getDocuments() );
        return values;
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        return visitChildren( other );
    }

}
