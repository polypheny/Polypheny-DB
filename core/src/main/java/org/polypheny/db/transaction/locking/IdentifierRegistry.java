package org.polypheny.db.transaction.locking;

import java.util.Set;
import java.util.TreeSet;

public class IdentifierRegistry {

    private static final Long MAX_IDENTIFIER_VALUE = Long.MAX_VALUE;
    public static final IdentifierRegistry INSTANCE = new IdentifierRegistry( MAX_IDENTIFIER_VALUE );

    private final TreeSet<IdentifierInterval> availableIdentifiers;


    IdentifierRegistry( long maxIdentifierValue ) {
        this.availableIdentifiers = new TreeSet<>();
        this.availableIdentifiers.add( new IdentifierInterval( 0, maxIdentifierValue ) );
    }


    public long getEntryIdentifier() {
        while ( !availableIdentifiers.first().hasNextIdentifier() ) {
            availableIdentifiers.pollFirst();
            if ( availableIdentifiers.isEmpty() ) {
                throw new IllegalStateException( "No identifiers available" );
            }
        }
        return availableIdentifiers.first().getNextIdentifier();
    }


    public void releaseEntryIdentifiers( Set<Long> identifiers ) {
        if ( identifiers.isEmpty() ) {
            return;
        }

        for ( long currentIdentifier : identifiers ) {
            IdentifierInterval newInterval = new IdentifierInterval( currentIdentifier, currentIdentifier + 1 );

            IdentifierInterval lowerAdjacentInterval = availableIdentifiers.floor( newInterval );
            IdentifierInterval upperAdjacentInterval = availableIdentifiers.ceiling( newInterval );

            boolean isMergedWithLower = mergeWithLowerInterval( lowerAdjacentInterval, currentIdentifier );
            boolean isMergedWithUpper = mergeWithUpperInterval( lowerAdjacentInterval, upperAdjacentInterval, currentIdentifier, isMergedWithLower );

            if ( isMergedWithLower || isMergedWithUpper ) {
                continue;
            }
            availableIdentifiers.add( newInterval );
        }
    }


    private boolean mergeWithLowerInterval( IdentifierInterval lowerInterval, long currentIdentifier ) {
        if ( lowerInterval == null ) {
            return false;
        }
        if ( lowerInterval.getUpperBound() != currentIdentifier ) {
            return false;
        }
        lowerInterval.setUpperBound( currentIdentifier + 1 );
        return true;
    }


    private boolean mergeWithUpperInterval( IdentifierInterval lowerInterval, IdentifierInterval upperInterval, long currentIdentifier, boolean isMergedWithLower ) {
        if ( upperInterval == null ) {
            return false;
        }
        if ( upperInterval.getLowerBound() != currentIdentifier + 1 ) {
            return false;
        }
        if ( !isMergedWithLower ) {
            upperInterval.setLowerBound( currentIdentifier );
            return true;
        }
        if ( lowerInterval == null ) {
            return true;
        }
        lowerInterval.setUpperBound( upperInterval.getUpperBound() );
        availableIdentifiers.remove( upperInterval );
        return true;
    }

}
