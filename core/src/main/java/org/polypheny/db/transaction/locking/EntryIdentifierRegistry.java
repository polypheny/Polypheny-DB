package org.polypheny.db.transaction.locking;

import java.util.Set;
import java.util.TreeSet;

public class EntryIdentifierRegistry {

    private static final Long MAX_IDENTIFIER_VALUE = Long.MAX_VALUE;
    private final TreeSet<IdentifierInterval> availableIdentifiers;

    private final long entityId;


    public EntryIdentifierRegistry(long entityId) {
        this.availableIdentifiers = new TreeSet<>();
        this.availableIdentifiers.add( new IdentifierInterval( IdentifierUtils.MISSING_IDENTIFIER + 1, MAX_IDENTIFIER_VALUE ) );
        this.entityId = entityId;
    }


    public EntryIdentifierRegistry(long entityId, long maxIdentifierValue ) {
        this.availableIdentifiers = new TreeSet<>();
        this.availableIdentifiers.add( new IdentifierInterval( IdentifierUtils.MISSING_IDENTIFIER + 1, maxIdentifierValue ) );
        this.entityId = entityId;
    }


    public VersionedEntryIdentifier getNextEntryIdentifier() {
        while ( !availableIdentifiers.first().hasNextIdentifier() ) {
            availableIdentifiers.pollFirst();
            if ( availableIdentifiers.isEmpty() ) {
                throw new IllegalStateException( "No identifiers available" );
            }
        }
        long nextIdentifier = availableIdentifiers.first().getNextIdentifier();
        return new VersionedEntryIdentifier(entityId, nextIdentifier );
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
