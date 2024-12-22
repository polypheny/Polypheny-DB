package org.polypheny.db.transaction.locking;

import java.math.BigDecimal;
import java.util.Set;
import java.util.TreeSet;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.entity.numerical.PolyLong;

public class EntryIdentifierRegistry {

    private static final Long MAX_IDENTIFIER_VALUE = Long.MAX_VALUE;
    private static final RexBuilder REX_BUILDER = new RexBuilder( AlgDataTypeFactoryImpl.DEFAULT );
    private final TreeSet<IdentifierInterval> availableIdentifiers;


    public EntryIdentifierRegistry() {
        this.availableIdentifiers = new TreeSet<>();
        this.availableIdentifiers.add( new IdentifierInterval( IdentifierUtils.MISSING_IDENTIFIER + 1, MAX_IDENTIFIER_VALUE ) );
    }

    public EntryIdentifierRegistry(long maxIdentifierValue) {
        this.availableIdentifiers = new TreeSet<>();
        this.availableIdentifiers.add( new IdentifierInterval( IdentifierUtils.MISSING_IDENTIFIER + 1, maxIdentifierValue ) );
    }


    public long getNextEntryIdentifier() {
        while ( !availableIdentifiers.first().hasNextIdentifier() ) {
            availableIdentifiers.pollFirst();
            if ( availableIdentifiers.isEmpty() ) {
                throw new IllegalStateException( "No identifiers available" );
            }
        }
        return availableIdentifiers.first().getNextIdentifier();
    }

    public RexLiteral getNextEntryIdentifierAsLiteral() {
        return REX_BUILDER.makeExactLiteral( BigDecimal.valueOf( getNextEntryIdentifier() ) );
    }


    public PolyLong getNextEntryIdentifierAsPolyLong() {
        return PolyLong.of( getNextEntryIdentifier() );
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
