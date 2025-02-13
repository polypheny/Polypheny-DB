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

import java.util.Set;
import java.util.TreeSet;
import org.polypheny.db.catalog.entity.Entity;

public class EntryIdentifierRegistry {

    private static final Long MAX_IDENTIFIER_VALUE = Long.MAX_VALUE;
    private final TreeSet<IdentifierInterval> availableIdentifiers;

    private final Entity entity;


    public EntryIdentifierRegistry(Entity entity) {
        this.availableIdentifiers = new TreeSet<>();
        this.availableIdentifiers.add( new IdentifierInterval( IdentifierUtils.MISSING_IDENTIFIER + 1, MAX_IDENTIFIER_VALUE ) );
        this.entity = entity;
    }


    public EntryIdentifierRegistry(Entity entity, long maxIdentifierValue ) {
        this.availableIdentifiers = new TreeSet<>();
        this.availableIdentifiers.add( new IdentifierInterval( IdentifierUtils.MISSING_IDENTIFIER + 1, maxIdentifierValue ) );
        this.entity = entity;
    }

    public EntryIdentifier getNextEntryIdentifier() {
        return new EntryIdentifier(entity, getNextIdentifier() );
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

    private long getNextIdentifier() {
        while ( !availableIdentifiers.first().hasNextIdentifier() ) {
            availableIdentifiers.pollFirst();
            if ( availableIdentifiers.isEmpty() ) {
                throw new IllegalStateException( "No identifiers available" );
            }
        }
        return availableIdentifiers.first().getNextIdentifier();
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
