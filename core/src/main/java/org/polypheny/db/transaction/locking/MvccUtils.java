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

package org.polypheny.db.transaction.locking;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

public class MvccUtils {

    public static boolean isInNamespaceUsingMvcc( Entity entity ) {
        return Catalog.getInstance().getSnapshot().getNamespace( entity.getNamespaceId() ).orElseThrow().isUseMvcc();
    }


    public static boolean isNamespaceUsingMvcc( long namespaceId ) {
        return Catalog.getInstance().getSnapshot().getNamespace( namespaceId ).orElseThrow().isUseMvcc();
    }


    public static boolean validateWriteSet( long sequenceNumber, Set<Entity> writtenEntities, Transaction transaction ) {
        long maxVersion = 0;

        for ( Entity writtenEntity : writtenEntities ) {
            maxVersion = Math.max( maxVersion, switch ( writtenEntity.getDataModel() ) {
                case RELATIONAL -> validateRelWrites( sequenceNumber, writtenEntity, transaction );
                case DOCUMENT -> validateDocWrites( sequenceNumber, writtenEntity, transaction );
                case GRAPH -> validateGraphWrites( sequenceNumber, writtenEntity, transaction );
            } );
        }

        return maxVersion <= sequenceNumber;
    }


    private static ExecutedContext executeStatement( Entity entitiy, Transaction transaction, QueryLanguage language, String query ) {
        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( query )
                        .language( language )
                        .origin( transaction.getOrigin() )
                        .namespaceId( entitiy.getNamespaceId() )
                        .transactionManager( transaction.getTransactionManager() )
                        .isMvccInternal( true )
                        .build(), transaction ).get( 0 );

        if ( context.getException().isPresent() ) {
            //ToDo TH: properly handle this
            throw new RuntimeException( context.getException().get() );
        }

        return context.execute( context.getStatement() );
    }


    private static long validateRelWrites( long sequenceNumber, Entity writtenEntity, Transaction transaction ) {
        String queryTemplate = """
                SELECT MAX(_vid) AS max_vid
                FROM %s
                WHERE _eid IN (
                    SELECT _eid FROM %s WHERE _vid = %d
                )
                """;

        String query = String.format( queryTemplate, writtenEntity.getName(), writtenEntity.getName(), -sequenceNumber );
        List<List<PolyValue>> res;
        try ( ResultIterator iterator = executeStatement( writtenEntity, transaction, QueryLanguage.from( "sql" ), query ).getIterator() ) {
            res = iterator.getNextBatch();
            return res.get( 0 ).get( 0 ).asLong().longValue();
        }
    }


    private static long validateDocWrites( long sequenceNumber, Entity writtenEntity, Transaction transaction ) {
        // ToDo TH. Replace with more efficient mechanism once group and addToSet are fixed

        // step 1: get list of all written document entry ids
        String writeSetTemplate = "db.%s.find( { \"_vid\": %d }, { \"_eid\": 1 } );";

        String writeSetQuery = String.format( writeSetTemplate, writtenEntity.getName(), -sequenceNumber );
        List<List<PolyValue>> res;
        HashSet<Long> entryIds = new HashSet<>();
        try ( ResultIterator iterator = executeStatement( writtenEntity, transaction, QueryLanguage.from( "mql" ), writeSetQuery ).getIterator() ) {
            while ( iterator.hasMoreRows() ) {
                res = iterator.getNextBatch();
                res.forEach( r -> entryIds.add( r.get( 0 ).asDocument().get( IdentifierUtils.getIdentifierKeyAsPolyString() ).asLong().longValue() ) );
            }
        }

        // step 2: get max of version ids present for each of the written entry ids
        String getMaxTemplate = "db.%s.aggregate(["
                + "    { \"$match\": { \"_eid\": { \"$in\": [%s] } } },"
                + "    { \"$group\": { \"_id\": null, \"max_vid\": { \"$max\": \"$_vid\" } } }"
                + "]);";

        String entryIdString = entryIds.stream().map( String::valueOf ).collect( Collectors.joining( ", " ) );
        String getMaxQuery = String.format( getMaxTemplate, writtenEntity.getName(), entryIdString );
        try ( ResultIterator iterator = executeStatement( writtenEntity, transaction, QueryLanguage.from( "mql" ), getMaxQuery ).getIterator() ) {
            return Objects.requireNonNull( iterator.getNextBatch().get( 0 ).get( 0 ).asDocument().get( PolyString.of( "max_vid" ) ).asBigDecimal().getValue() ).longValue();
        }
    }


    public static long validateGraphWrites( long sequenceNumber, Entity writtenEntity, Transaction transaction ) {
        // Step 1: get written node entry ids
        String writeSetQuery = String.format(
                "MATCH (m) WHERE m._vid = %d RETURN m._eid AS eids",
                -sequenceNumber
        );
        List<List<PolyValue>> res;
        List<Long> nodeEntryIds = new ArrayList<>();
        try (ResultIterator iterator = executeStatement(writtenEntity, transaction, QueryLanguage.from("cypher"), writeSetQuery).getIterator()) {
            while ( iterator.hasMoreRows() ) {
                res = iterator.getNextBatch();
                res.forEach( r -> nodeEntryIds.add( Long.parseLong(r.get(0).asString().getValue() ) ));
            }
        }

        // Step 2: find the maximum _vid for nodes
        long maxVidNodes = -1;
        for (long eid : nodeEntryIds) {
            String getMaxNodesQuery = String.format(
                    "MATCH (n) WHERE n._eid = %d RETURN MAX(n._vid) AS max_vid_nodes",
                    eid
            );
            maxVidNodes = getMaxVidOfGraphElements( writtenEntity, transaction, maxVidNodes, getMaxNodesQuery );
        }

        // Step 3: get written relationship entry ids
        String relWriteSetQuery = String.format(
                "MATCH ()-[r]->() WHERE r._vid = %d RETURN r._eid AS eids",
                -sequenceNumber
        );
        List<Long> relEntryIds = new ArrayList<>();
        try (ResultIterator iterator = executeStatement(writtenEntity, transaction, QueryLanguage.from("cypher"), relWriteSetQuery).getIterator()) {
            while (iterator.hasMoreRows()) {
                res = iterator.getNextBatch();
                res.forEach(r -> relEntryIds.add(Long.parseLong(r.get(0).asString().getValue())));
            }
        }

        // Step 4: Find the maximum _vid for relationships
        long maxVidRels = -1;
        for (long eid : relEntryIds) {
            String getMaxRelsQuery = String.format(
                    "MATCH ()-[r]->() WHERE r._eid = %d RETURN MAX(r._vid) AS max_vid_rels",
                    eid
            );
            maxVidRels = getMaxVidOfGraphElements( writtenEntity, transaction, maxVidRels, getMaxRelsQuery );
        }

        return Math.max(maxVidNodes, maxVidRels);
    }


    private static long getMaxVidOfGraphElements( Entity writtenEntity, Transaction transaction, long maxVidNodes, String getMaxNodesQuery ) {
        try ( ResultIterator iterator = executeStatement(writtenEntity, transaction, QueryLanguage.from("cypher"), getMaxNodesQuery).getIterator()) {
            long currentMax = Objects.requireNonNull( iterator.getNextBatch().get( 0 ).get( 0 ).asBigDecimal().getValue() ).longValue();
            maxVidNodes = Math.max( maxVidNodes, currentMax );
        }
        return maxVidNodes;
    }


    public static long updateWrittenVersionIds( long sequenceNumber, Set<Entity> writtenEntities, Transaction transaction ) {
        long commitSequenceNumber = SequenceNumberGenerator.getInstance().getNextNumber();
        for ( Entity writtenEntity : writtenEntities ) {
            switch ( writtenEntity.getDataModel() ) {
                case RELATIONAL -> updateWrittenRelVersionIds( sequenceNumber, commitSequenceNumber, writtenEntity, transaction );
                case DOCUMENT -> updateWrittenDocVersionIds( sequenceNumber, commitSequenceNumber, writtenEntity, transaction );
                case GRAPH -> updateWrittenGraphVersionIds( sequenceNumber, commitSequenceNumber, writtenEntity, transaction );
            }
        }
        SequenceNumberGenerator.getInstance().releaseNumber( sequenceNumber );
        return commitSequenceNumber;
    }


    private static void updateWrittenRelVersionIds( long sequenceNumber, long commitSequenceNumber, Entity writtenEntity, Transaction transaction ) {
        String queryTemplate = """
                UPDATE %s
                SET _vid = %d
                WHERE _vid = %d
                """;

        String query = String.format( queryTemplate, writtenEntity.getName(), commitSequenceNumber, -sequenceNumber );
        executeStatement( writtenEntity, transaction, QueryLanguage.from( "sql" ), query );
    }


    private static void updateWrittenGraphVersionIds( long sequenceNumber, long commitSequenceNumber, Entity writtenEntity, Transaction transaction ) {
        return;
        /**
        String updateTemplate = """
                MATCH (n)
                WHERE n._vid = %d
                SET n._vid = %d

                WITH n

                MATCH ()-[r]->()
                WHERE r._vid = %d
                SET r._vid = %d
                """;

        String updateQuery = String.format( updateTemplate,
                -sequenceNumber,
                commitSequenceNumber,
                -sequenceNumber,
                commitSequenceNumber );

        executeStatement( writtenEntity, transaction, QueryLanguage.from( "cypher" ), updateQuery );
         **/
    }


    private static void updateWrittenDocVersionIds( long sequenceNumber, long commitSequenceNumber, Entity writtenEntity, Transaction transaction ) {
        String updateTemplate = """
                        db.%s.updateMany(
                        { _vid: %d },
                        { $set: { _vid: %d } }
                        );
                """;

        String updateQuery = String.format( updateTemplate,
                writtenEntity.getName(),
                -sequenceNumber,
                commitSequenceNumber );
        executeStatement( writtenEntity, transaction, QueryLanguage.from( "mql" ), updateQuery );
    }

}


