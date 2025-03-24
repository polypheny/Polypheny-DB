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

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.db.transaction.mvcc.MvccUtils;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.document.PolyDocument;

public class DocGarbageCollector implements GarbageCollector {

    private static final String DOC_CLEANUP_LANGUAGE = "mongo";

    private static final String DOC_RELEASED_EID_QUERY = """
            db.%s.aggregate([
              {
                $match: {
                  _eid: { $lt: 0 },
                  _vid: { $lt: %s }
                }
              },
              {
                $project: {
                  _eid: { $abs: "$_eid" }
                }
              }
            ]);
            """;

    private static final String DOC_NEWEST_VERSION_STATEMENT = """
            db.%s.aggregate([
                {
                    "$group": {
                        "_id": "$_eid",
                        "max_vid": { "$max": "$_vid" }
                    }
                },
                {
                    "$project": {
                        "_id": { "$abs": "$_id" },
                        "_vid": "$max_vid"
                    }
                }
            ])
            """;

    private static final String DOC_FIND_GARBAGE_STATEMENT = """
            db.%s.aggregate([
                { "$match": {
                    "_vid": { "$lt": %d },
                    "$or": [
                        %s
                    ]
                }},
                { "$project": { "_id": 1 } }
            ])
            """;

    private static final String DOC_DELETE_STATEMENT = """
            db.%s.deleteMany({ "_id": { "$in": [ %s ] } })
            """;


    @Override
    public void collect( LogicalEntity entity, long lowestActiveVersion, Transaction transaction ) {
        releaseUnusedDocEntryIds( entity, lowestActiveVersion, transaction );

        String matchConditions = getMatchConditions( entity, transaction );
        if ( matchConditions.isEmpty() ) {
            return;
        }

        Set<Long> documentsToDelete = findGarbageDocuments( entity, lowestActiveVersion, matchConditions, transaction );
        if ( !documentsToDelete.isEmpty() ) {
            return;
        }
        deleteGarbageDocuments( entity, documentsToDelete, transaction );
    }


    private String getMatchConditions( LogicalEntity entity, Transaction transaction ) {
        PolyString identifierKey = PolyString.of( "_id" );
        Statement statement = transaction.createStatement();
        String getNewestVersionQuery = String.format( DOC_NEWEST_VERSION_STATEMENT, entity.getName() );

        StringBuilder matchConditions = new StringBuilder();
        try ( ResultIterator iterator = MvccUtils.executeStatement( QueryLanguage.from( DOC_CLEANUP_LANGUAGE ), getNewestVersionQuery, entity.getNamespaceId(), statement, transaction ).getIterator() ) {
            iterator.getIterator().forEachRemaining( r -> {
                PolyDocument document = r[0].asDocument();
                matchConditions.append( String.format(
                        "{ \"$and\": [ { \"_eid\": %d }, { \"_vid\": { \"$ne\": %d } } ] },",
                        document.get( identifierKey ).asLong().longValue(),
                        MvccUtils.collectLong( document.get( IdentifierUtils.getVersionKeyAsPolyString() ) )
                ) );
            } );
        }
        statement.close();

        if ( !matchConditions.isEmpty() ) {
            matchConditions.setLength( matchConditions.length() - 2 );
            matchConditions.append( "}" );
        }

        return matchConditions.toString();
    }


    private Set<Long> findGarbageDocuments( LogicalEntity entity, long lowestActiveVersion, String matchConditions, Transaction transaction ) {
        Statement statement = transaction.createStatement();
        String findGarbageQuery = String.format( DOC_FIND_GARBAGE_STATEMENT, entity.getName(), lowestActiveVersion, matchConditions );

        Set<Long> documentsToDelete = new HashSet<>();
        PolyString identifierKey = PolyString.of( "_id" );
        try ( ResultIterator iterator = MvccUtils.executeStatement( QueryLanguage.from( DOC_CLEANUP_LANGUAGE ), findGarbageQuery, entity.getNamespaceId(), statement, transaction ).getIterator() ) {
            iterator.getIterator().forEachRemaining( r -> {
                PolyDocument document = r[0].asDocument();
                documentsToDelete.add( MvccUtils.collectLong( document.get( identifierKey ) ) );
            } );
        }
        statement.close();

        return documentsToDelete;
    }


    private void deleteGarbageDocuments( LogicalEntity entity, Set<Long> documentsToDelete, Transaction transaction ) {
        Statement statement = transaction.createStatement();
        String deleteQuery = String.format(
                DOC_DELETE_STATEMENT,
                entity.getName(),
                documentsToDelete.stream().map( id -> "\"" + id + "\"" ).collect( Collectors.joining( ", " ) )
        );
        MvccUtils.executeStatement( QueryLanguage.from( DOC_CLEANUP_LANGUAGE ), deleteQuery, entity.getNamespaceId(), statement, transaction );
    }


    private void releaseUnusedDocEntryIds( LogicalEntity entity, long lowestActiveVersion, Transaction transaction ) {
        Statement statement = transaction.createStatement();
        Set<Long> releasedIds = new HashSet<>();
        String releasedEidQuery = String.format(
                DOC_RELEASED_EID_QUERY,
                entity.getName(),
                lowestActiveVersion
        );
        try ( ResultIterator iterator = MvccUtils.executeStatement( QueryLanguage.from( DOC_CLEANUP_LANGUAGE ), releasedEidQuery, entity.getNamespaceId(), statement, transaction ).getIterator() ) {
            iterator.getIterator().forEachRemaining( r -> {
                PolyDocument document = r[0].asDocument();
                releasedIds.add( MvccUtils.collectLong( document.get( IdentifierUtils.getVersionKeyAsPolyString() ) ) );
            } );
        }
        entity.getEntryIdentifiers().releaseEntryIdentifiers( releasedIds );
    }


}
