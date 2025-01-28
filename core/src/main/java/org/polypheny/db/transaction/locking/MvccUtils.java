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

import java.util.List;
import java.util.Set;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Transaction;
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
                case DOCUMENT -> validateDowWrites( sequenceNumber, writtenEntity, transaction );
                case GRAPH -> validateGraphWrites( sequenceNumber, writtenEntity, transaction );
            } );
        }

        return maxVersion <= sequenceNumber;
    }


    private static long validateRelWrites( long sequenceNumber, Entity writtenEntity, Transaction transaction ) {
        String queryTemplate = """
                SELECT MAX(_vid) AS max_vid
                FROM %s
                WHERE _eid IN (
                    SELECT _eid FROM %s WHERE _vid = %d
                )
                """;

        String query = String.format( queryTemplate, writtenEntity.getName(), writtenEntity.getName(), sequenceNumber );
        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( query )
                        .language( QueryLanguage.from( "sql" ) )
                        .origin( transaction.getOrigin() )
                        .namespaceId( writtenEntity.getNamespaceId() )
                        .transactionManager( transaction.getTransactionManager() )
                        .isMvccInternal( true )
                        .build(), transaction ).get( 0 );

        if ( context.getException().isPresent() ) {
            //ToDo TH: properly handle this
            throw new RuntimeException( context.getException().get() );
        }

        ResultIterator iterator = context.execute( context.getStatement() ).getIterator();
        List<List<PolyValue>> res = iterator.getNextBatch();
        return res.get( 0 ).get( 0 ).asLong().longValue();
    }

    private static long validateDowWrites(long sequenceNumber, Entity writtenEntity, Transaction transaction ) {
        String queryTemplate = """
                [
                { "$match": { "_eid": { "$in": db.%s.find({ "_vid": %d }, { "_id": 0, "_eid": 1 }).map(doc => doc._eid) } } },
                { "$group": { "_id": null, "max_vid": { "$max": "$_vid" } } },
                { "$project": { "_id": 0, "max_vid": 1 } }
                ]
                """;

        String query = String.format( queryTemplate, writtenEntity.getName(), sequenceNumber );
        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( query )
                        .language( QueryLanguage.from( "mql" ) )
                        .origin( transaction.getOrigin() )
                        .namespaceId( writtenEntity.getNamespaceId() )
                        .transactionManager( transaction.getTransactionManager() )
                        .isMvccInternal( true )
                        .build(), transaction ).get( 0 );

        if ( context.getException().isPresent() ) {
            //ToDo TH: properly handle this
            throw new RuntimeException( context.getException().get() );
        }

        ResultIterator iterator = context.execute( context.getStatement() ).getIterator();
        List<List<PolyValue>> res = iterator.getNextBatch();
        return res.get( 0 ).get( 0 ).asLong().longValue();

    }

    public static long validateGraphWrites(long sequenceNumber, Entity writtenEntity, Transaction transaction) {
        String queryTemplate = """
                MATCH (n)
                WHERE n._eid IN (
                    MATCH (m)
                    WHERE m._vid = %d
                    RETURN m._eid
                )
                RETURN MAX(n._vid) AS max_vid
                UNION
                MATCH ()-[r]->()
                WHERE r._eid IN (
                    MATCH (m)
                    WHERE m._vid = %d
                    RETURN m._eid
                )
                RETURN MAX(r._vid) AS max_vid
                """;

        String query = String.format( queryTemplate, sequenceNumber, sequenceNumber);
        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( query )
                        .language( QueryLanguage.from( "cypher" ) )
                        .origin( transaction.getOrigin() )
                        .namespaceId( writtenEntity.getNamespaceId() )
                        .transactionManager( transaction.getTransactionManager() )
                        .isMvccInternal( true )
                        .build(), transaction ).get( 0 );

        if ( context.getException().isPresent() ) {
            //ToDo TH: properly handle this
            throw new RuntimeException( context.getException().get() );
        }

        ResultIterator iterator = context.execute( context.getStatement() ).getIterator();
        List<List<PolyValue>> res = iterator.getNextBatch();
        return res.get( 0 ).get( 0 ).asLong().longValue();
    }



    public static long updateWrittenVersionIds( long sequenceNumber, Set<Entity> writtenEntities, Transaction transaction ) {
        long commitSequenceNumber = SequenceNumberGenerator.getInstance().getNextNumber();
        for ( Entity writtenEntity : writtenEntities ) {
            switch ( writtenEntity.getDataModel() ) {
                case RELATIONAL -> updateWrittenRelVersionIds( sequenceNumber, commitSequenceNumber, writtenEntity, transaction );
                case DOCUMENT -> updateWrittenDocVersionIds(sequenceNumber, commitSequenceNumber, writtenEntity, transaction);
                case GRAPH -> updateWrittenGraphVersionIds(sequenceNumber, commitSequenceNumber, writtenEntity, transaction);
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
        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( query )
                        .language( QueryLanguage.from( "sql" ) )
                        .origin( transaction.getOrigin() )
                        .namespaceId( writtenEntity.getNamespaceId() )
                        .transactionManager( transaction.getTransactionManager() )
                        .isMvccInternal( true )
                        .build(), transaction ).get( 0 );

        if ( context.getException().isPresent() ) {
            throw new RuntimeException( "Query preparation failed: " + context.getException().get() );
        }

        context.execute( context.getStatement() );
    }

    private static void updateWrittenGraphVersionIds( long sequenceNumber, long commitSequenceNumber, Entity writtenEntity, Transaction transaction ) {
        String updateTemplate = """
                MATCH (n)
                WHERE n._vid = %d
                SET n._vid = %d

                WITH n // Continue after updating nodes

                MATCH ()-[r]->()
                WHERE r._vid = %d
                SET r._vid = %d
                """;

        String updateQuery = String.format(updateTemplate,
                -sequenceNumber,
                commitSequenceNumber,
                -sequenceNumber,
                commitSequenceNumber);

        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query(updateQuery)
                        .language(QueryLanguage.from("cypher"))
                        .origin(transaction.getOrigin())
                        .namespaceId(writtenEntity.getNamespaceId())
                        .transactionManager(transaction.getTransactionManager())
                        .isMvccInternal(true)
                        .build(),
                transaction).get(0);

        if (context.getException().isPresent()) {
            throw new RuntimeException("Query preparation failed: " + context.getException().get());
        }

        context.execute(context.getStatement());
    }


    private static void updateWrittenDocVersionIds( long sequenceNumber, long commitSequenceNumber, Entity writtenEntity, Transaction transaction ) {
        String updateTemplate = """
                db.%s.updateMany(
                { _vid: %d },
                { $set: { _vid: %d } }
                );
        """;

        String updateQuery = String.format(updateTemplate,
                writtenEntity.getName(),
                -sequenceNumber,
                commitSequenceNumber);

        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query(updateQuery)
                        .language(QueryLanguage.from("mql"))
                        .origin(transaction.getOrigin())
                        .namespaceId(writtenEntity.getNamespaceId())
                        .transactionManager(transaction.getTransactionManager())
                        .isMvccInternal(true)
                        .build(),
                transaction).get(0);

        if (context.getException().isPresent()) {
            throw new RuntimeException("Query preparation failed: " + context.getException().get());
        }

        context.execute(context.getStatement());
    }
}


