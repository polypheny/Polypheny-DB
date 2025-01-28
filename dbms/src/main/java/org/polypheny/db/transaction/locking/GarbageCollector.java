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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

public class GarbageCollector {

    private static final String TRANSACTION_ORIGIN = "MVCC Garbage Collector";

    private static final String REL_RELEASED_EID_QUERY = """
           SELECT ABS(t._eid) AS released_eid
           FROM %s AS t
           WHERE t._eid < 0
             AND t._vid < %d;
           """;

    private static final String REL_CLEANUP_QUERY = """
            DELETE FROM %s
            WHERE EXISTS (
                SELECT 1
                FROM (
                    SELECT 
                        ABS(_eid) AS eid,
                        MAX(_vid) AS max_vid
                    FROM 
                        %s
                    GROUP BY 
                        ABS(_eid)
                ) AS _MaxVid
                JOIN %s AS t
                ON ABS(t._eid) = _MaxVid.eid
                WHERE 
                    t._vid < %d 
                    AND (
                        _MaxVid.max_vid >= %d 
                        OR t._vid < _MaxVid.max_vid 
                        OR t._eid < 0
                    )
                AND %s._eid = t._eid 
                AND %s._vid = t._vid
            );
            """;

    private static final String DOCUMENT_CLEANUP_STATEMENT = """
    db.%s.aggregate([
        {
            $group: {
                _id: { $abs: "$_eid" },
                max_vid: { $max: "$_vid" }
            }
        },
        {
            $lookup: {
                from: "%s",
                let: { eid: "$_id", max_vid: "$max_vid" },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: [ { $abs: "$_eid" }, "$$eid" ] },
                                    { $lt: ["$_vid", %d] },
                                    {
                                        $or: [
                                            { $gte: ["$$max_vid", %d] },
                                            { $lt: ["$_vid", "$$max_vid"] },
                                            { $lt: ["$_eid", 0] }
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                ],
                as: "toDelete"
            }
        },
        {
            $project: {
                _id: 0,
                deleteIds: "$toDelete._id"
            }
        }
    ]).forEach(doc => {
        db.%s.deleteMany({ _id: { $in: doc.deleteIds } });
    });
    """;

    private static final String GRAPH_CLEANUP_STATEMENT = """
    MATCH (t)
    OPTIONAL MATCH (m)
    WHERE t._vid < %d AND (
        EXISTS {
            MATCH (m)
            WHERE ABS(m._eid) = ABS(t._eid)
            WITH m, MAX(m._vid) AS max_vid
            RETURN max_vid
        } >= %d
        OR t._vid < (
            MATCH (m)
            WHERE ABS(m._eid) = ABS(t._eid)
            RETURN MAX(m._vid)
        )
        OR t._eid < 0
    )
    DETACH DELETE t
    """;

    private static final String RELATIONAL_CLEANUP_LANGUAGE = "sql";
    private static final String DOCUMENT_CLEANUP_LANGUAGE = "mongo";
    private static final String GRAPH_CLEANUP_LANGUAGE = "cypher";

    private final TransactionManager transactionManager;

    private long lastCleanupSequenceNumber;
    private volatile boolean isRunning;


    public GarbageCollector( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
        this.lastCleanupSequenceNumber = 0;
        this.isRunning = false;
    }


    public synchronized void runIfRequired( long totalTransactions ) {
        if (isRunning) {
            return;
        }

        if ( totalTransactions % RuntimeConfig.GARBAGE_COLLECTION_INTERVAL.getLong() != 0 ) {
            return;
        }

        long lowestActiveTransaction = SequenceNumberGenerator.getInstance().getLowestActive();
        if ( lowestActiveTransaction == lastCleanupSequenceNumber ) {
            return;
        }

        lastCleanupSequenceNumber = lowestActiveTransaction;

        // get all namespaces
        Snapshot snapshot = Catalog.snapshot();
        List<LogicalEntity> entities = snapshot.getNamespaces( null ).stream()
                .filter( LogicalNamespace::isUseMvcc )
                .flatMap( n -> snapshot.getLogicalEntities( n.getId() ).stream() )
                .toList();

        if ( entities.isEmpty() ) {
            return;
        }

        // for each entity run cleanup
        isRunning = true;
        Transaction transaction = transactionManager.startTransaction(  Catalog.defaultUserId, false, TRANSACTION_ORIGIN );
        entities.forEach( e -> garbageCollect( e, lowestActiveTransaction, transaction ) );
        transaction.commit();
        isRunning = false;
    }


    private void garbageCollect( LogicalEntity entity, long lowestActiveVersion, Transaction transaction ) {
        switch ( entity.getDataModel() ) {
            case RELATIONAL -> relGarbageCollect( entity, lowestActiveVersion, transaction );
            case DOCUMENT -> docGarbageCollect( entity, lowestActiveVersion, transaction );
            case GRAPH -> graphGarbageCollect( entity, lowestActiveVersion, transaction );
        }
    }


    private void relGarbageCollect( LogicalEntity entity, long lowestActiveVersion, Transaction transaction ) {
        String releasedEidQuery = String.format(
                REL_RELEASED_EID_QUERY,
                entity.getName(),
                lowestActiveVersion
        );

        Set<Long> releasedIds = new HashSet<>(); //ToDo TH: this will become a problem with table size
        Statement statement1 = transaction.createStatement();
        ResultIterator iterator = executeStatement( releasedEidQuery, RELATIONAL_CLEANUP_LANGUAGE, entity.getNamespaceId(), statement1, transaction).getIterator();
        iterator.getIterator(). forEachRemaining( r -> releasedIds.add( r[0].asLong().longValue() ));
        iterator.close();
        statement1.close();

        String query = String.format(
                REL_CLEANUP_QUERY,
                entity.getName(),
                entity.getName(),
                entity.getName(),
                lowestActiveVersion,
                lowestActiveVersion,
                entity.getName(),
                entity.getName()
                );
        Statement statement2 = transaction.createStatement();
        executeStatement( query, RELATIONAL_CLEANUP_LANGUAGE, entity.getNamespaceId(), statement2, transaction);
        statement2.close();

        entity.getEntryIdentifiers().releaseEntryIdentifiers( releasedIds );
    }


    private void docGarbageCollect( LogicalEntity entity, long lowestActiveVersion, Transaction transaction ) {
        // ToDo TH: implement
        //throw new NotImplementedException();
    }


    private void graphGarbageCollect( LogicalEntity entity, long lowestActiveVersion, Transaction transaction ) {
        // ToDo TH: implement
        //throw new NotImplementedException();
    }


    private ExecutedContext executeStatement( String query, String language, long namespaceId, Statement statement, Transaction transaction) {
        ImplementationContext implementationContext = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( query )
                        .language( QueryLanguage.from( language ) )
                        .statement( statement )
                        .transactions( List.of( transaction ) )
                        .origin( transaction.getOrigin() )
                        .namespaceId( namespaceId )
                        .transactionManager( transactionManager )
                        .isMvccInternal( true )
                        .build(), transaction ).get( 0 );
        return implementationContext.execute( implementationContext.getStatement() );
    }

}
