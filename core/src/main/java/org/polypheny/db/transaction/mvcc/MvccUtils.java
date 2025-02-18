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

import com.github.dockerjava.api.command.AuthCmd.Exec;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.ConcurrencyControlType;
import org.polypheny.db.transaction.locking.SequenceNumberGenerator;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

public class MvccUtils {

    public static boolean isInNamespaceUsingMvcc( Entity entity ) {
        return isNamespaceUsingMvcc( entity.getNamespaceId() );
    }


    public static boolean isNamespaceUsingMvcc( long namespaceId ) {
        return isNamespaceUsingMvcc( Catalog.getInstance().getSnapshot().getNamespace( namespaceId ).orElseThrow() );
    }


    public static boolean isNamespaceUsingMvcc( LogicalNamespace namespace ) {
        return namespace.getConcurrencyControlType() == ConcurrencyControlType.MVCC;
    }


    public static ConcurrencyControlType getDefaultConcurrencyControlType( DataModel dataModel ) {
        return switch ( dataModel ) {
            case RELATIONAL -> (ConcurrencyControlType) RuntimeConfig.REL_DEFAULT_CONCURRENCY_CONTROL.getEnum();
            case DOCUMENT -> (ConcurrencyControlType) RuntimeConfig.DOC_DEFAULT_CONCURRENCY_CONTROL.getEnum();
            case GRAPH -> (ConcurrencyControlType) RuntimeConfig.GRAPH_DEFAULT_CONCURRENCY_CONTROL.getEnum();
        };
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


    public static ExecutedContext executeStatement( QueryLanguage language, String query, long namespaceId, Transaction transaction ) {
        return executeStatement( language, query, namespaceId, null, transaction );
    }


    public static ExecutedContext executeStatement( QueryLanguage language, String query, long namespaceId, Statement statement ) {
        return executeStatement( language, query, namespaceId, statement, statement.getTransaction() );
    }


    public static ExecutedContext executeStatement( QueryLanguage language, String query, long namespaceId, Statement statement, Transaction transaction ) {
        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( query )
                        .statement( statement )
                        .language( language )
                        .origin( transaction.getOrigin() )
                        .namespaceId( namespaceId )
                        .transactionManager( transaction.getTransactionManager() )
                        .isMvccInternal( true )
                        .build(), transaction ).get( 0 );

        if ( context.getException().isPresent() ) {
            throw new RuntimeException( context.getException().get() );
        }

        return context.execute( context.getStatement() );
    }


    public static List<ExecutedContext> executeDmlAlgTree( AlgRoot root, Statement statement, long namespaceId ) {
        PolyImplementation implementation = statement.getQueryProcessor().prepareQuery( root, true );

        ParsedQueryContext dummyContext = getDummyContext( statement, namespaceId );
        ImplementationContext implementationContext = new ImplementationContext( implementation, dummyContext, statement, null );
        List<ExecutedContext> executedContexts = new ArrayList<>();
            try {
                if ( implementationContext.getException().isPresent() ) {
                    throw implementationContext.getException().get();
                }
                executedContexts.add( implementationContext.execute( implementationContext.getStatement() ) );
            } catch ( Throwable e ) {
                Transaction transaction = implementationContext.getStatement().getTransaction();
                if ( transaction.isAnalyze() && implementationContext.getException().isEmpty() ) {
                    transaction.getQueryAnalyzer().attachStacktrace( e );
                }
                if ( transaction.isActive() ) {
                    transaction.rollback( e.getMessage() );
                }
                executedContexts.add( ExecutedContext.ofError( e, implementationContext, null ) );
                return executedContexts;
            }
        return executedContexts;
    }

    private static ParsedQueryContext getDummyContext(Statement statement, long namespaceId) {
        QueryContext dummyContext = QueryContext.builder()
                .query("")
                .statement( statement )
                .language( QueryLanguage.from("sql") )
                .origin( statement.getTransaction().getOrigin() )
                .namespaceId( namespaceId )
                .transactionManager( statement.getTransaction().getTransactionManager() )
                .build();
        return ParsedQueryContext.fromQuery( "", null, dummyContext );
    }


    private static long validateRelWrites( long sequenceNumber, Entity writtenEntity, Transaction transaction ) {
        // ToDo TH: think about what happens to deletions
        String queryTemplate = """
                SELECT MAX(_vid) AS max_vid
                FROM %s
                WHERE _eid IN (
                    SELECT _eid FROM %s WHERE _vid = %d
                )
                """;

        String query = String.format( queryTemplate, writtenEntity.getName(), writtenEntity.getName(), -sequenceNumber );
        List<List<PolyValue>> res;
        try ( ResultIterator iterator = executeStatement( QueryLanguage.from( "sql" ), query, writtenEntity.getNamespaceId(), transaction ).getIterator() ) {
            res = iterator.getNextBatch();
            return Objects.requireNonNull( res.get( 0 ).get( 0 ).asBigDecimal().getValue() ).longValue();
        }
    }


    private static long validateDocWrites( long sequenceNumber, Entity writtenEntity, Transaction transaction ) {
        // ToDo TH. Replace with more efficient mechanism once group and addToSet are fixed

        // step 1: get list of all written document entry ids
        String writeSetTemplate = "db.%s.find( { \"_vid\": %d }, { \"_eid\": 1 } );";

        String writeSetQuery = String.format( writeSetTemplate, writtenEntity.getName(), -sequenceNumber );
        List<List<PolyValue>> res;
        HashSet<Long> entryIds = new HashSet<>();
        try ( ResultIterator iterator = executeStatement( QueryLanguage.from( "mql" ), writeSetQuery, writtenEntity.getNamespaceId(), transaction ).getIterator() ) {
            iterator.getIterator().forEachRemaining( r -> entryIds.add( r[0].asDocument().get( IdentifierUtils.getIdentifierKeyAsPolyString() ).asLong().longValue() ) );
        }

        // step 2: get max of version ids present for each of the written entry ids
        String getMaxTemplate = "db.%s.aggregate(["
                + "    { \"$match\": { \"_eid\": { \"$in\": [%s] } } },"
                + "    { \"$group\": { \"_id\": null, \"max_vid\": { \"$max\": \"$_vid\" } } }"
                + "]);";

        String entryIdString = entryIds.stream().map( String::valueOf ).collect( Collectors.joining( ", " ) );
        String getMaxQuery = String.format( getMaxTemplate, writtenEntity.getName(), entryIdString );
        try ( ResultIterator iterator = executeStatement( QueryLanguage.from( "mql" ), getMaxQuery, writtenEntity.getNamespaceId(), transaction ).getIterator() ) {
            return Objects.requireNonNull( iterator.getNextBatch().get( 0 ).get( 0 ).asDocument().get( PolyString.of( "max_vid" ) ).asBigDecimal().getValue() ).longValue();
        }
    }


    public static long validateGraphWrites( long sequenceNumber, Entity writtenEntity, Transaction transaction ) {
        throw new NotImplementedException();
        //ToDo TH: implement this
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
        executeStatement( QueryLanguage.from( "sql" ), query, writtenEntity.getNamespaceId(), transaction );
    }


    private static void updateWrittenGraphVersionIds( long sequenceNumber, long commitSequenceNumber, Entity writtenEntity, Transaction transaction ) {
        throw new NotImplementedException();
        //ToDo TH: implement this
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
        executeStatement( QueryLanguage.from( "mql" ), updateQuery, writtenEntity.getNamespaceId(), transaction );
    }

}


