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

package org.polypheny.db.prisminterface.statements;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.prisminterface.statements.PIStatement.ResultCursor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.PrepareStatementRequest;

@Slf4j
public class StatementManager {

    private final AtomicInteger statementIdGenerator;
    private final PIClient client;
    private final Map<Integer, PIStatement> openStatements;
    private final Map<Integer, PIUnparameterizedStatementBatch> openUnparameterizedBatches;


    public StatementManager( PIClient client ) {
        this.client = client;
        statementIdGenerator = new AtomicInteger( 1 );
        openStatements = new ConcurrentHashMap<>();
        openUnparameterizedBatches = new ConcurrentHashMap<>();
    }


    private LogicalNamespace getNamespace( String namespaceName ) {
        Optional<LogicalNamespace> optionalNamespace = Catalog.snapshot().getNamespace( namespaceName );

        if ( optionalNamespace.isEmpty() ) {
            throw new PIServiceException( "Getting namespace " + namespaceName + " failed." );
        }
        return optionalNamespace.get();
    }


    public PIUnparameterizedStatement createUnparameterizedStatement( ExecuteUnparameterizedStatementRequest request ) throws PIServiceException {
        String languageName = request.getLanguageName();
        if ( !isSupportedLanguage( languageName ) ) {
            throw new PIServiceException( "Language " + languageName + " not supported." );
        }
        int statementId = statementIdGenerator.getAndIncrement();
        LogicalNamespace namespace = client.getNamespace();
        if ( request.hasNamespaceName() ) {
            namespace = getNamespace( request.getNamespaceName() );
        }
        assert namespace != null;
        PIUnparameterizedStatement statement = new PIUnparameterizedStatement(
                statementId,
                client,
                QueryLanguage.from( languageName ),
                namespace,
                request.getStatement()
        );
        openStatements.put( statementId, statement );
        if ( log.isTraceEnabled() ) {
            log.trace( "created request {}", statement );
        }
        return statement;
    }


    public PIUnparameterizedStatementBatch createUnparameterizedStatementBatch( List<ExecuteUnparameterizedStatementRequest> statements ) {
        List<PIUnparameterizedStatement> PIUnparameterizedStatements = statements.stream()
                .map( this::createUnparameterizedStatement )
                .toList();
        final int batchId = statementIdGenerator.getAndIncrement();
        final PIUnparameterizedStatementBatch batch = new PIUnparameterizedStatementBatch( batchId, client, PIUnparameterizedStatements );
        openUnparameterizedBatches.put( batchId, batch );
        if ( log.isTraceEnabled() ) {
            log.trace( "created batch {}", batch );
        }
        return batch;
    }


    public PIPreparedIndexedStatement createIndexedPreparedInterfaceStatement( PrepareStatementRequest request ) throws PIServiceException {
        String languageName = request.getLanguageName();
        if ( !isSupportedLanguage( languageName ) ) {
            throw new PIServiceException( "Language " + languageName + " not supported." );
        }
        QueryLanguage language = QueryLanguage.from( languageName );
        if ( language.dataModel() == DataModel.GRAPH ) {
            throw new PIServiceException( "Indexed prepared Cypher statements are not supported by Prism. Use named parameters." );
        }
        int statementId = statementIdGenerator.getAndIncrement();
        LogicalNamespace namespace = client.getNamespace();
        if ( request.hasNamespaceName() ) {
            namespace = getNamespace( request.getNamespaceName() );
        }
        assert namespace != null;
        PIPreparedIndexedStatement statement = new PIPreparedIndexedStatement(
                statementId,
                client,
                language,
                namespace,
                request.getStatement()
        );
        openStatements.put( statementId, statement );
        if ( log.isTraceEnabled() ) {
            log.trace( "created named prepared statement {}", statement );
        }
        return statement;
    }


    public PIPreparedNamedStatement createNamedPreparedInterfaceStatement( PrepareStatementRequest request ) throws PIServiceException {
        String languageName = request.getLanguageName();
        if ( !isSupportedLanguage( languageName ) ) {
            throw new PIServiceException( "Language " + languageName + " not supported." );
        }
        final int statementId = statementIdGenerator.getAndIncrement();
        LogicalNamespace namespace = client.getNamespace();
        if ( request.hasNamespaceName() ) {
            namespace = getNamespace( request.getNamespaceName() );
        }
        assert namespace != null;
        PIPreparedNamedStatement statement = new PIPreparedNamedStatement(
                statementId,
                client,
                QueryLanguage.from( languageName ),
                namespace,
                request.getStatement()
        );
        openStatements.put( statementId, statement );
        if ( log.isTraceEnabled() ) {
            log.trace( "created named prepared statement {}", statement );
        }
        return statement;
    }


    public void closeAll() {
        openUnparameterizedBatches.values().forEach( this::closeBatch );
        openStatements.values().forEach( s -> closeStatement( s.getId() ) );
    }


    private void closeBatch( PIUnparameterizedStatementBatch toClose ) {
        toClose.getStatements().forEach( s -> closeStatement( s.getId() ) );
    }


    private void closeStatement( int statementId ) {
        PIStatement statementToClose = openStatements.get( statementId );
        if ( statementToClose == null ) {
            return;
        }

        openStatements.remove( statementId );
        statementToClose.close();
    }


    public void closeStatementOrBatch( int statementId ) {
        PIUnparameterizedStatementBatch batchToClose = openUnparameterizedBatches.remove( statementId );
        if ( batchToClose != null ) {
            closeBatch( batchToClose );
            return;
        }
        PIStatement statement = openStatements.get( statementId );
        try {
            if ( statement != null ) {
                statement.closeResultsAndCommitIfAuto();
            }
        } finally {
            closeStatement( statementId );
        }
    }


    public PIStatement getStatement( int statementId ) {
        if ( statementId == 0 ) {
            throw new PIServiceException( "Invalid statement id: 0 (possibly uninitialized prism protocol field)" );
        }
        PIStatement statement = openStatements.get( statementId );
        if ( statement == null ) {
            throw new PIServiceException( "A statement with id " + statementId + " does not exist for that client" );
        }
        return statement;
    }


    public void checkAutocommitExecution( boolean graphStatement ) {
        // Graph statements and unfinished write results require a transaction to themselves.
        if ( client.isAutoCommit() && hasOpenResult( (statement, cursor) -> graphStatement
                || statement.getLanguage().dataModel() == DataModel.GRAPH
                || Kind.DML.contains( cursor.iterator().getImplementation().getKind() ) ) ) {
            throw new PIServiceException( "Close or finish the open result before executing another statement in autocommit mode" );
        }
    }


    public void commitIfAutocommitAndNoOpenResults( Transaction transaction ) {
        // Overlapping SQL and MQL results can share the transaction.
        if ( client.isAutoCommit() && transaction == client.getCurrentTransaction()
                && transaction.isActive() && !hasOpenResult( transaction, (statement, cursor) -> true ) ) {
            client.commitCurrentTransactionIfAuto();
        }
    }


    public boolean hasOpenResult( boolean graphOnly ) {
        return hasOpenResult( (statement, cursor) -> !graphOnly || statement.getLanguage().dataModel() == DataModel.GRAPH );
    }


    private boolean hasOpenResult( BiPredicate<PIStatement, ResultCursor> predicate ) {
        Transaction transaction = client.getCurrentTransaction();
        return transaction != null && transaction.isActive() && hasOpenResult( transaction, predicate );
    }


    private boolean hasOpenResult( Transaction transaction, BiPredicate<PIStatement, ResultCursor> predicate ) {
        return openStatements.values().stream().anyMatch( statement -> {
            ResultCursor cursor = statement.getResultCursor();
            return cursor != null && cursor.transaction() == transaction && predicate.test( statement, cursor );
        } );
    }


    private void rebindPreparedStatement( PIPreparedStatement statement ) {
        StatementProcessor.parseWithOpenAutocommitResult( statement );
        statement.closeResultsAndCommitIfAuto();
        Statement s = statement.getStatement();
        if ( s != null && s.getTransaction().getId() != client.getOrCreateNewTransaction().getId() ) {
            statement.setStatement( client.getCurrentTransaction().createStatement() );
        }
    }


    public PIPreparedNamedStatement getNamedPreparedStatement( int statementId ) throws PIServiceException {
        PIStatement statement = getStatement( statementId );
        if ( !(statement instanceof PIPreparedNamedStatement prepared) ) {
            throw new PIServiceException( "A named prepared statement with id " + statementId + " does not exist for that client" );
        }
        if ( prepared.getLanguage().dataModel() != DataModel.GRAPH ) {
            rebindPreparedStatement( prepared );
        }
        return prepared;
    }


    public PIPreparedIndexedStatement getIndexedPreparedStatement( int statementId ) throws PIServiceException {
        PIStatement statement = getStatement( statementId );
        if ( !(statement instanceof PIPreparedIndexedStatement prepared) ) {
            throw new PIServiceException( "A prepared indexed statement with id " + statementId + " does not exist for that client" );
        }
        rebindPreparedStatement( prepared );
        return prepared;
    }


    public boolean isSupportedLanguage( String statementLanguageName ) {
        return LanguageManager.getLanguages()
                .stream()
                .map( QueryLanguage::serializedName )
                .collect( Collectors.toSet() )
                .contains( statementLanguageName );
    }


    public int openStatementCount() {
        return openStatements.size();
    }

}
