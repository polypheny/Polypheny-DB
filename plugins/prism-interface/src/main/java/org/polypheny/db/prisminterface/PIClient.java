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

package org.polypheny.db.prisminterface;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.prisminterface.statements.StatementManager;
import org.polypheny.db.transaction.QueryExecutionScope;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

@Slf4j
public class PIClient {

    @Getter
    private final String clientUUID;
    private final LogicalUser catalogUser;
    @Getter
    @Setter
    private Transaction currentTransaction;
    @Getter
    private final TransactionManager transactionManager;
    private final Set<Transaction> isolatedReadTransactions = new LinkedHashSet<>();
    private final int prismMinorVersion;
    @Setter
    private boolean partialBatchResults;
    @Setter
    private boolean transactionBoundaryEvents;
    @Getter
    private final StatementManager statementManager;
    @Getter
    private final PIClientInfoProperties PIClientInfoProperties;
    @Getter
    @Setter
    private boolean isAutoCommit;
    private boolean deferAutoCommit;
    @Getter
    @Setter
    private LogicalNamespace namespace;
    @Getter
    private final MonitoringPage monitoringPage;


    PIClient(
            String clientUUID,
            LogicalUser catalogUser,
            TransactionManager transactionManager,
            LogicalNamespace namespace,
            MonitoringPage monitoringPage,
            boolean isAutoCommit ) {
        this( clientUUID, catalogUser, transactionManager, namespace, monitoringPage, isAutoCommit, 2 );
    }


    PIClient(
            String clientUUID,
            LogicalUser catalogUser,
            TransactionManager transactionManager,
            LogicalNamespace namespace,
            MonitoringPage monitoringPage,
            boolean isAutoCommit,
            int prismMinorVersion ) {
        this.prismMinorVersion = prismMinorVersion;
        this.statementManager = new StatementManager( this );
        this.PIClientInfoProperties = new PIClientInfoProperties();
        this.namespace = namespace;
        this.clientUUID = clientUUID;
        this.catalogUser = catalogUser;
        this.transactionManager = transactionManager;
        this.isAutoCommit = isAutoCommit;
        this.monitoringPage = monitoringPage;
        monitoringPage.addStatementManager( statementManager );
    }


    public boolean supportsGraphValues() {
        return prismMinorVersion >= 3;
    }


    public boolean supportsPartialBatchResults() {
        return prismMinorVersion >= 5 && partialBatchResults;
    }


    List<Transaction> observeTransactionBoundaries( boolean explicitCommit ) {
        if ( prismMinorVersion < 5 || !transactionBoundaryEvents ) {
            return List.of();
        }
        if ( !isAutoCommit ) {
            return hasNoTransaction() ? List.of() : List.of( currentTransaction );
        }
        // Individual auto-commit requests can finish without affecting other open results.
        // An explicit COMMIT finishes every transaction owned by this connection.
        return explicitCommit ? connectionTransactions().stream().filter( Transaction::isActive ).toList() : List.of();
    }


    boolean supportsQueryTimeouts() {
        return prismMinorVersion >= 5;
    }


    boolean supportsClientInfoRemoval() {
        return prismMinorVersion >= 5 && transactionBoundaryEvents;
    }


    public boolean wantsRelationalCypherResults() {
        return PIClientInfoProperties.RELATIONAL_FORMAT.equals( this.PIClientInfoProperties.getProperty( PIClientInfoProperties.CYPHER_RESULT_FORMAT ) );
    }


    public Transaction getOrCreateNewTransaction() {
        if ( hasNoTransaction() ) {
            currentTransaction = transactionManager.startTransaction( catalogUser.id, namespace.id, null, "PrismInterface" );
        }
        QueryExecutionScope.statementStarted( currentTransaction );
        return currentTransaction;
    }


    public Transaction startIsolatedReadTransaction() {
        Transaction transaction = transactionManager.startTransaction( catalogUser.id, namespace.id, null, "PrismInterface" );
        isolatedReadTransactions.add( transaction );
        QueryExecutionScope.statementStarted( transaction );
        return transaction;
    }


    public boolean hasActiveIsolatedReadTransactions() {
        return isolatedReadTransactions.stream().anyMatch( Transaction::isActive );
    }


    /**
     * Creates a read transaction owned by a metadata request, without changing the
     * connection's transaction. The caller must complete this transaction.
     */
    Transaction createMetadataTransaction( long namespaceId ) {
        return transactionManager.startTransaction( catalogUser.id, namespaceId, null, "PrismInterface metadata" );
    }


    public void commitCurrentTransactionIfAuto() {
        commitAutocommitTransaction( currentTransaction );
    }


    public void commitAutocommitTransaction( @Nullable Transaction transaction ) {
        if ( isAutoCommit && !deferAutoCommit ) {
            finishTransaction( transaction, PIClient::commitTransaction );
        }
    }


    public void rollbackAutocommitTransaction( Transaction transaction, @Nullable String reason ) {
        if ( isAutoCommit ) {
            finishTransaction( transaction, owned -> rollbackTransaction( owned, reason ) );
        }
    }


    public <T> T executeAtomicBatch( Supplier<T> execute ) {
        // The request loop serializes operations on this client. Defer automatic
        // commits until the batch completes and preserve the connection's commit mode.
        deferAutoCommit = true;
        try {
            T result = execute.get();
            if ( isAutoCommit ) {
                commitCurrentTransaction();
            }
            return result;
        } catch ( Throwable failure ) {
            // There is no savepoint within a transaction. A failed batch aborts it.
            try {
                rollbackCurrentTransaction( failure.getMessage() );
            } catch ( RuntimeException rollbackFailure ) {
                failure.addSuppressed( rollbackFailure );
            }
            throw failure;
        } finally {
            deferAutoCommit = false;
        }
    }


    public void commitCurrentTransaction() throws PIServiceException {
        finishAllTransactions( PIClient::commitTransaction );
    }


    public void rollbackCurrentTransaction( @Nullable String reason ) throws PIServiceException {
        finishAllTransactions( transaction -> rollbackTransaction( transaction, reason ) );
    }


    void rollbackTimedOutQuery( Transaction transaction ) {
        String reason = "Prism query timed out";
        if ( transaction == currentTransaction || isolatedReadTransactions.contains( transaction ) ) {
            finishTransaction( transaction, owned -> rollbackTransaction( owned, reason ) );
        } else if ( transaction.isActive() ) {
            rollbackTransaction( transaction, reason );
        }
    }


    private List<Transaction> connectionTransactions() {
        List<Transaction> transactions = new ArrayList<>( isolatedReadTransactions );
        if ( currentTransaction != null ) {
            transactions.add( 0, currentTransaction );
        }
        return transactions;
    }


    private void finishAllTransactions( Consumer<Transaction> finish ) {
        RuntimeException failure = null;
        for ( Transaction transaction : connectionTransactions() ) {
            try {
                finishTransaction( transaction, finish );
            } catch ( RuntimeException e ) {
                if ( failure == null ) {
                    failure = e;
                } else {
                    failure.addSuppressed( e );
                }
            }
        }
        if ( failure != null ) {
            throw failure;
        }
    }


    private void finishTransaction( @Nullable Transaction transaction, Consumer<Transaction> finish ) {
        if ( transaction == null || (transaction != currentTransaction && !isolatedReadTransactions.contains( transaction )) ) {
            return;
        }
        try {
            if ( transaction.isActive() ) {
                finish.accept( transaction );
            }
        } finally {
            if ( transaction == currentTransaction ) {
                currentTransaction = null;
            }
            isolatedReadTransactions.remove( transaction );
        }
    }


    private static void commitTransaction( Transaction transaction ) {
        try {
            transaction.commit();
        } catch ( Throwable failure ) {
            if ( transaction.isActive() ) {
                try {
                    rollbackTransaction( transaction, "Prism commit failed" );
                } catch ( RuntimeException rollbackFailure ) {
                    failure.addSuppressed( rollbackFailure );
                }
            }
            throw new PIServiceException( "Committing current transaction failed: " + failure.getMessage(), failure );
        }
    }


    private static void rollbackTransaction( Transaction transaction, @Nullable String reason ) {
        try {
            transaction.getCancelFlag().set( true );
            transaction.rollback( reason );
        } catch ( TransactionException e ) {
            throw new PIServiceException( "Rollback of current transaction failed: " + e.getLocalizedMessage(), e );
        }
    }


    public boolean hasNoTransaction() {
        return currentTransaction == null || !currentTransaction.isActive();
    }


    void prepareForDisposal( @Nullable String reason ) {
        try {
            statementManager.closeAll();
        } finally {
            try {
                rollbackCurrentTransaction( reason );
            } finally {
                monitoringPage.removeStatementManager( statementManager );
            }
        }
    }

}
