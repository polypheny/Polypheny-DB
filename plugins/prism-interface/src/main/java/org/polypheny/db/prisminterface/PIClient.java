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

import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.prisminterface.statements.StatementManager;
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
    private final TransactionManager transactionManager;
    private final int prismMinorVersion;
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


    public boolean wantsRelationalCypherResults() {
        return PIClientInfoProperties.RELATIONAL_FORMAT.equals( this.PIClientInfoProperties.getProperty( PIClientInfoProperties.CYPHER_RESULT_FORMAT ) );
    }


    public Transaction getOrCreateNewTransaction() {
        if ( hasNoTransaction() ) {
            currentTransaction = transactionManager.startTransaction( catalogUser.id, namespace.id, null, "PrismInterface" );
        }
        return currentTransaction;
    }


    /**
     * Creates a read transaction owned by a metadata request, without changing the
     * connection's transaction. The caller must complete this transaction.
     */
    Transaction createMetadataTransaction( long namespaceId ) {
        return transactionManager.startTransaction( catalogUser.id, namespaceId, null, "PrismInterface metadata" );
    }


    public void commitCurrentTransactionIfAuto() {
        if ( !isAutoCommit || deferAutoCommit ) {
            return;
        }
        commitCurrentTransactionUnsynchronized();
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
        commitCurrentTransactionUnsynchronized();
    }


    private void commitCurrentTransactionUnsynchronized() throws PIServiceException {
        if ( hasNoTransaction() ) {
            return;
        }
        try {
            currentTransaction.commit();
        } catch ( Throwable e ) {
            throw new PIServiceException( "Committing current transaction failed: " + e.getMessage() );
        } finally {
            clearCurrentTransaction();
        }
    }


    public void rollbackCurrentTransaction( @Nullable String reason ) throws PIServiceException {
        if ( hasNoTransaction() ) {
            return;
        }
        try {
            currentTransaction.getCancelFlag().set( true );
            currentTransaction.rollback( reason );
        } catch ( TransactionException e ) {
            throw new PIServiceException( "Rollback of current transaction failed: " + e.getLocalizedMessage() );
        } finally {
            clearCurrentTransaction();
        }
    }


    private void clearCurrentTransaction() {
        currentTransaction = null;
    }


    public boolean hasNoTransaction() {
        return currentTransaction == null || !currentTransaction.isActive();
    }


    void prepareForDisposal( @Nullable String reason ) {
        statementManager.closeAll();
        rollbackCurrentTransaction( reason );
        monitoringPage.removeStatementManager( statementManager );
    }

}
