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

package org.polypheny.db.protointerface;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.protointerface.statements.StatementManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

@Slf4j
public class PIClient {

    @Getter
    private final String clientUUID;
    private final LogicalUser catalogUser;
    private Transaction currentTransaction;
    private final TransactionManager transactionManager;
    @Getter
    private final StatementManager statementManager;
    @Getter
    private final PIClientInfoProperties PIClientInfoProperties;
    @Getter
    @Setter
    private boolean isAutoCommit;
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


    public Transaction getOrCreateNewTransaction() {
        //synchronized ( this ) {
        if ( hasNoTransaction() ) {
            currentTransaction = transactionManager.startTransaction( catalogUser.id, namespace.id, false, "PrismInterface" );
        }
        return currentTransaction;
        //}
    }


    public void commitCurrentTransactionIfAuto() {
        if ( !isAutoCommit ) {
            return;
        }
        commitCurrentTransactionUnsynchronized();
    }


    public void commitCurrentTransaction() throws PIServiceException {
        //synchronized ( this ) {
        commitCurrentTransactionUnsynchronized();
        //}
    }


    private void commitCurrentTransactionUnsynchronized() throws PIServiceException {
        if ( hasNoTransaction() ) {
            return;
        }
        try {
            currentTransaction.commit();
        } catch ( TransactionException e ) {
            throw new PIServiceException( "Committing current transaction failed: " + e.getMessage() );
        } finally {
            clearCurrentTransaction();
        }
    }


    public void rollbackCurrentTransaction() throws PIServiceException {
        //synchronized ( this ) {
        if ( hasNoTransaction() ) {
            return;
        }
        try {
            currentTransaction.getCancelFlag().set( true );
            currentTransaction.rollback();
        } catch ( TransactionException e ) {
            throw new PIServiceException( "Rollback of current transaction failed: " + e.getLocalizedMessage() );
        } finally {
            clearCurrentTransaction();
        }
        //}
    }


    private void clearCurrentTransaction() {
        currentTransaction = null;
    }


    public boolean hasNoTransaction() {
        return currentTransaction == null || !currentTransaction.isActive();
    }


    void prepareForDisposal() {
        statementManager.closeAll();
        rollbackCurrentTransaction();
        monitoringPage.removeStatementManager( statementManager );
    }

}
