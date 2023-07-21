/*
 * Copyright 2019-2023 The Polypheny Project
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
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.protointerface.proto.ConnectionProperties;
import org.polypheny.db.protointerface.statements.StatementManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

@Slf4j
public class PIClient {
    @Getter
    private String clientUUID;
    private CatalogUser catalogUser;
    private LogicalNamespace logicalNamespace;
    private Transaction currentTransaction;
    private TransactionManager transactionManager;
    @Getter
    private StatementManager statementManager;
    @Getter
    private PIClientProperties properties;
    @Getter
    private PIClientInfoProperties PIClientInfoProperties;
    private boolean isActive;


    private PIClient(Builder connectionBuilder) {
        this.statementManager = new StatementManager(this);
        this.clientUUID = connectionBuilder.clientUUID;
        this.catalogUser = connectionBuilder.catalogUser;
        this.logicalNamespace = connectionBuilder.logicalNamespace;
        this.transactionManager = connectionBuilder.transactionManager;
        this.properties = connectionBuilder.clientProperties;
        this.PIClientInfoProperties = new PIClientInfoProperties();
        this.isActive = true;
    }

    public void setClientProperties(ConnectionProperties connectionProperties) {
        properties.set(connectionProperties);
    }


    public Transaction getCurrentOrCreateNewTransaction() {
        synchronized (this) {
            if (currentTransaction == null || !currentTransaction.isActive()) {
                //TODO TH: can a single transaction contain changes to different namespaces
                currentTransaction = transactionManager.startTransaction(catalogUser, logicalNamespace, false, "ProtoInterface");
            }
            return currentTransaction;
        }
    }


    public Transaction getCurrentTransaction() {
        return currentTransaction;
    }

    public void commitCurrentTransactionIfAuto() {
        if (!properties.isAutoCommit()) {
            return;
        }
        commitCurrentTransactionUnsynchronized();
    }

    public void commitCurrentTransaction() {
        synchronized (this) {
            commitCurrentTransactionUnsynchronized();
        }
    }

    private void commitCurrentTransactionUnsynchronized() {
        if (hasNoTransaction()) {
            return;
        }
        try {
            currentTransaction.commit();
        } catch (TransactionException e) {
            throw new PIServiceException("Committing current transaction failed: " + e.getMessage());
        } finally {
            endCurrentTransaction();
        }
    }


    public void rollbackCurrentTransaction() {
        synchronized (this) {
            if (hasNoTransaction()) {
                return;
            }
            try {
                currentTransaction.rollback();
            } catch (TransactionException e) {
                throw new PIServiceException("Rollback of current transaction failed: " + e.getLocalizedMessage());
            } finally {
                endCurrentTransaction();
            }
        }
    }


    private void endCurrentTransaction() {
        currentTransaction = null;
    }


    public boolean hasNoTransaction() {
        return currentTransaction == null || !currentTransaction.isActive();
    }


    public void prepareForDisposal() {
        statementManager.closeAll();
        if (!hasNoTransaction()) {
            rollbackCurrentTransaction();
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public void setIsActive() {
        isActive = true;
    }

    public boolean returnAndResetIsActive() {
        boolean oldIsActive = isActive;
        isActive = false;
        return oldIsActive;
    }


    static class Builder {

        private String clientUUID;
        private CatalogUser catalogUser;
        private LogicalNamespace logicalNamespace;
        private TransactionManager transactionManager;
        private PIClientProperties clientProperties;


        private Builder() {
        }


        public Builder setClientUUID(String clientUUID) {
            this.clientUUID = clientUUID;
            return this;
        }


        public Builder setCatalogUser(CatalogUser catalogUser) {
            this.catalogUser = catalogUser;
            return this;
        }


        public Builder setLogicalNamespace(LogicalNamespace logicalNamespace) {
            this.logicalNamespace = logicalNamespace;
            return this;
        }


        public Builder setTransactionManager(TransactionManager transactionManager) {
            this.transactionManager = transactionManager;
            return this;
        }


        public Builder setClientProperties(PIClientProperties clientProperties) {
            this.clientProperties = clientProperties;
            return this;
        }


        public PIClient build() {
            return new PIClient(this);
        }

    }

}
