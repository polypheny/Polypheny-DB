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

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

@Slf4j
public class ProtoInterfaceClient {

    private String clientUUID;
    private CatalogUser catalogUser;
    private CatalogDatabase catalogDatabase;
    private LogicalNamespace logicalNamespace;
    private Transaction currentTransaction;
    private TransactionManager transactionManager;

    @Setter
    @Getter
    private Map<String, String> statementProperties;

    private InterfaceClientProperties clientProperties;
    private int minorApiVersion;
    private int majorApiVersion;


    private ProtoInterfaceClient( Builder connectionBuilder ) {
        this.clientUUID = connectionBuilder.clientUUID;
        this.catalogUser = connectionBuilder.catalogUser;
        this.logicalNamespace = connectionBuilder.logicalNamespace;
        this.currentTransaction = connectionBuilder.currentTransaction;
        this.transactionManager = connectionBuilder.transactionManager;
        this.clientProperties = connectionBuilder.clientProperties;
        this.majorApiVersion = connectionBuilder.majorApiVersion;
        this.minorApiVersion = connectionBuilder.minorApiVersion;
    }


    public String getClientUUID() {
        return clientUUID;
    }


    public synchronized Transaction getCurrentOrCreateNewTransaction() {
        if ( currentTransaction == null || !currentTransaction.isActive() ) {
            currentTransaction = transactionManager.startTransaction( catalogUser, logicalNamespace, false, "ProtoInterface" );
        }
        return currentTransaction;
    }


    public Transaction getCurrentTransaction() {
        return currentTransaction;
    }


    public synchronized void commitCurrentTransaction() {
        if ( log.isTraceEnabled() ) {
            log.trace( "commitCurrentTransaction() for client: {}", clientUUID );
        }
        if ( hasNoTransaction() ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "No open transaction for client: {}", clientUUID );
            }
            return;
        }
        try {
            currentTransaction.commit();
        } catch ( TransactionException e ) {
            throw new ProtoInterfaceServiceException( "Committing current transaction failed: " + e.getLocalizedMessage() );
        } finally {
            endCurrentTransaction();
        }
    }


    public synchronized void rollbackCurrentTransaction() {
        if ( log.isTraceEnabled() ) {
            log.trace( "commitCurrentTransaction() for client: {}", clientUUID );
        }
        if ( hasNoTransaction() ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "No open transaction for client: {}", clientUUID );
            }
            return;
        }
        try {
            currentTransaction.rollback();
        } catch ( TransactionException e ) {
            throw new ProtoInterfaceServiceException( "Rollback of current transaction failed: " + e.getLocalizedMessage() );
        } finally {
            endCurrentTransaction();
        }
    }


    private synchronized void endCurrentTransaction() {
        currentTransaction = null;
    }


    public boolean hasNoTransaction() {
        return currentTransaction == null || !currentTransaction.isActive();
    }


    public boolean isAutocommit() throws IllegalArgumentException{
        return clientProperties.isAutoCommit();
    }


    public static Builder newBuilder() {
        return new Builder();
    }


    static class Builder {

        private String clientUUID;
        private CatalogUser catalogUser;
        private LogicalNamespace logicalNamespace;
        private Transaction currentTransaction;
        private TransactionManager transactionManager;
        private InterfaceClientProperties clientProperties;
        private int minorApiVersion;
        private int majorApiVersion;


        private Builder() {
        }


        public Builder setClientUUID( String clientUUID ) {
            this.clientUUID = clientUUID;
            return this;
        }


        public Builder setCatalogUser( CatalogUser catalogUser ) {
            this.catalogUser = catalogUser;
            return this;
        }


        public Builder setLogicalNamespace( LogicalNamespace logicalNamespace ) {
            this.logicalNamespace = logicalNamespace;
            return this;
        }



        public Builder setTransactionManager( TransactionManager transactionManager ) {
            this.transactionManager = transactionManager;
            return this;
        }


        public Builder setClientProperties( InterfaceClientProperties clientProperties ) {
            this.clientProperties = clientProperties;
            return this;
        }


        public Builder setMajorApiVersion( int majorApiVersion ) {
            this.majorApiVersion = majorApiVersion;
            return this;
        }


        public Builder setMinorApiVersion( int minorApiVersion ) {
            this.minorApiVersion = minorApiVersion;
            return this;
        }


        public ProtoInterfaceClient build() {
            return new ProtoInterfaceClient( this );
        }

    }

}
