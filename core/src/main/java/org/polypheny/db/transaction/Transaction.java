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

package org.polypheny.db.transaction;


import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.transaction.locking.Lockable;


public interface Transaction {

    long getId();

    PolyXid getXid();

    Statement createStatement();

    LogicalUser getUser();

    void attachCommitAction( Runnable action );

    void attachCommitConstraint( Supplier<Boolean> constraintChecker, String description );

    void commit() throws TransactionException;

    /**
     * Rolls back the transaction
     * Null for user initiated
     *
     * @param reason the reason to cancel the transaction.
     * @throws TransactionException if the rollback was not successful.
     */
    void rollback( @Nullable String reason ) throws TransactionException;

    void registerInvolvedAdapter( Adapter<?> adapter );

    Set<Adapter<?>> getInvolvedAdapters();

    Snapshot getSnapshot();

    boolean isActive();

    JavaTypeFactory getTypeFactory();

    Processor getProcessor( QueryLanguage language );

    boolean isAnalyze();

    void setAnalyze( boolean analyze );

    InformationManager getQueryAnalyzer();

    AtomicBoolean getCancelFlag();

    LogicalNamespace getDefaultNamespace();

    String getOrigin();

    MultimediaFlavor getFlavor();

    long getNumberOfStatements();

    DataMigrator getDataMigrator();

    void setUseCache( boolean useCache );

    boolean getUseCache();

    void addUsedTable( LogicalTable table );

    void removeUsedTable( LogicalTable table );

    void getNewEntityConstraints( long entity );

    void addNewConstraint( long entityId, LogicalConstraint constraint );

    TransactionManager getTransactionManager();

    List<LogicalConstraint> getUsedConstraints( long id );

    void releaseAllLocks();

    void acquireLockable( Lockable lockable, Lockable.LockType lockType );

    /**
     * Flavor, how multimedia results should be returned from a store.
     */
    enum MultimediaFlavor {
        DEFAULT, FILE
    }


    /**
     * Transaction Access mode.
     */
    enum AccessMode {

        /**
         * Transaction does not access anything.
         */
        NO_ACCESS,

        /**
         * Transaction is read only.
         */
        READ_ACCESS,

        /**
         * Transaction is used for write only.
         */
        WRITE_ACCESS,

        /**
         * Transaction is used for both read and write.
         */
        READWRITE_ACCESS
    }

}
