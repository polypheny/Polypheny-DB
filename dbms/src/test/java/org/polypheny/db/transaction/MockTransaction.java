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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.entity.Entity;
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
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.VersionedEntryIdentifier;

public class MockTransaction implements Transaction {

    private long id;

    private Set<Lockable> locks;

    private boolean committed = false;


    public MockTransaction( long id ) {
        this.id = id;
        this.locks = new HashSet<>();
    }


    @Override
    public long getId() {
        return id;
    }


    @Override
    public PolyXid getXid() {
        return null;
    }


    @Override
    public Statement createStatement() {
        return null;
    }


    @Override
    public LogicalUser getUser() {
        return null;
    }


    @Override
    public void attachCommitAction( Runnable action ) {
    }


    @Override
    public void attachCommitConstraint( Supplier<Boolean> constraintChecker, String description ) {
    }


    @Override
    public void commit() throws TransactionException {
        releaseAllLocks();
        committed = true;
    }


    @Override
    public void rollback( @Nullable String reason ) throws TransactionException {
        releaseAllLocks();
    }


    @Override
    public void registerInvolvedAdapter( Adapter<?> adapter ) {

    }


    @Override
    public Set<Adapter<?>> getInvolvedAdapters() {
        return Set.of();
    }


    @Override
    public Snapshot getSnapshot() {
        return null;
    }


    @Override
    public boolean isActive() {
        return false;
    }


    @Override
    public JavaTypeFactory getTypeFactory() {
        return null;
    }


    @Override
    public Processor getProcessor( QueryLanguage language ) {
        return null;
    }


    @Override
    public boolean isAnalyze() {
        return false;
    }


    @Override
    public void setAnalyze( boolean analyze ) {

    }


    @Override
    public InformationManager getQueryAnalyzer() {
        return null;
    }


    @Override
    public AtomicBoolean getCancelFlag() {
        return null;
    }


    @Override
    public LogicalNamespace getDefaultNamespace() {
        return null;
    }


    @Override
    public String getOrigin() {
        return "";
    }


    @Override
    public MultimediaFlavor getFlavor() {
        return null;
    }


    @Override
    public void wakeup() {
        Thread.currentThread().interrupt();
    }


    @Override
    public long getNumberOfStatements() {
        return 0;
    }


    @Override
    public DataMigrator getDataMigrator() {
        return null;
    }


    @Override
    public void setUseCache( boolean useCache ) {

    }


    @Override
    public boolean getUseCache() {
        return false;
    }


    @Override
    public void addUsedTable( LogicalTable table ) {

    }


    @Override
    public void removeUsedTable( LogicalTable table ) {

    }


    @Override
    public void getNewEntityConstraints( long entity ) {

    }


    @Override
    public void addNewConstraint( long entityId, LogicalConstraint constraint ) {

    }


    @Override
    public TransactionManager getTransactionManager() {
        return null;
    }


    @Override
    public List<LogicalConstraint> getUsedConstraints( long id ) {
        return List.of();
    }


    @Override
    public void releaseAllLocks() {
        locks.forEach( l -> l.release( this ) );
    }


    @Override
    public void acquireLockable( Lockable lockable, LockType lockType ) {
        lockable.acquire( this, lockType );
        locks.add( lockable );
    }


    @Override
    public long getSequenceNumber() {
        return 0;
    }


    @Override
    public void addWrittenEntitiy( Entity entity ) {
        return;
    }

}
