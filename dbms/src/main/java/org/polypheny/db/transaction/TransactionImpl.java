/*
 * Copyright 2019-2022 The Polypheny Project
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


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer.EnforcementInformation;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogKey.EnforcementTime;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.piglet.PigProcessorImpl;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.ConstraintEnforceAttacher;
import org.polypheny.db.processing.CypherProcessorImpl;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.processing.DataMigratorImpl;
import org.polypheny.db.processing.JsonRelProcessorImpl;
import org.polypheny.db.processing.MqlProcessorImpl;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.processing.SqlProcessorImpl;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.view.MaterializedViewManager;


@Slf4j
public class TransactionImpl implements Transaction, Comparable<Object> {

    private static final AtomicLong TRANSACTION_COUNTER = new AtomicLong();

    @Getter
    private final long id;

    @Getter
    private final PolyXid xid;

    @Getter
    private final AtomicBoolean cancelFlag = new AtomicBoolean();

    @Getter
    private final CatalogUser user;
    @Getter
    private final CatalogSchema defaultSchema;
    @Getter
    private final CatalogDatabase database;

    private final TransactionManagerImpl transactionManager;

    @Getter
    private final String origin;

    @Getter
    private final MultimediaFlavor flavor;

    @Getter
    @Setter
    private boolean analyze;

    private final List<Statement> statements = new ArrayList<>();

    private final List<String> changedTables = new ArrayList<>();

    @Getter
    private final Set<CatalogTable> catalogTables = new TreeSet<>();

    @Getter
    private final List<Adapter> involvedAdapters = new CopyOnWriteArrayList<>();

    private final Set<Lock> lockList = new HashSet<>();
    private boolean useCache = true;

    private boolean acceptsOutdated = false;

    private AccessMode accessMode = AccessMode.NO_ACCESS;

    @Getter
    private final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();


    TransactionImpl(
            PolyXid xid,
            TransactionManagerImpl transactionManager,
            CatalogUser user,
            CatalogSchema defaultSchema,
            CatalogDatabase database,
            boolean analyze,
            String origin,
            MultimediaFlavor flavor ) {
        this.id = TRANSACTION_COUNTER.getAndIncrement();
        this.xid = xid;
        this.transactionManager = transactionManager;
        this.user = user;
        this.defaultSchema = defaultSchema;
        this.database = database;
        this.analyze = analyze;
        this.origin = origin;
        this.flavor = flavor;
    }


    @Override
    public PolyphenyDbSchema getSchema() {
        return PolySchemaBuilder.getInstance().getCurrent();
    }


    @Override
    public InformationManager getQueryAnalyzer() {
        return InformationManager.getInstance( xid.toString() );
    }


    @Override
    public void registerInvolvedAdapter( Adapter adapter ) {
        if ( !involvedAdapters.contains( adapter ) ) {
            involvedAdapters.add( adapter );
        }
    }


    @Override
    public void commit() throws TransactionException {
        if ( !isActive() ) {
            log.trace( "This transaction has already been finished!" );
            return;
        }
        // Prepare to commit changes on all involved adapters and the catalog
        boolean okToCommit = true;
        if ( RuntimeConfig.TWO_PC_MODE.getBoolean() ) {
            for ( Adapter adapter : involvedAdapters ) {
                okToCommit &= adapter.prepare( xid );
            }
        }

        if ( !catalogTables.isEmpty() ) {
            Statement statement = createStatement();
            QueryProcessor processor = statement.getQueryProcessor();
            List<EnforcementInformation> infos = ConstraintEnforceAttacher
                    .getConstraintAlg( catalogTables, statement, EnforcementTime.ON_COMMIT );
            List<PolyImplementation> results = infos
                    .stream()
                    .map( s -> processor.prepareQuery( AlgRoot.of( s.getControl(), Kind.SELECT ), s.getControl().getCluster().getTypeFactory().builder().build(), false, true, false ) ).collect( Collectors.toList() );
            List<List<List<Object>>> rows = results.stream().map( r -> r.getRows( statement, -1 ) ).filter( r -> r.size() != 0 ).collect( Collectors.toList() );
            if ( rows.size() != 0 ) {
                Integer index = (Integer) rows.get( 0 ).get( 0 ).get( 1 );
                rollback();
                throw new TransactionException( infos.get( 0 ).getErrorMessages().get( index ) + "\nThere are violated constraints, the transaction was rolled back!" );
            }
        }

        if ( okToCommit ) {
            // Commit changes
            for ( Adapter adapter : involvedAdapters ) {
                adapter.commit( xid );
            }

            this.statements.forEach( statement -> {
                if ( statement.getMonitoringEvent() != null ) {
                    StatementEvent eventData = statement.getMonitoringEvent();
                    eventData.setCommitted( true );
                    MonitoringServiceProvider.getInstance().monitorEvent( eventData );
                }
            } );

            IndexManager.getInstance().commit( this.xid );
        } else {
            log.error( "Unable to prepare all involved entities for commit. Rollback changes!" );
            rollback();
            throw new TransactionException( "Unable to prepare all involved entities for commit. Changes have been rolled back." );
        }
        // Free resources hold by statements
        statements.forEach( Statement::close );

        // Release locks
        LockManager.INSTANCE.removeTransaction( this );
        // Remove transaction
        transactionManager.removeTransaction( xid );

        // Handover information about commit to Materialized Manager
        MaterializedViewManager.getInstance().updateCommittedXid( xid );
    }


    @Override
    public void rollback() throws TransactionException {
        if ( !isActive() ) {
            log.trace( "This transaction has already been finished!" );
            return;
        }
        try {
            //  Rollback changes to the adapters
            for ( Adapter adapter : involvedAdapters ) {
                adapter.rollback( xid );
            }
            IndexManager.getInstance().rollback( this.xid );
            Catalog.getInstance().rollback();
            // Free resources hold by statements
            statements.forEach( statement -> {
                if ( statement.getMonitoringEvent() != null ) {
                    StatementEvent eventData = statement.getMonitoringEvent();
                    eventData.setCommitted( false );
                    MonitoringServiceProvider.getInstance().monitorEvent( eventData );
                }
                statement.close();
            } );
        } finally {
            // Release locks
            LockManager.INSTANCE.removeTransaction( this );
            // Remove transaction
            transactionManager.removeTransaction( xid );
        }
    }


    @Override
    public boolean isActive() {
        return transactionManager.isActive( xid );
    }


    @Override
    public PolyphenyDbCatalogReader getCatalogReader() {
        return new PolyphenyDbCatalogReader(
                PolyphenyDbSchema.from( getSchema().plus() ),
                PolyphenyDbSchema.from( getSchema().plus() ).path( null ),
                getTypeFactory() );
    }


    @Override
    public Processor getProcessor( QueryLanguage language ) {
        // note dl, while caching the processors works in most cases,
        // it can lead to validator bleed when using multiple simultaneous insert for example
        // caching therefore is not possible atm
        switch ( language ) {
            case SQL:
                return new SqlProcessorImpl();
            case REL_ALG:
                return new JsonRelProcessorImpl();
            case MONGO_QL:
                return new MqlProcessorImpl();
            case PIG:
                return new PigProcessorImpl();
            case CYPHER:
                return new CypherProcessorImpl();
            default:
                throw new RuntimeException( "This language seems to not be supported!" );
        }
    }


    @Override
    public StatementImpl createStatement() {
        StatementImpl statement = new StatementImpl( this );
        statements.add( statement );
        return statement;
    }


    @Override
    public void addChangedTable( String qualifiedTableName ) {
        if ( !this.changedTables.contains( qualifiedTableName ) ) {
            if ( log.isDebugEnabled() ) {
                log.debug( "Add changed table: {}", qualifiedTableName );
            }
            this.changedTables.add( qualifiedTableName );
        }
    }


    @Override
    public int compareTo( @NonNull Object o ) {
        Transaction that = (Transaction) o;
        return this.xid.hashCode() - that.getXid().hashCode();
    }


    @Override
    public int hashCode() {
        return Objects.hash( xid );
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        Transaction that = (Transaction) o;
        return xid.equals( that.getXid() );
    }


    @Override
    public void setUseCache( boolean useCache ) {
        this.useCache = useCache;
    }


    @Override
    public boolean getUseCache() {
        return this.useCache;
    }


    /**
     * Used to specify if a TX was started using freshness tolerance levels and
     * therefore allows the usage of outdated replicas.
     * <p>
     * If this is active no DML operations are possible for this TX.
     * If however a DML operation was already executed by this TX.
     * This TX can now support no more freshness-related queries.
     */
    @Override
    public void setAcceptsOutdated( boolean acceptsOutdated ) {
        this.acceptsOutdated = acceptsOutdated;
    }


    @Override
    public boolean acceptsOutdated() {
        return this.acceptsOutdated;
    }


    @Override
    public AccessMode getAccessMode() {
        return accessMode;
    }


    @Override
    public void updateAccessMode( AccessMode accessModeCandidate ) {

        // If TX is already in RW access we can skip immediately
        if ( this.accessMode.equals( AccessMode.READWRITE_ACCESS ) || this.accessMode.equals( accessModeCandidate ) ) {
            return;
        }

        switch ( accessModeCandidate ) {
            case WRITE_ACCESS:
                if ( this.accessMode.equals( AccessMode.READ_ACCESS ) ) {
                    accessModeCandidate = AccessMode.READWRITE_ACCESS;
                }
                break;

            case READ_ACCESS:
                if ( this.accessMode.equals( AccessMode.WRITE_ACCESS ) ) {
                    accessModeCandidate = AccessMode.READWRITE_ACCESS;
                }
                break;

            case NO_ACCESS:
                throw new RuntimeException( "Not possible to reset the access mode to NO_ACCESS" );
        }

        // If nothing else has matched so far. It's safe to simply use the input
        this.accessMode = accessModeCandidate;

    }

    //
    // For locking
    //


    Set<Lock> getLocks() {
        return lockList;
    }


    void addLock( Lock lock ) {
        lockList.add( lock );
    }


    void removeLock( Lock lock ) {
        lockList.remove( lock );
    }


    void abort() {
        Thread.currentThread().interrupt();
    }


    @Override
    public long getNumberOfStatements() {
        return statements.size();
    }


    @Override
    public DataMigrator getDataMigrator() {
        return new DataMigratorImpl();
    }

}
