/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.QueryProcessor;
import org.polypheny.db.SqlProcessor;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.CatalogManagerImpl;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.CatalogTransactionException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationDuration;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.jdbc.ContextImpl;
import org.polypheny.db.jdbc.JavaTypeFactoryImpl;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.DataContextImpl;
import org.polypheny.db.processing.QueryProviderImpl;
import org.polypheny.db.processing.SqlProcessorImpl;
import org.polypheny.db.processing.VolcanoQueryProcessor;
import org.polypheny.db.router.RouterManager;
import org.polypheny.db.routing.Router;
import org.polypheny.db.schema.PolySchemaBuilder;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.sql.parser.SqlParser.SqlParserConfig;
import org.polypheny.db.statistic.StatisticsManager;


@Slf4j
public class TransactionImpl implements Transaction, Comparable {

    @Getter
    private final PolyXid xid;

    @Getter
    private final AtomicBoolean cancelFlag = new AtomicBoolean();

    private QueryProcessor queryProcessor;
    private Catalog catalog;

    @Getter
    private CatalogUser user;
    @Getter
    private CatalogSchema defaultSchema;
    private CatalogDatabase database;

    private TransactionManagerImpl transactionManager;

    @Getter
    private final boolean analyze;

    private final List<String> changedTables = new ArrayList<>();
    private DataContext dataContext = null;
    private ContextImpl prepareContext = null;

    @Getter
    private List<Store> involvedStores = new CopyOnWriteArrayList<>();

    private PolyphenyDbSchema cachedSchema = null;

    private InformationDuration duration = null;

    private Set<Lock> lockList = new HashSet<>();


    TransactionImpl(
            PolyXid xid,
            TransactionManagerImpl transactionManager,
            CatalogUser user,
            CatalogSchema defaultSchema,
            CatalogDatabase database,
            boolean analyze ) {
        this.xid = xid;
        this.transactionManager = transactionManager;
        this.user = user;
        this.defaultSchema = defaultSchema;
        this.database = database;
        this.analyze = analyze;
    }


    @Override
    public QueryProcessor getQueryProcessor() {
        if ( queryProcessor == null ) {
            queryProcessor = new VolcanoQueryProcessor( this );
        }
        return queryProcessor;
    }


    @Override
    public SqlProcessor getSqlProcessor( SqlParserConfig parserConfig ) {
        return new SqlProcessorImpl( this, parserConfig );
    }


    @Override
    public Catalog getCatalog() {
        if ( catalog == null ) {
            catalog = CatalogManagerImpl.getInstance().getCatalog( xid );
        }
        return catalog;
    }


    @Override
    public PolyphenyDbSchema getSchema() {
        if ( cachedSchema == null ) {
            cachedSchema = PolySchemaBuilder.getInstance().getCurrent( this );
        }
        return cachedSchema;
    }


    @Override
    public InformationManager getQueryAnalyzer() {
        return InformationManager.getInstance( xid.toString() );
    }


    @Override
    public void registerInvolvedStore( Store store ) {
        if ( !involvedStores.contains( store ) ) {
            involvedStores.add( store );
        }
    }


    @Override
    public void commit() throws TransactionException {
        try {
            // Prepare to commit changes on all involved stores and the catalog
            boolean okToCommit = true;
            if ( catalog != null ) {
                okToCommit &= catalog.prepare();
            }
            if ( RuntimeConfig.TWO_PC_MODE.getBoolean() ) {
                for ( Store store : involvedStores ) {
                    okToCommit &= store.prepare( xid );
                }
            }

            if ( okToCommit ) {
                // Commit changes
                if ( catalog != null ) {
                    catalog.commit();
                }
                for ( Store store : involvedStores ) {
                    store.commit( xid );
                }

                if ( changedTables.size() > 0 ) {
                    StatisticsManager.getInstance().apply( changedTables );
                }

            } else {
                log.error( "Unable to prepare all involved entities for commit. Rollback changes!" );
                rollback();
                throw new TransactionException( "Unable to prepare all involved entities for commit. Changes have been rolled back." );
            }

            // Release locks
            LockManager.INSTANCE.removeTransaction( this );

            transactionManager.removeTransaction( xid );
        } catch ( CatalogTransactionException e ) {
            log.error( "Exception while committing changes. Execution rollback!" );
            rollback();
            throw new TransactionException( e );
        } finally {
            cachedSchema = null;
        }


    }


    @Override
    public void rollback() throws TransactionException {
        try {
            //  Rollback changes to the stores
            for ( Store store : involvedStores ) {
                store.rollback( xid );
            }

            // Rollback changes to the catalog
            try {
                if ( catalog != null ) {
                    catalog.rollback();
                }
            } catch ( CatalogTransactionException e ) {
                throw new TransactionException( e );
            } finally {
                cachedSchema = null;
            }
        } finally {
            // Release locks
            LockManager.INSTANCE.removeTransaction( this );
        }
    }


    @Override
    public DataContext getDataContext() {
        if ( dataContext == null ) {
            Map<String, Object> map = new LinkedHashMap<>();
            // Avoid overflow
            int queryTimeout = RuntimeConfig.QUERY_TIMEOUT.getInteger();
            if ( queryTimeout > 0 && queryTimeout < Integer.MAX_VALUE / 1000 ) {
                map.put( DataContext.Variable.TIMEOUT.camelName, queryTimeout * 1000L );
            }

            final AtomicBoolean cancelFlag;
            cancelFlag = getCancelFlag();
            map.put( DataContext.Variable.CANCEL_FLAG.camelName, cancelFlag );
            if ( RuntimeConfig.SPARK_ENGINE.getBoolean() ) {
                return new SlimDataContext();
            }
            dataContext = new DataContextImpl( new QueryProviderImpl(), map, getSchema(), getTypeFactory(), this );
        }
        return dataContext;
    }


    @Override
    public JavaTypeFactory getTypeFactory() {
        return new JavaTypeFactoryImpl();
    }


    @Override
    public ContextImpl getPrepareContext() {
        if ( prepareContext == null ) {
            prepareContext = new ContextImpl(
                    getSchema(),
                    getDataContext(),
                    defaultSchema.name,
                    database.id,
                    user.id,
                    this );
        }
        return prepareContext;
    }


    @Override
    public PolyphenyDbCatalogReader getCatalogReader() {
        return new PolyphenyDbCatalogReader(
                PolyphenyDbSchema.from( getSchema().plus() ),
                PolyphenyDbSchema.from( getSchema().plus() ).path( null ),
                getTypeFactory() );
    }


    /*
     * This should be called in the query interfaces for every new query before we start with processing it.
     */
    @Override
    public void resetQueryProcessor() {
        queryProcessor = null;
        dataContext = null;
        prepareContext = null;
        cachedSchema = null; // TODO: This should no be necessary.
    }


    @Override
    public void addChangedTable( String qualifiedTableName ) {
        if ( !this.changedTables.contains( qualifiedTableName ) ) {
            log.debug( "Add changed table: {}", qualifiedTableName );
            this.changedTables.add( qualifiedTableName );
        }
    }


    @Override
    public Router getRouter() {
        return RouterManager.getInstance().getRouter();
    }


    @Override
    public InformationDuration getDuration() {
        if ( duration == null ) {
            InformationManager im = getQueryAnalyzer();
            InformationGroup group = new InformationGroup( "p1", "Processing" );
            im.addGroup( group );
            duration = new InformationDuration( group );
            im.registerInformation( duration );
        }
        return duration;
    }


    @Override
    public int compareTo( @NotNull Object o ) {
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

    // For locking


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

}
