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

package org.polypheny.db.adapter.index;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.index.Index.IndexFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;


public class IndexManager {

    private static final IndexManager INSTANCE = new IndexManager();

    private static List<IndexFactory> INDEX_FACTORIES = Arrays.asList(
            new CoWHashIndex.Factory(),
            new CowMultiHashIndex.Factory()
    );

    private final Map<Long, Index> indexById = new HashMap<>();
    private final Map<String, Index> indexByName = new HashMap<>();
    private final Map<PolyXid, List<Index>> openTransactions = new HashMap<>();
    private TransactionManager transactionManager = null;


    public static IndexManager getInstance() {
        return INSTANCE;
    }


    private IndexManager() {
        // intentionally empty
    }

    void begin( PolyXid xid, Index index ) {
        if (!openTransactions.containsKey( xid )) {
            openTransactions.put( xid, new ArrayList<>(  ) );
        }
        openTransactions.get( xid ).add( index );
    }

    public void commit( PolyXid xid ) {
        List<Index> idxs = openTransactions.remove( xid );
        if (idxs == null) {
            return;
        }
        for (final Index idx : idxs) {
            idx.commit( xid );
        }
    }

    public void rollback( PolyXid xid ) {
        List<Index> idxs = openTransactions.remove( xid );
        if (idxs == null) {
            return;
        }
        for (final Index idx : idxs) {
            idx.rollback( xid );
        }
    }


    public void initialize( final TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }


    public void restoreIndices() throws UnknownSchemaException, GenericCatalogException, UnknownTableException, UnknownKeyException, UnknownDatabaseException, UnknownUserException, TransactionException {
        for ( final CatalogIndex index : Catalog.getInstance().getIndices() ) {
            System.err.println( "Restoring index: " + index.name );
            addIndex( index );
        }
    }


    public void addIndex( final CatalogIndex index ) throws UnknownSchemaException, GenericCatalogException, UnknownTableException, UnknownKeyException, UnknownUserException, UnknownDatabaseException, TransactionException {
        addIndex( index, null );
    }


    public void addIndex( final CatalogIndex index, final Transaction transaction ) throws UnknownSchemaException, GenericCatalogException, UnknownTableException, UnknownKeyException, UnknownUserException, UnknownDatabaseException, TransactionException {
        addIndex( index.id, index.name, index.key, index.type, index.unique, transaction );
    }


    protected void addIndex( final long id, final String name, final CatalogKey key, final IndexType type, final boolean unique, final Transaction transaction ) throws UnknownSchemaException, GenericCatalogException, UnknownTableException, UnknownKeyException, UnknownDatabaseException, UnknownUserException, TransactionException {
        final IndexFactory factory = INDEX_FACTORIES.stream().filter( it -> it.isUnique() == unique && it.getType() == type ).findFirst().orElseThrow( IllegalArgumentException::new );
        final CatalogTable table = Catalog.getInstance().getTable( key.tableId );
        final CatalogPrimaryKey pk = Catalog.getInstance().getPrimaryKey( table.primaryKey );
        final Index index = factory.create(
                id, name, Catalog.getInstance().getSchema( key.schemaId ),
                table,
                key.getColumnNames(),
                pk.getColumnNames() );
        indexById.put( id, index );
        indexByName.put( name, index );
        System.err.println( String.format( "Creating %s for key %s", index.getClass().getSimpleName(), key ) );
        final Transaction tx = transaction != null ? transaction : transactionManager.startTransaction( "pa", "APP", false );
        try {
            index.rebuild( tx );
            if (transaction == null) {
                tx.commit();
            }
        } catch ( TransactionException e ) {
            tx.rollback();
            throw e;
        }
    }


    public void deleteIndex( final CatalogIndex index ) {
        deleteIndex( index.id );
    }


    public void deleteIndex( final long indexId ) {
        final Index idx = indexById.remove( indexId );
        indexByName.remove( idx.name );
    }


    public Index getIndex( CatalogSchema schema, CatalogTable table, List<String> columns ) {
        return this.indexById.values().stream().filter( index ->
                index.schema.equals( schema )
                        && index.table.equals( table )
                        && index.columns.equals( columns )
        ).findFirst().orElse( null );
    }


    public Index getIndex( CatalogSchema schema, CatalogTable table, List<String> columns, IndexType type, boolean unique ) {
        return this.indexById.values().stream().filter( index ->
                index.schema.equals( schema )
                        && index.table.equals( table )
                        && index.columns.equals( columns )
                        && index.getType() == type
                        && index.isUnique() == unique
        ).findFirst().orElse( null );
    }


    public List<Index> getIndices( CatalogSchema schema, CatalogTable table ) {
        return this.indexById.values().stream().filter( index -> index.schema.equals( schema ) && index.table.equals( table ) ).collect( Collectors.toList() );
    }

}
