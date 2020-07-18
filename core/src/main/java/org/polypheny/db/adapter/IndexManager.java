package org.polypheny.db.adapter;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
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
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;


public class IndexManager {

    private static final IndexManager INSTANCE = new IndexManager();

    private final Map<Long, Index> indexById = new HashMap<>();
    private final Map<String, Index> indexByName = new HashMap<>();
    private TransactionManager transactionManager = null;


    public static IndexManager getInstance() {
        return INSTANCE;
    }


    private IndexManager() {
        // intentionally empty
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
        // TODO(s3lph): INDEX TYPES
        final Index index;
        if ( Catalog.getInstance().isPrimaryKey( key.id ) ) {
            index = new HashIndex(
                    id, name, unique, Catalog.getInstance().getSchema( key.schemaId ),
                    Catalog.getInstance().getTable( key.tableId ),
                    Catalog.getInstance().getTable( key.tableId ),
                    key.getColumnNames(),
                    key.getColumnNames() );
        } else if (Catalog.getInstance().isForeignKey( key.id ) ) {
            final CatalogForeignKey cfk = Catalog.getInstance().getForeignKeys( key.tableId ).stream().filter( x -> x.id == key.id ).findFirst().get();
            index = new HashIndex(
                    id, name, unique, Catalog.getInstance().getSchema( key.schemaId ),
                    Catalog.getInstance().getTable( key.tableId ),
                    Catalog.getInstance().getTable( cfk.referencedKeyTableId ),
                    key.getColumnNames(),
                    cfk.getReferencedKeyColumnNames() );
        } else {
            // Other type of index, e.g. plain UNIQUE constraint: map columns -> primary key columns
            final CatalogTable table = Catalog.getInstance().getTable( key.tableId );
            final CatalogPrimaryKey pk = Catalog.getInstance().getPrimaryKey( table.primaryKey );
            index = new HashIndex(
                    id, name, unique, Catalog.getInstance().getSchema( key.schemaId ),
                    table,
                    table,
                    key.getColumnNames(),
                    pk.getColumnNames() );
        }
        indexById.put( id, index );
        indexByName.put( name, index );
        final Transaction tx = transaction != null ? transaction : transactionManager.startTransaction( "pa", "APP", false );
        try {
            index.rebuild( tx );
            tx.commit();
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


    public Index getIndex( long indexId ) {
        return this.indexById.get( indexId );
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
                        && index.type == type
                        && index.unique == unique
        ).findFirst().orElse( null );
    }


    public List<Index> getIndices( CatalogSchema schema, CatalogTable table ) {
        return this.indexById.values().stream().filter( index -> index.schema.equals( schema ) && index.table.equals( table ) ).collect( Collectors.toList() );
    }


    public List<Index> getReverseIndices( CatalogSchema schema, CatalogTable table ) {
        return this.indexById.values().stream().filter( index -> index.schema.equals( schema ) && index.targetTable.equals( table ) ).collect( Collectors.toList() );
    }

}
