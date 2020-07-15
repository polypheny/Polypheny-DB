package org.polypheny.db.adapter;


import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
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
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.jdbc.Context;


public class IndexManager implements PropertyChangeListener {

    private static final IndexManager INSTANCE = new IndexManager();

    private Context context = null;
    private final Map<Long, Index> indexById = new HashMap<>();
    private final Map<String, Index> indexByName = new HashMap<>();


    public static IndexManager getInstance() {
        return INSTANCE;
    }


    private IndexManager() {
        // intentionally empty
    }


    public void restoreIndices( final Catalog catalog, final Context context ) throws UnknownSchemaException, GenericCatalogException, UnknownTableException, UnknownKeyException {
        catalog.addObserver( this );
        this.context = context;
        for ( final CatalogIndex index : catalog.getIndices() ) {
            addIndex( index );
        }
    }


    @Override
    public void propertyChange( PropertyChangeEvent propertyChangeEvent ) {
        // TODO(s3lph): There are separate keys for indexes, so this never gives us the actual primary or foreign key objects
        if ( !"index".equals( propertyChangeEvent.getPropertyName() ) ) {
            return;
        }
        if ( propertyChangeEvent.getNewValue() == null && propertyChangeEvent.getOldValue() != null ) {
            assert propertyChangeEvent.getOldValue() instanceof CatalogKey;
            final long keyId = ((CatalogKey) propertyChangeEvent.getOldValue()).id;
            indexById.remove( keyId );
        } else if ( propertyChangeEvent.getNewValue() != null && propertyChangeEvent.getOldValue() == null ) {
            assert propertyChangeEvent.getNewValue() instanceof Long;
            final long keyId = (long) propertyChangeEvent.getNewValue();
            final CatalogKey catalogKey = Catalog.getInstance().getKeys().stream().filter( k -> k.id == keyId ).findFirst().orElse( null );
            if ( catalogKey == null ) {
                return;
            }
            for ( final CatalogIndex index : Catalog.getInstance().getIndices( catalogKey ) ) {
                if ( this.indexById.get( index.keyId ) == null ) {
                    try {
                        this.addIndex( index );
                    } catch ( UnknownSchemaException | GenericCatalogException | UnknownTableException | UnknownKeyException e ) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


    public void addIndex( final CatalogIndex index ) throws UnknownSchemaException, GenericCatalogException, UnknownTableException, UnknownKeyException {
        addIndex( context, index.id, index.name, index.key, index.type, index.unique );
    }


    protected void addIndex( final Context context, final long id, final String name, final CatalogKey key, final IndexType type, final boolean unique ) throws UnknownSchemaException, GenericCatalogException, UnknownTableException, UnknownKeyException {
        // TODO(s3lph): INDEX TYPES
        final Index index;
        if ( key instanceof CatalogPrimaryKey ) {
            index = new HashIndex(
                    id, name, unique, Catalog.getInstance().getSchema( key.schemaId ),
                    Catalog.getInstance().getTable( key.tableId ),
                    Catalog.getInstance().getTable( key.tableId ),
                    key.getColumnNames(),
                    key.getColumnNames() );
        } else if (key instanceof CatalogForeignKey ) {
            final CatalogForeignKey cfk = (CatalogForeignKey) key;
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
        index.rebuild( context );
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
