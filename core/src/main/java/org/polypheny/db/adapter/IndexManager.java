package org.polypheny.db.adapter;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;


public class IndexManager {

    private static final IndexManager INSTANCE = new IndexManager();

    private final Map<Integer, Index> indexById = new HashMap<>();
    private final Map<String, Index> indexByName = new HashMap<>();


    public static IndexManager getInstance() {
        return INSTANCE;
    }


    private IndexManager() {
        // intentionally empty
    }


    public Index getIndex( CatalogSchema schema, CatalogTable table, List<String> columns ) {
        return new HashIndex( true, schema, table, columns );
//        return this.indexById.values().stream().filter( index ->
//                index.schema.equals( schema )
//                        && index.table.equals( table )
//                        && index.columns.equals( columns )
//        ).findFirst().orElse( null );
    }


    public Index getIndex( CatalogSchema schema, CatalogTable table, List<String> columns, IndexType type ) {
        return this.indexById.values().stream().filter( index ->
                index.schema.equals( schema )
                        && index.table.equals( table )
                        && index.columns.equals( columns )
                        && index.type == type
        ).findFirst().orElse( null );
    }


}
