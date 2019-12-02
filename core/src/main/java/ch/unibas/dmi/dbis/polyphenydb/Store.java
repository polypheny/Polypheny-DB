package ch.unibas.dmi.dbis.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import lombok.Getter;


public abstract class Store {

    @Getter
    private final int storeId;
    @Getter
    private final String uniqueName;


    public Store( final int storeId, final String uniqueName ) {
        this.storeId = storeId;
        this.uniqueName = uniqueName;
    }


    public abstract void createNewSchema( Transaction transaction, SchemaPlus rootSchema, String name );

    public abstract Table createTableSchema( CatalogCombinedTable combinedTable );

    public abstract Schema getCurrentSchema();

    public abstract void createTable( Context context, CatalogCombinedTable combinedTable );

    public abstract void dropTable( Context context, CatalogCombinedTable combinedTable );

    public abstract void addColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn );

    public abstract void dropColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn );

    public abstract boolean prepare( PolyXid xid );

    public abstract void commit( PolyXid xid );

    public abstract void truncate( Context context, CatalogCombinedTable table );

    public abstract void updateColumnType( Context context, CatalogColumn catalogColumn );

    public abstract String getAdapterName();
}
