package ch.unibas.dmi.dbis.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;


public interface Store {

    void createNewSchema( Transaction transaction, SchemaPlus rootSchema, String name );

    Table createTableSchema( CatalogCombinedTable combinedTable );

    Schema getCurrentSchema();

    void createTable( Context context, CatalogCombinedTable combinedTable );

    void dropTable( Context context, CatalogCombinedTable combinedTable );

    void addColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn );

    void dropColumn( Context context, CatalogCombinedTable catalogTable, CatalogColumn catalogColumn );

    boolean prepare( PolyXid xid );

    void commit( PolyXid xid );

    void truncate( Context context, CatalogCombinedTable table );

    void updateColumnType( Context context, CatalogColumn catalogColumn );
}
