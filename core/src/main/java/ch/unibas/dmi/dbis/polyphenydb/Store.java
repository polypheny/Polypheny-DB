package ch.unibas.dmi.dbis.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;


public interface Store {

    void createNewSchema( SchemaPlus rootSchema, String name );

    Table createTableSchema( CatalogCombinedTable combinedTable );
}
