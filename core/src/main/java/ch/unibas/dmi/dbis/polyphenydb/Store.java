package ch.unibas.dmi.dbis.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;


public interface Store {


    void addDatabase( String databaseName );

    void addSchema( String schemaName, String databaseName );

    void addTable( String tableName, String schemaName, String databaseName );

    Schema getSchema();

}
