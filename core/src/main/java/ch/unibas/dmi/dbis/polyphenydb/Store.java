package ch.unibas.dmi.dbis.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import java.util.Map;


public interface Store {

    Map<String, Table> getTables( SchemaPlus rootSchema );

}
