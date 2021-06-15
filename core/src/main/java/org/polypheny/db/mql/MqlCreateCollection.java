package org.polypheny.db.mql;

import java.util.ArrayList;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.mql.Mql.Type;
import org.polypheny.db.transaction.Statement;

public class MqlCreateCollection extends MqlNode implements MqlExecutableStatement {

    String name;


    public MqlCreateCollection( String name ) {
        this.name = name;
    }


    @Override
    public Type getKind() {
        return Type.CREATE_COLLECTION;
    }


    @Override
    public String toString() {
        return "MqlCreateCollection{" +
                "name='" + name + '\'' +
                '}';
    }


    @Override
    public void execute( Context context, Statement statement ) {
        Catalog catalog = Catalog.getInstance();
        AdapterManager adapterManager = AdapterManager.getInstance();

        long schemaId;
        try {

            schemaId = catalog.getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;

        } catch ( UnknownSchemaException e ) {
            throw new RuntimeException( "not impl yet." );
        }
        PlacementType placementType = PlacementType.AUTOMATIC;

        try {
            DdlManager.getInstance().createTable(
                    schemaId,
                    name,
                    new ArrayList<>(),
                    new ArrayList<>(),
                    false,
                    stores.stream().map( store -> (DataStore) adapterManager.getAdapter( store ) ).collect( Collectors.toList() ),
                    placementType,
                    statement );
        } catch ( TableAlreadyExistsException | ColumnNotExistsException | UnknownPartitionTypeException e ) {
            throw new RuntimeException( "not impl yet." );
        }
    }

}
