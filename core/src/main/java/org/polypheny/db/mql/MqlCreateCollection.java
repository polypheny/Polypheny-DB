package org.polypheny.db.mql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnInformation;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.mql.Mql.Type;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;

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

            schemaId = catalog.getSchema( context.getDatabaseId(), context.getDefaultDocumentSchemaName() ).id;

        } catch ( UnknownSchemaException e ) {
            throw new RuntimeException( "not impl yet." );
        }
        PlacementType placementType = PlacementType.AUTOMATIC;

        try {
            ColumnTypeInformation type = new ColumnTypeInformation( PolyType.VARCHAR, null, 12, null, null, null, false );
            ColumnInformation information = new ColumnInformation( "_id", type, Collation.CASE_INSENSITIVE, null, 0 );
            List<DataStore> dataStores = stores.stream().map( store -> (DataStore) adapterManager.getAdapter( store ) ).collect( Collectors.toList() );
            DdlManager.getInstance().createTable(
                    schemaId,
                    name,
                    Collections.singletonList( information ),
                    new ArrayList<>(),
                    false,
                    dataStores.size() == 0 ? null : dataStores,
                    placementType,
                    statement );
        } catch ( TableAlreadyExistsException | ColumnNotExistsException | UnknownPartitionTypeException e ) {
            throw new RuntimeException( "not impl yet." );
        }
    }

}
