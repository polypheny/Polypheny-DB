package org.polypheny.db.mql;

import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.ddl.exception.SchemaNotExistException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.mql.Mql.Type;
import org.polypheny.db.transaction.Statement;

public class MqlDrop extends MqlCollectionStatement implements MqlExecutableStatement {

    public MqlDrop( String collection ) {
        super( collection );
    }


    @Override
    public Type getKind() {
        return Type.DROP;
    }


    @Override
    public void execute( Context context, Statement statement, String database ) {
        DdlManager ddlManager = DdlManager.getInstance();

        try {
            ddlManager.dropSchema( Catalog.defaultDatabaseId, database, true, statement );
        } catch ( SchemaNotExistException | DdlOnSourceException e ) {
            throw new RuntimeException( "An error occurred while dropping the database (Polypheny Schema): " + e );
        }


    }

}
