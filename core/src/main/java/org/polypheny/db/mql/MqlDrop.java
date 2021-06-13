package org.polypheny.db.mql;

import org.polypheny.db.mql.Mql.Type;

public class MqlDrop extends MqlCollectionStatement {

    public MqlDrop( String collection ) {
        super( collection, null );
    }


    @Override
    Type getKind() {
        return Type.DROP;
    }

}
