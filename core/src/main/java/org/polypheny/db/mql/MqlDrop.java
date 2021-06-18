package org.polypheny.db.mql;

import org.polypheny.db.mql.Mql.Type;

public class MqlDrop extends MqlCollectionStatement {

    public MqlDrop( String collection ) {
        super( collection );
    }


    @Override
    public Type getKind() {
        return Type.DROP;
    }

}
