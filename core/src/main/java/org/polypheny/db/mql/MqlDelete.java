package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlDelete extends MqlCollectionStatement {

    public MqlDelete( String collection, BsonDocument document ) {
        super( collection, document );
    }


    @Override
    Type getKind() {
        return Type.DELETE;
    }

}
