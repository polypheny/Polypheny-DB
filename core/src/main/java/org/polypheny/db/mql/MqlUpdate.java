package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlUpdate extends MqlCollectionStatement {

    public MqlUpdate( String collection, BsonDocument document ) {
        super( collection, document );
    }


    @Override
    Type getKind() {
        return Type.UPDATE;
    }

}
