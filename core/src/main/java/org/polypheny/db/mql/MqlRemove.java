package org.polypheny.db.mql;

import org.bson.BsonDocument;
import org.polypheny.db.mql.Mql.Type;

public class MqlRemove extends MqlCollectionStatement {

    public MqlRemove( String collection, BsonDocument document ) {
        super( collection, document );
    }


    @Override
    Type getKind() {
        return Type.REMOVE;
    }

}
