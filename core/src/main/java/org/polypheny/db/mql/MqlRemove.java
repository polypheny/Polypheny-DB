package org.polypheny.db.mql;

import org.bson.BsonDocument;

public class MqlRemove extends MqlCollectionStatement {

    public MqlRemove( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
