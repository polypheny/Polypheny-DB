package org.polypheny.db.mql;

import org.bson.BsonDocument;

public class MqlInsert extends MqlCollectionStatement {

    public MqlInsert( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
