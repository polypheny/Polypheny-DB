package org.polypheny.db.mql;

import org.bson.BsonDocument;

public class MqlUpdate extends MqlCollectionStatement {

    public MqlUpdate( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
