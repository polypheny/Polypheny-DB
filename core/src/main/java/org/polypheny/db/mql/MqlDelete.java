package org.polypheny.db.mql;

import org.bson.BsonDocument;

public class MqlDelete extends MqlCollectionStatement {

    public MqlDelete( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
