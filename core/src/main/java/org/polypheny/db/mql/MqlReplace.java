package org.polypheny.db.mql;

import org.bson.BsonDocument;

public class MqlReplace extends MqlCollectionStatement {

    public MqlReplace( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
