package org.polypheny.db.mql;

import org.bson.BsonDocument;

public class MqlSave extends MqlCollectionStatement {

    public MqlSave( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
