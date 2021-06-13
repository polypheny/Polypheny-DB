package org.polypheny.db.mql;

import org.bson.BsonDocument;

public class MqlFind extends MqlCollectionStatement {

    public MqlFind( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
