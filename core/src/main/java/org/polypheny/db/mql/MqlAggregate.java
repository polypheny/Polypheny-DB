package org.polypheny.db.mql;

import org.bson.BsonDocument;

public class MqlAggregate extends MqlCollectionStatement {

    public MqlAggregate( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
