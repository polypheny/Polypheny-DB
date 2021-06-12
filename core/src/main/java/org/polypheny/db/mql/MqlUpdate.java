package org.polypheny.db.mql;

public class MqlUpdate extends MqlCollectionStatement {

    public MqlUpdate( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
