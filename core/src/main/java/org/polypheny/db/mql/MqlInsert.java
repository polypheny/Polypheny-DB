package org.polypheny.db.mql;

public class MqlInsert extends MqlCollectionStatement {

    public MqlInsert( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
