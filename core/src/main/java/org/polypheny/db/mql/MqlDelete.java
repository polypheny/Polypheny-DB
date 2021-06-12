package org.polypheny.db.mql;

public class MqlDelete extends MqlCollectionStatement {

    public MqlDelete( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
