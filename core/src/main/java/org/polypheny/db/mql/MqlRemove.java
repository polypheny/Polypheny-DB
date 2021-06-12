package org.polypheny.db.mql;

public class MqlRemove extends MqlCollectionStatement {

    public MqlRemove( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
