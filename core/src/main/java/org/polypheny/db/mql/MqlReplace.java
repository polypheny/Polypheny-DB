package org.polypheny.db.mql;

public class MqlReplace extends MqlCollectionStatement {

    public MqlReplace( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
