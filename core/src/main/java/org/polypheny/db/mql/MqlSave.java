package org.polypheny.db.mql;

public class MqlSave extends MqlCollectionStatement {

    public MqlSave( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
