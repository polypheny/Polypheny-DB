package org.polypheny.db.mql;

public class MqlFind extends MqlCollectionStatement {

    public MqlFind( String collection, BsonDocument document ) {
        super( collection, document );
    }

}
