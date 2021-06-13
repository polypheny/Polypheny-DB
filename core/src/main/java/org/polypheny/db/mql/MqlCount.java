package org.polypheny.db.mql;

import org.bson.BsonDocument;

public class MqlCount extends MqlCollectionStatement {

    private final boolean isEstimate;


    public MqlCount( String collection, BsonDocument document ) {
        this( collection, document, false );
    }


    public MqlCount( String collection, BsonDocument document, boolean isEstimate ) {
        super( collection, document );
        this.isEstimate = isEstimate;
    }

}
