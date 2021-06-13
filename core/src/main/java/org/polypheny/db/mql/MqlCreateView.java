package org.polypheny.db.mql;

import org.bson.BsonDocument;

public class MqlCreateView extends MqlCollectionStatement {

    private final String source;


    public MqlCreateView( String collection, String source, BsonDocument document ) {
        super( collection, document );
        this.source = source;
    }

}
