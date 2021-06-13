package org.polypheny.db.mql;

import org.bson.BsonDocument;

public abstract class MqlCollectionStatement extends MqlNode {

    private final BsonDocument document;
    private final String collection;


    public MqlCollectionStatement( String collection, BsonDocument document ) {
        this.document = document;
        this.collection = collection;
        System.out.println( this );
    }


    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "document=" + document +
                ", collection='" + collection + '\'' +
                '}';
    }

}
